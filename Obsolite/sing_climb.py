# =============================================================================
# sing_climb.py
# Climb API integration for SING Pipeline Scheduler
#
# Replaces manual CSV exports with live API data.
# Output DataFrames match the exact column names of the current CSV exports.
#
# Usage (standalone):  python sing_climb.py
# Usage (from pipeline): from sing_climb import get_animals_df, get_births_df,
#                                              get_matings_df, get_next_sample_number
# =============================================================================

import requests
import time
import pandas as pd
from datetime import datetime, date

# ── Credentials — loaded from sing_credentials.json (never committed) ─────────
import json as _json

def _load_credentials() -> dict:
    """Load Climb API credentials from sing_credentials.json in the script directory."""
    import os as _os_cred
    cred_path = _os_cred.path.join(
        _os_cred.path.dirname(_os_cred.path.abspath(__file__)),
        'sing_credentials.json'
    )
    if not _os_cred.path.exists(cred_path):
        raise FileNotFoundError(
            f'Climb credentials file not found: {cred_path}\n'
            'Copy sing_credentials.json.template to sing_credentials.json '
            'and fill in your client_id, client_secret, and workgroup_key.'
        )
    with open(cred_path, 'r', encoding='utf-8') as _f:
        _creds = _json.load(_f)
    if not _creds.get('client_id') or not _creds.get('client_secret'):
        raise ValueError(
            'sing_credentials.json is missing client_id or client_secret.'
        )
    return _creds

_creds         = _load_credentials()
_CLIENT_ID     = _creds['client_id']
_CLIENT_SECRET = _creds['client_secret']
_WORKGROUP_KEY = _creds['workgroup_key']
_TOKEN_URL     = 'https://climb-admin.azurewebsites.net/token'
_API_BASE      = 'https://api.climb.bio'

# ── Config ────────────────────────────────────────────────────────────────────
ANIMAL_STATUS_FILTER = 'Alive'
ANIMAL_USE_FILTER    = 'Sing Inventory'
MATING_STATUS_FILTER = 'Active Mating'
SING_STUDY_NAME      = 'Sing Project'

# Tim's room identifiers in Climb location strings
TIM_ROOM_IDENTIFIER     = 'RAF-B6'
SOPHIE_ROOM_IDENTIFIER  = 'F29'

# Rate limit: 20 req/sec — stay at 10 req/sec
_REQUEST_DELAY = 0.1
_PAGE_SIZE     = 500

# ── Token cache ───────────────────────────────────────────────────────────────
_token        = None
_token_expiry = 0.0


# =============================================================================
# Auth + request helpers
# =============================================================================

def _get_token() -> str:
    global _token, _token_expiry
    if _token and time.time() < _token_expiry - 60:
        return _token
    r = requests.post(_TOKEN_URL, data={
        'grant_type':    'client_credentials',
        'client_id':     _CLIENT_ID,
        'client_secret': _CLIENT_SECRET,
    }, timeout=15)
    r.raise_for_status()
    data          = r.json()
    _token        = data['access_token']
    _token_expiry = time.time() + data.get('expires_in', 3600)
    return _token


def _headers() -> dict:
    return {
        'Authorization':   f'Bearer {_get_token()}',
        'X-Workgroup-Key': _WORKGROUP_KEY,
    }


def _get(url: str, params: dict = None, retries: int = 3) -> dict:
    """Rate-limited GET with automatic 429 retry."""
    for attempt in range(retries):
        time.sleep(_REQUEST_DELAY)
        r = requests.get(url, headers=_headers(), params=params, timeout=30)
        if r.status_code == 429:
            wait = int(r.headers.get('Retry-After', 2))
            print(f'  Rate limited — waiting {wait}s...')
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f'Climb API: max retries exceeded for {url}')


def _get_all(endpoint: str, params: dict = None) -> list:
    """Fetch every page of a paged endpoint."""
    p = dict(params or {})
    p['pageSize'] = _PAGE_SIZE
    all_items, page, total = [], 1, None
    while True:
        p['pageNumber'] = page
        data  = _get(f'{_API_BASE}{endpoint}', params=p)
        items = data.get('data', {}).get('items', [])
        if total is None:
            total = data.get('data', {}).get('totalItemCount', 0)
        all_items.extend(items)
        if len(all_items) >= total or not items:
            break
        page += 1
    return all_items


# =============================================================================
# Lines map — the backbone for Line (Short), room filter, breeding strategy
# =============================================================================

def _get_lines_maps() -> tuple:
    """
    Fetch all lines and return two dicts:
      by_key  : {lineKey (int) -> info}
      by_name : {full line name (str) -> info}
    Each info dict has: short_name, default_location, technician,
                        breeding_strategy
    """
    items  = _get_all('/api/lines')
    by_key, by_name = {}, {}
    for item in items:
        info = {
            'short_name':        str(item.get('shortName',        '') or '').strip(),
            'default_location':  str(item.get('defaultLocation',  '') or '').strip(),
            'technician':        str(item.get('technician',        '') or '').strip(),
            'breeding_strategy': str(item.get('breedingStrategy', '') or '').strip(),
        }
        if item.get('lineKey') is not None:
            by_key[item['lineKey']] = info
        if item.get('name'):
            by_name[item['name'].strip()] = info
    return by_key, by_name


def _is_tims_line(info: dict) -> bool:
    """Return True if a line belongs to Tim based on room or technician."""
    loc  = info.get('default_location', '')
    tech = info.get('technician', '').lower()
    return TIM_ROOM_IDENTIFIER in loc or tech == 'tjleach'


# =============================================================================
# Housing location helper
# =============================================================================

def _get_housing_location(housing_id: str) -> str:
    """
    Return the current location string for a housing cage.
    Returns '' if not set (caller should flag as no-location).
    """
    if not housing_id:
        return ''
    try:
        data  = _get(f'{_API_BASE}/api/housings', params={'HousingID': str(housing_id)})
        items = data.get('data', {}).get('items', [])
        if not items:
            return ''
        h = items[0]
        # currentLocation is the full path string when set
        loc = str(h.get('currentLocation') or '').strip()
        if loc:
            return loc
        # Fall back to most recent active location entry
        locs   = h.get('locations', [])
        active = [l for l in locs if not l.get('dateOut')]
        if active:
            return active[0].get('locationPositionName', '')
        if locs:
            latest = sorted(locs, key=lambda x: x.get('dateIn', ''), reverse=True)
            return latest[0].get('locationPositionName', '')
        return ''
    except Exception:
        return ''


# =============================================================================
# Animals
# =============================================================================

def get_animals_df(verbose: bool = True) -> pd.DataFrame:
    """
    Fetch all Alive + Sing Inventory animals.
    Line (Short) comes from lines map (for ungenotyped) or genotypes (for genotyped).
    """
    if verbose:
        print('Fetching animals from Climb API...')
    raw     = _get_all('/api/animals')
    animals = [a for a in raw
               if a.get('status') == ANIMAL_STATUS_FILTER
               and a.get('use')    == ANIMAL_USE_FILTER]
    if verbose:
        print(f'  Total in workgroup: {len(raw)}  '
              f'\u2192  {ANIMAL_STATUS_FILTER} + {ANIMAL_USE_FILTER}: {len(animals)}')

    if not animals:
        return pd.DataFrame()

    if verbose:
        print('Fetching lines...')
    lines_by_key, _ = _get_lines_maps()

    if verbose:
        print('Fetching genotypes...')
    geno_raw = _get_all('/api/genotypes')

    # Build per-animal genotype lookup: materialKey -> {line_short, genotype_str}
    geno_map: dict = {}
    for g in geno_raw:
        mk      = g.get('materialKey')
        assay   = str(g.get('assay',    '') or '').strip()
        symbol  = str(g.get('genotype', '') or '').strip()
        ls      = str(g.get('lineShortName', '') or '').strip()
        geno_str = f'{assay} {symbol}'.strip() if assay else symbol
        if mk not in geno_map:
            geno_map[mk] = {'line_short': ls, 'genotype_parts': []}
        geno_map[mk]['genotype_parts'].append(geno_str)
        if ls and not geno_map[mk]['line_short']:
            geno_map[mk]['line_short'] = ls
    if verbose:
        print(f'  Genotype records: {len(geno_raw)}')

    today = pd.Timestamp(date.today())
    rows  = []
    for a in animals:
        mk         = a.get('materialKey')
        geno_info  = geno_map.get(mk, {})
        # Line (Short): genotype endpoint first, then lines map fallback
        line_short = geno_info.get('line_short', '')
        if not line_short:
            lk = a.get('lineKey')
            line_short = lines_by_key.get(lk, {}).get('short_name', '')

        geno_parts = geno_info.get('genotype_parts', [])
        genotype   = ', '.join(geno_parts) if geno_parts else 'Blank'

        born      = pd.to_datetime(a.get('dateBorn'), errors='coerce')
        age_days  = int((today - born).days)  if pd.notna(born) else None
        age_weeks = round(age_days / 7, 1)    if age_days is not None else None
        age_months= round(age_days / 30, 1)   if age_days is not None else None

        rows.append({
            'ID':               a.get('animalId'),
            'Name':             a.get('animalName'),
            'Marker Type':      a.get('markerType'),
            'Marker':           a.get('physicalMarker'),
            'Sex':              a.get('sex'),
            'Status':           a.get('status'),
            'Mating Status':    a.get('animalMatingStatus'),
            'Breeding Status':  a.get('breedingStatus'),
            'Owner':            a.get('owner'),
            'Use':              a.get('use'),
            'IACUC Protocol':   a.get('iacucProtocol'),
            'Line':             a.get('line'),
            'Line (Short)':     line_short,
            'Species':          a.get('species'),
            'Origin':           a.get('origin'),
            'Genotype':         genotype,
            'Generation':       a.get('generation'),
            'Diet':             a.get('diet'),
            'Microchip ID':     a.get('microchipIdentifier'),
            'External ID':      a.get('externalIdentifier'),
            'Birth ID':         a.get('birthId'),
            'Parent Mating ID': a.get('parentMatingId'),
            'Birth Date':       born.date().isoformat() if pd.notna(born) else None,
            'Arrival Date':     a.get('arrivalDate'),
            'Age (days)':       age_days,
            'Age (weeks)':      age_weeks,
            'Age (months)':     age_months,
            'Housing ID':       a.get('housingID'),
            'Location':         a.get('location'),
            # Internal write-back keys (dropped on CSV export)
            '_materialKey':             mk,
            '_animalUseKey':            a.get('animalUseKey'),
            '_physicalMarkerTypeKey':   a.get('physicalMarkerTypeKey'),
            '_lineKey':                 a.get('lineKey'),
        })

    df = pd.DataFrame(rows)
    if verbose:
        print(f'  Animals DataFrame: {len(df)} rows, {len(df.columns)} columns')
    return df


# =============================================================================
# Births
# =============================================================================

def get_births_df(verbose: bool = True) -> pd.DataFrame:
    """Fetch all birth records — no status filter, all births are relevant."""
    if verbose:
        print('Fetching births from Climb API...')
    raw = _get_all('/api/birth')
    if verbose:
        print(f'  Birth records: {len(raw)}')

    rows = []
    for b in raw:
        rows.append({
            'Birth ID':          b.get('birthKey'),
            'Mating ID':         b.get('matingID'),
            'Housing ID':        b.get('housingID'),
            'Status':            b.get('status'),
            'Line':              b.get('line'),
            'Birth Date':        b.get('birthDate'),
            'Wean Date':         b.get('weanDate'),
            'Live Count':        b.get('liveBornCount'),
            'Stillborn Count':   b.get('stillbornCount'),
            'Dead Count':        b.get('deadCount'),
            'Foster':            b.get('isFoster'),
            'Foster Housing ID': b.get('fosterHousingID'),
            'Location':          b.get('location'),
            'Notes':             b.get('comments'),
            'Created By':        b.get('createdBy'),
            'Created Date':      b.get('dateCreated'),
            'Modified By':       b.get('modifiedBy'),
            'Modified Date':     b.get('dateModified'),
        })

    df = pd.DataFrame(rows)
    if verbose:
        print(f'  Births DataFrame: {len(df)} rows')
    return df


# =============================================================================
# Matings
# =============================================================================

def get_matings_df(verbose: bool = True,
                   remaining_quota: dict = None) -> pd.DataFrame:
    """
    Fetch Tim's active matings filtered by:
      - status = Active Mating
      - line belongs to Tim (technician=tjleach OR defaultLocation contains RAF-B6)
    Adds housing location for each mating and flags:
      - no_location  : housing has no location set in Climb
      - in_sophie_room : mating is in F29 (Sophie's room)
      - finish_strain  : <= 3 animals remaining in quota AND all matings
                         for that strain are in Sophie's room (no B6 coverage)
    remaining_quota : optional dict {line_short: int} from tracking sheet
    """
    if verbose:
        print('Fetching matings from Climb API...')

    _, lines_by_name = _get_lines_maps()

    raw    = _get_all('/api/matings')
    active = []
    for m in raw:
        if m.get('status') != MATING_STATUS_FILTER:
            continue
        line_info = lines_by_name.get(m.get('line', '').strip(), {})
        if _is_tims_line(line_info):
            m['_line_info'] = line_info
            active.append(m)

    if verbose:
        print(f'  Total: {len(raw)}  \u2192  Tim\u2019s active: {len(active)}')

    # ── Fetch housing location for each mating ────────────────────────────────
    if verbose:
        print(f'  Fetching housing locations ({len(active)} cages)...')

    rows = []
    for m in active:
        line_info = m.pop('_line_info', {})
        raw_date  = str(m.get('date', '') or '').strip()
        try:
            mating_date = pd.to_datetime(raw_date, format='mixed').date().isoformat()
        except Exception:
            mating_date = raw_date

        housing_id  = str(m.get('housingID') or '').strip()
        location    = _get_housing_location(housing_id)

        no_location    = location == ''
        in_sophie_room = SOPHIE_ROOM_IDENTIFIER in location and not no_location
        in_tims_room   = TIM_ROOM_IDENTIFIER    in location

        rows.append({
            'Mating ID':      m.get('matingID'),
            'Housing ID':     housing_id,
            'Status':         m.get('status'),
            'Line':           m.get('line'),
            'Line (Short)':   line_info.get('short_name', ''),
            'Mating Date':    mating_date,
            'Type':           m.get('type'),
            'Births':         m.get('birthCount', 0),
            'Comments':       line_info.get('breeding_strategy', ''),
            'Location':       location,
            'In Tims Room':   in_tims_room,
            'In Sophies Room':in_sophie_room,
            'No Location':    no_location,
            # Finish strain flag calculated below
            'Finish Strain Flag': False,
            '_modified':      m.get('dateModified', ''),
        })

    df = pd.DataFrame(rows)

    # ── Finish strain flag ────────────────────────────────────────────────────
    # Flag strains where: quota <= 3 AND no mating in B6 for that strain
    if remaining_quota and not df.empty:
        for line_short, remaining in remaining_quota.items():
            if remaining > 3:
                continue
            strain_matings = df[df['Line (Short)'] == line_short]
            if strain_matings.empty:
                continue
            any_in_b6 = strain_matings['In Tims Room'].any()
            if not any_in_b6:
                df.loc[df['Line (Short)'] == line_short, 'Finish Strain Flag'] = True

    if verbose:
        no_loc = df['No Location'].sum()
        sophie = df['In Sophies Room'].sum()
        finish = df['Finish Strain Flag'].sum()
        print(f'  Matings DataFrame: {len(df)} rows')
        if no_loc:  print(f'  \u26a0  No location set: {no_loc}')
        if sophie:  print(f'  \u26a0  In Sophie\u2019s room (F29): {sophie}')
        if finish:  print(f'  \u26a0  Finish strain flag: {finish}')

    return df


# =============================================================================
# Next sample number
# =============================================================================

def get_next_sample_number(verbose: bool = True) -> int:
    """
    Fetch all SING Project samples and return the next sequential sample number.
    Sample names are expected to be numeric strings (e.g. '5432').
    """
    if verbose:
        print('Fetching samples to find next sample number...')
    raw          = _get_all('/api/samples')
    sing_samples = [s for s in raw
                    if (s.get('study') or '').lower() == SING_STUDY_NAME.lower()]
    if verbose:
        print(f'  Total samples: {len(raw)}  SING samples: {len(sing_samples)}')

    max_num = 0
    for s in sing_samples:
        name = str(s.get('name') or '').strip()
        try:
            # Handle names like "5432" or "5432-1"
            num = int(name.split('-')[0])
            if num > max_num:
                max_num = num
        except (ValueError, TypeError):
            pass

    next_num = max_num + 1
    if verbose:
        print(f'  Highest sample number: {max_num}  \u2192  Next: {next_num}')
    return next_num


# =============================================================================
# Save exports to CSV
# =============================================================================

def save_exports(output_dir: str = '.', verbose: bool = True,
                 animals_df: pd.DataFrame = None,
                 births_df:  pd.DataFrame = None,
                 matings_df: pd.DataFrame = None) -> dict:
    """
    Save all three DataFrames as CSV files.
    Pass pre-fetched DataFrames to avoid double-fetching.
    """
    import os
    ts    = datetime.now().strftime('%Y%m%d_%H%M%S')
    saved = {}
    _drop = lambda df: df.drop(columns=[c for c in df.columns if c.startswith('_')],
                                errors='ignore')

    if animals_df is None:
        animals_df = get_animals_df(verbose=verbose)
    if not animals_df.empty:
        path = os.path.join(output_dir, f'animals_{ts}.csv')
        _drop(animals_df).to_csv(path, index=False, encoding='utf-8-sig')
        saved['animals'] = path
        if verbose: print(f'  Saved: {path}')

    if births_df is None:
        births_df = get_births_df(verbose=verbose)
    if not births_df.empty:
        path = os.path.join(output_dir, f'births_{ts}.csv')
        births_df.to_csv(path, index=False, encoding='utf-8-sig')
        saved['births'] = path
        if verbose: print(f'  Saved: {path}')

    if matings_df is None:
        matings_df = get_matings_df(verbose=verbose)
    if not matings_df.empty:
        path = os.path.join(output_dir, f'matings_{ts}.csv')
        _drop(matings_df).to_csv(path, index=False, encoding='utf-8-sig')
        saved['matings'] = path
        if verbose: print(f'  Saved: {path}')

    return saved


# =============================================================================
# Behavior booked counts from P56 cohorts
# =============================================================================

def get_behavior_booked(verbose: bool = True) -> dict:
    """
    Fetch all cohorts whose name starts with 'P56' and return
    {wednesday_date_str (YYYY-MM-DD): animal_count}.
    Cohort names follow the pattern: P56 YYYY_MM_DD [optional suffix]
    e.g. 'P56 2026_08_26'  or  'P56 2026_07_01 - Only NB Mice'
    """
    import re as _re
    if verbose:
        print('Fetching P56 behavior cohorts from Climb API...')
    cohorts = _get_all('/api/cohorts')
    pattern = _re.compile(r'^P56\s+(\d{4})_(\d{2})_(\d{2})', _re.IGNORECASE)
    booked  = {}
    for c in cohorts:
        name = str(c.get('name', '') or '').strip()
        m    = pattern.match(name)
        if m:
            date_str        = f'{m.group(1)}-{m.group(2)}-{m.group(3)}'
            booked[date_str] = int(c.get('animalCount', 0) or 0)
    if verbose:
        print(f'  P56 behavior cohorts found: {len(booked)}')
        for d, n in sorted(booked.items(), reverse=True)[:8]:
            print(f'    {d}  \u2192  {n} animals booked')
    return booked


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == '__main__':
    import os
    print('=' * 60)
    print('  sing_climb.py \u2014 Climb API Export Test')
    print('=' * 60)

    script_dir = os.path.dirname(os.path.abspath(__file__))

    print('\nStep 1 \u2014 Authenticating...')
    _get_token()
    print('  \u2713 Token received')

    print('\nStep 2 \u2014 Fetching animals...')
    animals = get_animals_df()
    if not animals.empty:
        cols = ['Name', 'Line (Short)', 'Genotype', 'Sex', 'Age (days)', 'Housing ID']
        print(animals[cols].head(8).to_string(index=False))
        blank = animals[animals['Genotype'] == 'Blank']
        filled = blank['Line (Short)'].notna() & (blank['Line (Short)'] != '')
        print(f'  Ungenotyped with Line (Short): {filled.sum()} / {len(blank)}')

    print('\nStep 3 \u2014 Fetching births...')
    births = get_births_df()
    if not births.empty:
        print(births[['Mating ID', 'Birth Date', 'Live Count', 'Status']].head(5).to_string(index=False))

    print('\nStep 4 \u2014 Fetching matings...')
    matings = get_matings_df()
    if not matings.empty:
        print(matings[['Mating ID', 'Housing ID', 'Line (Short)',
                        'Type', 'Location', 'No Location',
                        'In Sophies Room']].to_string(index=False))

    print('\nStep 5 \u2014 Next sample number...')
    next_n = get_next_sample_number()
    print(f'  Next sample number: {next_n}')

    print('\nStep 6 \u2014 Saving CSV exports...')
    saved = save_exports(output_dir=script_dir,
                         animals_df=animals,
                         births_df=births,
                         matings_df=matings)

    print('\n' + '=' * 60)
    print('  Done')
    print('=' * 60)
    input('\nPress Enter to close...')
