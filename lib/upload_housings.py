"""
upload_housings.py
Reads the fixed housings_incomplete CSV and updates Climb via PUT /api/housings/{housingID}.
Only updates rows where owner, date, or location actually changed.
Includes a dry-run preview before writing anything.

Lives in lib/. Credentials are read from lib/credentials/sing_credentials.json —
the canonical location. The old lib/ path is still accepted so a half-finished
move doesn't break this, but it warns so the stray copy gets cleaned up.
"""

import requests
import json
import os
import time
import pandas as pd
from datetime import datetime, timezone

# ── Credentials ───────────────────────────────────────────────────────────────
TOKEN_URL = 'https://climb-admin.azurewebsites.net/token'
API_BASE  = 'https://api.climb.bio'

_here      = os.path.dirname(os.path.abspath(__file__))
_preferred = os.path.join(_here, 'credentials', 'sing_credentials.json')
_legacy    = os.path.join(_here, 'sing_credentials.json')

if os.path.exists(_preferred):
    _cred_path = _preferred
    if os.path.exists(_legacy):
        print(f'  [!] A second copy of sing_credentials.json is still at {_legacy}')
        print( '      Using lib/credentials/. Delete the stray copy.')
elif os.path.exists(_legacy):
    _cred_path = _legacy
    print(f'  [!] sing_credentials.json found at the old location: {_legacy}')
    print( '      Move it to lib/credentials/ — that folder is git-ignored.')
else:
    raise FileNotFoundError(
        f'Climb credentials file not found. Expected at:\n  {_preferred}'
    )

with open(_cred_path, 'r', encoding='utf-8') as f:
    _creds = json.load(f)

WORKGROUP = _creds['workgroup_key']

# ── Change this to your fixed CSV filename ────────────────────────────────────
FIXED_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         'housings_incomplete_20260724_074639.csv')

# ── API helpers ───────────────────────────────────────────────────────────────
_token       = None
_token_time  = 0

def get_token():
    global _token, _token_time
    if _token and time.time() - _token_time < 3500:
        return _token
    r = requests.post(TOKEN_URL, data={
        'grant_type':    'client_credentials',
        'client_id':     _creds['client_id'],
        'client_secret': _creds['client_secret'],
    }, timeout=15)
    r.raise_for_status()
    _token      = r.json()['access_token']
    _token_time = time.time()
    return _token

def headers():
    return {'Authorization': f'Bearer {get_token()}',
            'X-Workgroup-Key': WORKGROUP,
            'Content-Type': 'application/json'}

def api_get(path, params=None, retries=3):
    for _ in range(retries):
        time.sleep(0.1)
        r = requests.get(f'{API_BASE}{path}', headers=headers(),
                         params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(int(r.headers.get('Retry-After', 2)))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f'GET {path} failed after retries')

def api_put(path, body, retries=3):
    for _ in range(retries):
        time.sleep(0.1)
        r = requests.put(f'{API_BASE}{path}', headers=headers(),
                         json=body, timeout=30)
        if r.status_code == 429:
            time.sleep(int(r.headers.get('Retry-After', 2)))
            continue
        return r
    raise RuntimeError(f'PUT {path} failed after retries')

def get_all(endpoint, page_size=500):
    items, page, total = [], 1, None
    while True:
        data  = api_get(endpoint, {'pageSize': page_size, 'pageNumber': page})
        batch = data.get('data', {}).get('items', [])
        if total is None:
            total = data.get('data', {}).get('totalItemCount', 0)
        items.extend(batch)
        if len(items) >= total or not batch:
            break
        page += 1
    return items

# ── Build location path → key lookup ─────────────────────────────────────────
def build_location_lookup():
    """Return {full_path_string: locationKey} for all locations."""
    locs     = get_all('/api/locations')
    by_key   = {l['locationKey']: l for l in locs}

    def full_path(loc):
        parts   = [loc['locationName']]
        parent  = loc.get('parentLocationKey', 0)
        visited = set()
        while parent and parent not in visited:
            visited.add(parent)
            p = by_key.get(parent)
            if not p:
                break
            parts.insert(0, p['locationName'])
            parent = p.get('parentLocationKey', 0)
        return ' > '.join(parts)

    return {full_path(l): l['locationKey'] for l in locs}


def build_vocab_lookup(endpoint: str, name_field: str, key_field: str) -> dict:
    """Return {name: key} from a vocabulary endpoint."""
    items = get_all(endpoint)
    return {str(i.get(name_field, '') or '').strip(): i.get(key_field)
            for i in items if i.get(name_field)}

# ── Get current housing record ────────────────────────────────────────────────
def get_housing(housing_id: str) -> dict:
    data  = api_get('/api/housings', {'HousingID': str(housing_id)})
    items = data.get('data', {}).get('items', [])
    return items[0] if items else {}

# ── Build PUT body from current record + fixes ─────────────────────────────────
def build_put_body(current: dict, new_owner: str, new_date: str,
                   new_loc_key: int, new_type_key: int,
                   new_status_key: int, new_container_key: int) -> dict:
    """
    Build the full PUT body applying all fixes from CSV.
    Date must be YYYYMMDD. Locations only contains the new assignment.
    """
    formatted_date = ''
    if new_date:
        try:
            formatted_date = pd.to_datetime(new_date).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        except Exception:
            formatted_date = ''

    # Get current active location key from the housing record
    current_loc_key = None
    for loc in current.get('locations', []):
        if not loc.get('dateOut'):
            current_loc_key = loc.get('locationPositionKey')

    # Only send a new location entry if it differs from what's already set
    locations = []
    if new_loc_key and new_loc_key != current_loc_key:
        locations = [{
            'locationKey': new_loc_key,
            'dateIn':      datetime.now(timezone.utc).strftime('%Y%m%d'),
            'dateOut':     '',
        }]

    return {
        'housingTypeKey':   new_type_key   or 0,
        'housingStatusKey': new_status_key or 0,
        'date':             formatted_date,
        'owner':            str(new_owner or '').strip(),
        'containerTypeKey': new_container_key or 0,
        'comments':         str(current.get('comments', '') or ''),
        'animalIds':        [],
        'locations':        locations,
    }

# ── Main ──────────────────────────────────────────────────────────────────────
print('=' * 60)
print('  Climb Housing Upload')
print('=' * 60)

print(f'\nReading: {os.path.basename(FIXED_CSV)}')
df = pd.read_csv(FIXED_CSV, dtype=str, encoding='utf-8-sig')
df = df.fillna('')
print(f'  Rows loaded: {len(df)}')

print('\nFetching location lookup...')
loc_lookup = build_location_lookup()
print(f'  Locations mapped: {len(loc_lookup)}')

print('Fetching vocabulary lookups...')
# Fetch one record from each vocab endpoint to find correct field names
def build_vocab_lookup_auto(endpoint: str) -> dict:
    """Auto-detect key/name fields and return {name: key}."""
    items = get_all(endpoint)
    if not items:
        return {}
    sample = items[0]
    # Find the key field (ends with 'Key') and name field
    key_field  = next((k for k in sample if k.endswith('Key') and isinstance(sample[k], int)), None)
    name_field = next((k for k in sample if 'name' in k.lower() or 'type' in k.lower()
                       and k != key_field and isinstance(sample[k], str)), None)
    if not key_field or not name_field:
        return {}
    return {str(i.get(name_field, '') or '').strip(): i.get(key_field)
            for i in items if i.get(name_field)}

type_lookup      = build_vocab_lookup_auto('/api/vocabulary/housingType')
status_lookup    = build_vocab_lookup_auto('/api/vocabulary/housingStatus')
container_lookup = build_vocab_lookup_auto('/api/vocabulary/containerType')
print(f'  Types: {len(type_lookup)}  Statuses: {len(status_lookup)}  Containers: {len(container_lookup)}')
if type_lookup:
    print(f'  Sample types: {list(type_lookup.keys())[:5]}')

# Validate location strings in CSV
unknown_locs = set()
for loc_str in df['Current Location'].unique():
    if loc_str.strip() and loc_str.strip() not in loc_lookup:
        unknown_locs.add(loc_str.strip())
if unknown_locs:
    print(f'\n  ⚠  Unknown location strings (will be skipped):')
    for u in sorted(unknown_locs):
        print(f'      {u!r}')

# ── Dry run: preview first 10 rows ───────────────────────────────────────────
print('\n' + '─' * 60)
print('DRY RUN — first 10 rows that would be updated:')
print('─' * 60)
preview_count = 0
for _, row in df.head(30).iterrows():
    housing_id    = str(row.get('Housing ID',      '')).strip()
    new_owner     = str(row.get('Owner',            '')).strip()
    new_date      = str(row.get('Date',             '')).strip()
    new_loc       = str(row.get('Current Location', '')).strip()
    new_type      = str(row.get('Type',             '')).strip()
    new_status    = str(row.get('Status',           '')).strip()
    new_container = str(row.get('Container',        '')).strip()
    loc_key       = loc_lookup.get(new_loc) if new_loc else None

    if not housing_id:
        continue

    print(f'  {housing_id:<12}  owner={new_owner:<12}  date={new_date:<12}  '
          f'type={new_type:<8}  status={new_status:<10}  '
          f'container={new_container:<8}  location={new_loc} (key={loc_key})')
    preview_count += 1
    if preview_count >= 10:
        break

print(f'\nTotal rows to process: {len(df)}')
confirm = input('\nProceed with upload? (y/n): ').strip().lower()
if confirm not in ('y', 'yes'):
    print('Aborted.')
    input('\nPress Enter to close...')
    raise SystemExit(0)

# ── Upload ────────────────────────────────────────────────────────────────────
print('\nUploading...')
ok_count   = 0
skip_count = 0
err_count  = 0
errors     = []

for i, (_, row) in enumerate(df.iterrows(), 1):
    housing_id    = str(row.get('Housing ID',      '')).strip()
    new_owner     = str(row.get('Owner',            '')).strip()
    new_date      = str(row.get('Date',             '')).strip()
    new_loc       = str(row.get('Current Location', '')).strip()
    new_type      = str(row.get('Type',             '')).strip()
    new_status    = str(row.get('Status',           '')).strip()
    new_container = str(row.get('Container',        '')).strip()

    if not housing_id:
        skip_count += 1
        continue

    loc_key       = loc_lookup.get(new_loc)       if new_loc       else None
    type_key      = type_lookup.get(new_type)      if new_type      else None
    status_key    = status_lookup.get(new_status)  if new_status    else None
    container_key = container_lookup.get(new_container) if new_container else None

    if new_loc and loc_key is None:
        print(f'  [{i:>4}] SKIP  {housing_id}  — unknown location: {new_loc!r}')
        skip_count += 1
        continue

    try:
        current = get_housing(housing_id)
        if not current:
            print(f'  [{i:>4}] SKIP  {housing_id}  — not found in Climb')
            skip_count += 1
            continue

        body = build_put_body(current, new_owner, new_date, loc_key,
                              type_key, status_key, container_key)
        r    = api_put(f'/api/housings/{housing_id}', body)

        if r.status_code == 200:
            ok_count += 1
            if i % 50 == 0 or i <= 5:
                print(f'  [{i:>4}] OK    {housing_id}')
        else:
            err_count += 1
            msg = f'{housing_id}: HTTP {r.status_code} — {r.text}'
            errors.append(msg)
            print(f'  [{i:>4}] ERROR {housing_id}: HTTP {r.status_code}')
            print(f'          {r.text[:200]}')

    except Exception as ex:
        err_count += 1
        msg = f'{housing_id}: {ex}'
        errors.append(msg)
        print(f'  [{i:>4}] ERROR {msg}')

print(f'\n{"="*60}')
print(f'  Done.  OK: {ok_count}  Skipped: {skip_count}  Errors: {err_count}')
if errors:
    print(f'\n  Errors:')
    for e in errors[:20]:
        print(f'    {e}')
print('=' * 60)
input('\nPress Enter to close...')
