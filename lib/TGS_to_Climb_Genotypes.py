"""
================================================================================
 TGS -> CLIMB  —  GENOTYPE UPLOAD
================================================================================
 Reads TGS typing reports and writes the genotype calls into Climb.

 Drop every TGS_Typing_*.xls into this folder and run. The script parses them
 all, shows you what it found, and uploads on confirmation.

 HOW THE MAPPING WORKS
 ---------------------
   TGS 'Pedigree #'  ->  Climb animalName        (NOT 'Mouse Id' — that's the
                                                  ear notch)
   TGS call column   ->  genotype symbol
       wild          ->  +/+
       het           ->  -/+
       hom           ->  -/-
       INCONCLUSIVE  ->  Inconclusive
   TGS assay name    ->  Climb assay, matched after stripping < >
                         'Shank3tm2Gfng Probe' = 'Shank3<tm2Gfng> Probe'

 SAFETY
 ------
 Climb does not deduplicate genotypes — posting twice leaves two identical
 records. This script reads existing genotypes first and skips any animal that
 already has the same assay and call.

 SETUP
 -----
   In this folder (or a lib subfolder for the first two):
     - sing_climb*.py
     - sing_credentials.json
     - TGS_Typing_*.xls
================================================================================
"""

import json
import os
import sys
import glob
import time
import traceback
from datetime import datetime

import requests
import pandas as pd


# ==============================================================================
# CONFIGURATION
# ==============================================================================

CONFIG = {
    # Which date to record against the genotype:
    #   'completed' — when TGS finished the assay  (recommended)
    #   'sampled'   — the TGS sampling date
    #   'today'     — the date you run this
    'genotype_date': 'completed',

    'request_delay': 0.12,
}

# TGS call column -> the genotype symbol name in Climb
CALL_COLUMNS = {
    'wild':         '+/+',
    'het':          '-/+',
    'hom':          '-/-',
    'inconclusive': 'Inconclusive',
}

DATA_TABLE_INDEX = 5     # which read_html table holds the calls
INFO_TABLE_INDEX = 1     # submitter / strain / sampling date
LOG_TABLE_INDEX  = 9     # submitted / received / completed timestamps


def norm_assay(name: str) -> str:
    """Strip < > and whitespace so TGS and Climb assay names compare equal."""
    return (str(name).replace('<', '').replace('>', '')
            .replace(' ', '').strip().lower())


def ask(prompt):
    return input(prompt).strip() == 'YES'


def bail(msg='Aborted.'):
    print(msg)
    input('\nPress ENTER to exit...')
    raise SystemExit(0)


# ==============================================================================
# Parsing
# ==============================================================================

def parse_tgs_report(path):
    """
    Pull the genotype calls out of one TGS typing report.

    Returns (records, meta) where records is a list of
    {animal, notch, assay, symbol, sex, dob} and meta carries request-level
    detail for the log.
    """
    tables = pd.read_html(path)

    meta = {'file': os.path.basename(path), 'strain': '',
            'sampled': '', 'completed': ''}

    # -- request info ---------------------------------------------------------
    try:
        info = tables[INFO_TABLE_INDEX]
        for _, r in info.iterrows():
            label = str(r[0]).strip().rstrip(':').lower()
            if label == 'strain':
                meta['strain'] = str(r[1]).strip()
            elif label == 'sampler' and len(r) > 3:
                meta['sampled'] = str(r[3]).strip()
    except Exception:
        pass

    # -- completion timestamp -------------------------------------------------
    try:
        log = tables[LOG_TABLE_INDEX]
        done = log[log[0].astype(str).str.strip() == 'Completed']
        if not done.empty:
            meta['completed'] = str(done.iloc[-1][1]).strip()
    except Exception:
        pass

    # -- the calls ------------------------------------------------------------
    t = tables[DATA_TABLE_INDEX]
    assay_row  = t.iloc[0]
    header_row = t.iloc[1]
    body       = t.iloc[2:]

    headers = {}
    for i, h in enumerate(header_row):
        key = str(h).strip().lower()
        if key and key != 'nan':
            headers.setdefault(key, []).append(i)

    def col(name):
        idx = headers.get(name)
        return idx[0] if idx else None

    c_animal = col('pedigree #')
    c_notch  = col('mouse id')
    c_sex    = col('sex')
    c_dob    = col('date of birth')

    if c_animal is None:
        raise ValueError(f"{os.path.basename(path)}: no 'Pedigree #' column. "
                         f"Headers: {sorted(headers)}")

    # Every call column, paired with the assay named above it
    call_cols = []
    for label, symbol in CALL_COLUMNS.items():
        for i in headers.get(label, []):
            assay = str(assay_row.iloc[i]).strip()
            if assay and assay.lower() != 'nan':
                call_cols.append((i, symbol, assay))

    if not call_cols:
        raise ValueError(f'{os.path.basename(path)}: no call columns found.')

    assays = sorted({a for _, _, a in call_cols})
    meta['assays'] = assays

    records = []
    for _, row in body.iterrows():
        animal = str(row.iloc[c_animal]).strip()
        if not animal or animal.lower() == 'nan':
            continue

        hits = []
        for i, symbol, assay in call_cols:
            v = row.iloc[i]
            if pd.notna(v) and str(v).strip() != '':
                hits.append((assay, symbol, str(v).strip()))

        if not hits:
            records.append({'animal': animal, 'assay': assays[0],
                            'symbol': None, 'raw': '',
                            'notch': str(row.iloc[c_notch]).strip() if c_notch is not None else '',
                            'sex':   str(row.iloc[c_sex]).strip()   if c_sex   is not None else '',
                            'dob':   str(row.iloc[c_dob]).strip()   if c_dob   is not None else ''})
            continue

        for assay, symbol, raw in hits:
            records.append({
                'animal': animal,
                'assay':  assay,
                'symbol': symbol,
                'raw':    raw,
                'notch':  str(row.iloc[c_notch]).strip() if c_notch is not None else '',
                'sex':    str(row.iloc[c_sex]).strip()   if c_sex   is not None else '',
                'dob':    str(row.iloc[c_dob]).strip()   if c_dob   is not None else '',
            })

    return records, meta


def pick_date(meta) -> str:
    """Genotype date in Climb's required YYYYMMDD format."""
    mode = CONFIG['genotype_date']
    raw  = {'completed': meta.get('completed'),
            'sampled':   meta.get('sampled')}.get(mode)
    if mode != 'today' and raw:
        try:
            return pd.to_datetime(raw, dayfirst=False).strftime('%Y%m%d')
        except Exception:
            pass
    return datetime.now().strftime('%Y%m%d')


# ==============================================================================
# Main
# ==============================================================================

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    stamp      = datetime.now().strftime('%Y%m%d_%H%M%S')

    print('=' * 74)
    print(' TGS -> CLIMB — GENOTYPE UPLOAD')
    print('=' * 74)

    # -- API module -----------------------------------------------------------
    cands = []
    for d in (script_dir, os.path.join(script_dir, 'lib'),
              os.path.dirname(script_dir),
              os.path.join(os.path.dirname(script_dir), 'lib')):
        cands += sorted(glob.glob(os.path.join(d, 'sing_climb*.py')), reverse=True)
    if not cands:
        raise FileNotFoundError('sing_climb*.py not found nearby.')

    import importlib.util
    sp = importlib.util.spec_from_file_location('sing_climb', cands[0])
    sc = importlib.util.module_from_spec(sp)
    sp.loader.exec_module(sc)
    print(f'API module : {os.path.basename(cands[0])}')

    def headers():
        return {'Authorization':   f'Bearer {sc._get_token()}',
                'X-Workgroup-Key': sc._WORKGROUP_KEY,
                'Content-Type':    'application/json'}

    # -- Reports --------------------------------------------------------------
    # Look in this folder, a 'genotypes' subfolder, and the parent's
    # 'genotypes' folder — so the script works from either location.
    search_dirs = [
        os.path.join(script_dir, 'genotypes'),
        script_dir,
        os.path.join(os.path.dirname(script_dir), 'genotypes'),
    ]
    reports, seen = [], set()
    for d in search_dirs:
        if not os.path.isdir(d):
            continue
        for p in sorted(glob.glob(os.path.join(d, 'TGS_Typing_*.xls'))):
            real = os.path.realpath(p)
            if real not in seen:
                seen.add(real)
                reports.append(p)

    if not reports:
        raise FileNotFoundError(
            'No TGS_Typing_*.xls found. Searched:\n  ' +
            '\n  '.join(search_dirs) +
            '\n\nDownload the typing reports from TGS and drop them in the '
            'genotypes folder.')

    out_dir = os.path.dirname(reports[0])
    print(f'Reports    : {len(reports)}  (from {out_dir})')

    all_records, metas, parse_errors = [], [], []
    for p in reports:
        try:
            recs, meta = parse_tgs_report(p)
            meta['date'] = pick_date(meta)
            for r in recs:
                r['_file'] = meta['file']
                r['_date'] = meta['date']
            all_records += recs
            metas.append(meta)
            called = sum(1 for r in recs if r['symbol'])
            print(f"  {meta['file']}: {called} call(s), "
                  f"assay {', '.join(meta.get('assays', []))}, date {meta['date']}")
        except Exception as e:
            parse_errors.append((os.path.basename(p), str(e)))
            print(f'  {os.path.basename(p)}: PARSE FAILED — {e}')

    called   = [r for r in all_records if r['symbol']]
    no_call  = [r for r in all_records if not r['symbol']]
    print(f'\n  {len(called)} call(s) across {len(reports)} report(s)')
    if no_call:
        print(f'  {len(no_call)} row(s) with no result — skipped: '
              f'{[r["animal"] for r in no_call][:8]}')

    if not called:
        bail('\nNothing to upload.')

    # -- Climb lookups --------------------------------------------------------
    print('\nLoading Climb...')
    animals = sc._get_all('/api/animals')
    by_name = {str(a.get('animalName', '')).strip(): a for a in animals
               if str(a.get('animalName', '')).strip()}
    print(f'  {len(by_name)} animals')

    def get_vocab(endpoint):
        out, page = {}, 1
        while True:
            time.sleep(CONFIG['request_delay'])
            r = requests.get(f'{sc._API_BASE}{endpoint}', headers=headers(),
                             params={'pageNumber': page, 'pageSize': 100},
                             timeout=30)
            r.raise_for_status()
            body = r.json().get('data', {})
            for item in body.get('items', []):
                out[item['name']] = int(item['key'])
            if page >= body.get('pageCount', 1):
                break
            page += 1
        return out

    assay_keys  = get_vocab('/api/vocabulary/genotypeAssay')
    symbol_keys = get_vocab('/api/vocabulary/genotypeSymbol')
    print(f'  {len(assay_keys)} assays, {len(symbol_keys)} symbols')

    assay_lookup = {norm_assay(k): (k, v) for k, v in assay_keys.items()}

    # Existing genotypes, so we don't double-post
    existing = set()
    for g in sc._get_all('/api/genotypes'):
        nm = str(g.get('animalName', '')).strip()
        if nm:
            existing.add((nm, norm_assay(g.get('assay', '')),
                          str(g.get('genotype', '')).strip()))
    print(f'  {len(existing)} existing genotype records')

    # -- Build the work list --------------------------------------------------
    jobs, problems, dupes = [], [], []

    for r in called:
        animal = r['animal']
        climb  = by_name.get(animal)
        if not climb:
            problems.append(f'  {animal}: not in Climb')
            continue

        hit = assay_lookup.get(norm_assay(r['assay']))
        if not hit:
            problems.append(f"  {animal}: assay '{r['assay']}' not in Climb")
            continue
        climb_assay, assay_key = hit

        if r['symbol'] not in symbol_keys:
            problems.append(f"  {animal}: symbol '{r['symbol']}' not in Climb")
            continue
        symbol_key = symbol_keys[r['symbol']]

        if (animal, norm_assay(climb_assay), r['symbol']) in existing:
            dupes.append(animal)
            continue

        jobs.append({
            'animal':      animal,
            'animal_id':   climb.get('animalId') or climb.get('animalID'),
            'assay':       climb_assay,
            'assay_key':   assay_key,
            'symbol':      r['symbol'],
            'symbol_key':  symbol_key,
            'date':        r['_date'],
            'notch':       r['notch'],
            'file':        r['_file'],
        })

    # -- Report ---------------------------------------------------------------
    print('\n' + '=' * 74)
    print(' PLAN')
    print('=' * 74)
    print(f'  To upload        : {len(jobs)}')
    print(f'  Already in Climb : {len(dupes)}  (skipped)')
    print(f'  Problems         : {len(problems)}')
    for p in problems[:15]:
        print(p)
    if len(problems) > 15:
        print(f'  ... and {len(problems) - 15} more')

    out_csv = os.path.join(out_dir, f'tgs_genotype_upload_{stamp}.csv')
    pd.DataFrame(jobs).to_csv(out_csv, index=False)
    print(f'\nPlan saved: {os.path.basename(out_csv)}')

    if not jobs:
        bail('\nNothing to upload.')

    by_symbol = {}
    for j in jobs:
        by_symbol[j['symbol']] = by_symbol.get(j['symbol'], 0) + 1
    print('\n  Breakdown:')
    for s, n in sorted(by_symbol.items()):
        print(f'    {s:14} {n}')

    print(f'\n  First 10:')
    print(f'    {"animal":10} {"notch":6} {"assay":26} {"call":14} date')
    for j in jobs[:10]:
        print(f'    {j["animal"]:10} {j["notch"]:6} {j["assay"][:26]:26} '
              f'{j["symbol"]:14} {j["date"]}')

    if not ask(f'\nType YES to upload {len(jobs)} genotypes: '):
        bail()

    # -- Upload ---------------------------------------------------------------
    print('\n' + '=' * 74)
    print(' UPLOADING')
    print('=' * 74)

    done, failed = [], []
    qc = False

    for i, j in enumerate(jobs):
        tag = f'[{i+1}/{len(jobs)}]'
        payload = {'genotypeRequestDtos': [{
            'animalID': j['animal_id'],
            'genotypes': [{
                'date':              j['date'],
                'genotypeAssayKey':  j['assay_key'],
                'genotypeSymbolKey': j['symbol_key'],
            }]
        }]}

        if not qc:
            print('\n--- QC: first genotype ---')
            print(f'  Animal : {j["animal"]}  (animalID {j["animal_id"]})')
            print(f'  Assay  : {j["assay"]}  (key {j["assay_key"]})')
            print(f'  Call   : {j["symbol"]}  (key {j["symbol_key"]})')
            print(f'  Date   : {j["date"]}')
            print(f'  Payload: {json.dumps(payload, indent=4)}')
            if not ask('\n  Type YES to post this one: '):
                bail()

        try:
            time.sleep(CONFIG['request_delay'])
            r = requests.post(f'{sc._API_BASE}/api/genotypes',
                              headers=headers(), json=payload, timeout=30)
            if not r.ok:
                raise requests.HTTPError(f'{r.status_code} {r.text[:200]}')
            print(f'{tag} OK   {j["animal"]}  {j["assay"]}  {j["symbol"]}')
            done.append(j)
        except Exception as e:
            print(f'{tag} FAIL {j["animal"]}: {e}')
            failed.append({'animal': j['animal'], 'error': str(e)})

        if not qc:
            qc = True
            print('\n  Check this animal in Climb before continuing.')
            if not ask('  Type YES to upload the rest: '):
                bail('Stopped after QC. Only one genotype was posted.')
            print()

    # -- Summary --------------------------------------------------------------
    print('\n' + '=' * 74)
    print(' COMPLETE')
    print('=' * 74)
    print(f'  Uploaded : {len(done)}')
    print(f'  Skipped  : {len(dupes)}')
    print(f'  Failed   : {len(failed)}')
    for f in failed:
        print(f'    {f["animal"]}: {f["error"]}')

    log_path = os.path.join(out_dir, f'tgs_genotype_upload_{stamp}.json')
    with open(log_path, 'w') as f:
        json.dump({'run_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                   'reports':  metas,
                   'uploaded': done, 'skipped_duplicates': dupes,
                   'failed':   failed, 'problems': problems,
                   'parse_errors': parse_errors}, f, indent=2, default=str)
    print(f'\nLog: {os.path.basename(log_path)}')


if __name__ == '__main__':
    try:
        main()
    except SystemExit:
        pass
    except Exception:
        err = traceback.format_exc()
        print('\n--- ERROR ---')
        print(err)
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         'tgs_genotype_error.txt')
        with open(p, 'w') as f:
            f.write(err)
        print(f'\nDetails written to: {p}')

    input('\nPress ENTER to exit...')
