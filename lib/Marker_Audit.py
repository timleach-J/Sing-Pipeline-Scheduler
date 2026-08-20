"""
================================================================================
 MARKER AUDIT  —  Harvest Sheet  vs  Climb
================================================================================
 Compares the Identification recorded on the Harvest Worksheet against the
 Marker held in Climb, for adult animals that have already been harvested.

 Reports every mismatch, then offers to update Climb to match the Harvest Sheet.
 Nothing is written until you confirm, and a CSV of the differences is saved
 either way.

 The Harvest Sheet is treated as the source of truth — it is what was physically
 recorded at harvest.

 SETUP
 -----
   Put in the same folder (or a lib subfolder for the first two):
     - sing_climb*.py
     - sing_credentials.json
     - Sing Harvest Sheet.xlsx
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
    # Which animals to audit
    'adults_only':     True,      # skip P14 — they have no RapID tag
    'harvested_only':  True,      # only animals whose harvest date has passed

    # Set to 'RapID' to also change Marker Type when applying a fix.
    # None leaves Marker Type untouched — safer, and the default.
    'set_marker_type': None,

    'request_delay':   0.12,
}

HARVEST_TAB = 'Harvest Worksheet'

COL_NAME    = 'Name'
COL_IDENT   = 'Identification'
COL_AGE     = 'Age (Days)'
COL_HARVEST = 'Harvest Date'

PRESERVE = [
    'alternatePhysicalID', 'heldFor', 'citesNumber', 'lineKey', 'sexKey',
    'generationKey', 'breedingStatusKey', 'dietKey', 'animalStatusKey',
    'exitReasonKey', 'animalName', 'dateBorn', 'dateExit', 'comments',
    'commentStatus', 'owner', 'arrivalDate', 'animalUseKey',
    'iacucprotocolKey', 'physicalMarkerTypeKey', 'materialOriginKey',
    'externalIdentifier', 'microchipIdentifier',
]


def norm(v) -> str:
    """Normalise a marker for comparison: trim, collapse spaces, upper."""
    s = str(v or '').strip()
    if s.lower() in ('nan', 'none', 'nat'):
        return ''
    return ' '.join(s.split()).upper()


# NOTE: dates are NOT reformatted. Climb stores birth dates at 16:00 UTC so
# they render correctly in Eastern; rewriting them as date-only sets 00:00 UTC,
# which displays as the PREVIOUS day. Anything merely passing through goes back
# exactly as it arrived.


def ask(prompt):
    return input(prompt).strip() == 'YES'


def bail(msg='Aborted.'):
    print(msg)
    input('\nPress ENTER to exit...')
    raise SystemExit(0)


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    stamp      = datetime.now().strftime('%Y%m%d_%H%M%S')

    print('=' * 74)
    print(' MARKER AUDIT — Harvest Sheet vs Climb')
    print('=' * 74)

    # -- Load the API module --------------------------------------------------
    cands = []
    for d in (script_dir,
              os.path.join(script_dir, 'lib'),
              os.path.dirname(script_dir)):
        cands += sorted(glob.glob(os.path.join(d, 'sing_climb*.py')), reverse=True)
    if not cands:
        raise FileNotFoundError(
            'sing_climb*.py not found in this folder, lib\\, or the parent folder')

    import importlib.util
    sp = importlib.util.spec_from_file_location('sing_climb', cands[0])
    sc = importlib.util.module_from_spec(sp)
    sp.loader.exec_module(sc)
    print(f'API module : {os.path.basename(cands[0])}')

    def headers():
        return {'Authorization':   f'Bearer {sc._get_token()}',
                'X-Workgroup-Key': sc._WORKGROUP_KEY,
                'Content-Type':    'application/json'}

    # -- Harvest Sheet --------------------------------------------------------
    # Look in this folder, its parent (in case the script sits in lib\), and a
    # lib subfolder. Outputs are written next to whichever copy is found, so
    # reports land in the main folder rather than lib\.
    search_dirs = [
        script_dir,
        os.path.dirname(script_dir),
        os.path.join(script_dir, 'lib'),
    ]
    hs_path = None
    for d in search_dirs:
        p = os.path.join(d, 'Sing Harvest Sheet.xlsx')
        if os.path.exists(p):
            hs_path = p
            break

    if not hs_path:
        raise FileNotFoundError(
            'Sing Harvest Sheet.xlsx not found. Searched:\n  ' +
            '\n  '.join(search_dirs))

    out_dir = os.path.dirname(hs_path)
    print(f'Harvest    : {hs_path}')

    hs = pd.read_excel(hs_path, sheet_name=HARVEST_TAB, dtype=str).fillna('')
    print(f'             {len(hs)} worksheet rows')

    for c in (COL_NAME, COL_IDENT):
        if c not in hs.columns:
            raise ValueError(f"Harvest Worksheet has no '{c}' column.\n"
                             f'Found: {list(hs.columns)}')

    # -- Filter ---------------------------------------------------------------
    before = len(hs)
    if CONFIG['adults_only'] and COL_AGE in hs.columns:
        hs = hs[~hs[COL_AGE].astype(str).str.strip().str.upper().str.startswith('P14')]
        print(f'  adults only     : {len(hs)} rows  (dropped {before - len(hs)})')

    if CONFIG['harvested_only'] and COL_HARVEST in hs.columns:
        n = len(hs)
        hd = pd.to_datetime(hs[COL_HARVEST], errors='coerce')
        hs = hs[hd.notna() & (hd <= pd.Timestamp.now())]
        print(f'  already harvested: {len(hs)} rows  (dropped {n - len(hs)})')

    # Keep only rows with a real animal name
    hs = hs[hs[COL_NAME].astype(str).str.strip() != '']
    print(f'  auditable       : {len(hs)} rows')

    if hs.empty:
        bail('\nNothing to audit.')

    # -- Climb ----------------------------------------------------------------
    print('\nLoading animals from Climb...')
    animals = sc._get_all('/api/animals')
    by_name = {}
    type_keys = {}
    for a in animals:
        nm = str(a.get('animalName', '')).strip()
        if nm:
            by_name[nm] = a
        if a.get('physicalMarkerTypeKey') and a.get('markerType'):
            type_keys[a['markerType']] = a['physicalMarkerTypeKey']
    print(f'  {len(by_name)} animals')

    new_type_key = None
    if CONFIG['set_marker_type']:
        if CONFIG['set_marker_type'] not in type_keys:
            raise ValueError(f"Marker type '{CONFIG['set_marker_type']}' not in Climb.\n"
                             f'Available: {sorted(type_keys)}')
        new_type_key = type_keys[CONFIG['set_marker_type']]

    # -- Compare --------------------------------------------------------------
    rows, mismatches, not_found, blank_sheet = [], [], [], []

    for _, r in hs.iterrows():
        name  = str(r[COL_NAME]).strip()
        sheet = str(r[COL_IDENT]).strip()

        climb = by_name.get(name)
        if not climb:
            not_found.append(name)
            rows.append({'Animal': name, 'Harvest Sheet': sheet,
                         'Climb': '', 'Marker Type': '',
                         'Verdict': 'NOT IN CLIMB',
                         'Harvest Date': str(r.get(COL_HARVEST, '')).strip()})
            continue

        current = str(climb.get('physicalMarker') or '').strip()

        if sheet == '':
            blank_sheet.append(name)
            verdict = 'NO IDENTIFICATION ON SHEET'
        elif norm(sheet) == norm(current):
            verdict = 'MATCH'
        else:
            verdict = 'MISMATCH'
            mismatches.append({
                'name':  name,
                'climb': climb,
                'sheet': sheet,
                'was':   current,
            })

        rows.append({
            'Animal':        name,
            'Harvest Sheet': sheet,
            'Climb':         current,
            'Marker Type':   climb.get('markerType', ''),
            'Verdict':       verdict,
            'Harvest Date':  str(r.get(COL_HARVEST, '')).strip(),
        })

    report = pd.DataFrame(rows)
    counts = report['Verdict'].value_counts().to_dict()

    print('\n' + '=' * 74)
    print(' RESULTS')
    print('=' * 74)
    for verdict in ('MATCH', 'MISMATCH', 'NOT IN CLIMB',
                    'NO IDENTIFICATION ON SHEET'):
        if verdict in counts:
            print(f'  {verdict:28} {counts[verdict]}')

    # -- Save the report ------------------------------------------------------
    out_csv = os.path.join(out_dir, f'marker_audit_{stamp}.csv')
    report.sort_values(['Verdict', 'Animal']).to_csv(out_csv, index=False)
    print(f'\nReport: {os.path.basename(out_csv)}')

    if not mismatches:
        print('\nNo mismatches — nothing to fix.')
        input('\nPress ENTER to exit...')
        return

    # -- Show the mismatches --------------------------------------------------
    print('\n' + '=' * 74)
    print(f' {len(mismatches)} MISMATCH(ES) — Climb would be changed to match the sheet')
    print('=' * 74)
    print(f'  {"animal":10}  {"climb has":22}  {"sheet says":22}')
    print(f'  {"-"*10}  {"-"*22}  {"-"*22}')
    for m in mismatches[:40]:
        print(f'  {m["name"]:10}  {(m["was"] or "(blank)"):22}  {m["sheet"]:22}')
    if len(mismatches) > 40:
        print(f'  ... and {len(mismatches) - 40} more (see the CSV)')

    if CONFIG['set_marker_type']:
        print(f'\n  Marker Type will also be set to: {CONFIG["set_marker_type"]}')
    else:
        print('\n  Marker Type will be left unchanged.')

    print('\nReview the CSV before continuing. Climb will be updated to match')
    print('the Harvest Sheet — the sheet is treated as correct.')

    if not ask(f'\nType YES to update {len(mismatches)} animals in Climb: '):
        bail('\nNothing was changed.')

    # -- Apply ----------------------------------------------------------------
    print('\n' + '=' * 74)
    print(' UPDATING')
    print('=' * 74)

    done, failed = [], []
    qc = False

    for i, m in enumerate(mismatches):
        tag   = f'[{i+1}/{len(mismatches)}]'
        climb = m['climb']

        payload = {}
        for f in PRESERVE:
            v = climb.get(f)
            if v is not None:
                payload[f] = v
        payload['physicalMarker'] = m['sheet']
        if new_type_key is not None:
            payload['physicalMarkerTypeKey'] = new_type_key

        # Climb rejects the PUT unless these four arrays are present.
        # cohortKeys MUST carry the animal's real cohorts — sending [] removes
        # the animal from every cohort it belongs to.
        payload['cohortKeys'] = [c.get('cohortKey')
                                 for c in (climb.get('cohorts') or [])
                                 if c.get('cohortKey') is not None]
        payload['jobKeys']               = []
        payload['housings']              = []
        payload['animalCharacteristics'] = []

        if not qc:
            print('\n--- QC: first animal ---')
            print(f'  Animal : {m["name"]}')
            print(f'  Marker : {m["was"] or "(blank)"}  ->  {m["sheet"]}')
            print(f'  Payload: {json.dumps(payload, indent=4)}')
            print('\n  Every other field is carried over unchanged from Climb.')
            if not ask('\n  Type YES to apply this one: '):
                bail('\nNothing was changed.')

        animal_id = climb.get('animalId') or climb.get('animalID')
        try:
            time.sleep(CONFIG['request_delay'])
            r = requests.put(f'{sc._API_BASE}/api/animals/{animal_id}',
                             headers=headers(), json=payload, timeout=30)
            if not r.ok:
                raise requests.HTTPError(f'{r.status_code} {r.text[:200]}')
            print(f'{tag} OK   {m["name"]}  {m["was"] or "(blank)"} -> {m["sheet"]}')
            done.append({'animal': m['name'], 'was': m['was'], 'now': m['sheet']})
        except Exception as e:
            print(f'{tag} FAIL {m["name"]}: {e}')
            failed.append({'animal': m['name'], 'error': str(e)})

        if not qc:
            qc = True
            print('\n  Check this animal in Climb — confirm the marker AND that')
            print('  line, sex, birth date and owner are unchanged.')
            if not ask('  Type YES to update the rest: '):
                bail('Stopped after QC. Only one animal was updated.')
            print()

    # -- Summary --------------------------------------------------------------
    print('\n' + '=' * 74)
    print(' COMPLETE')
    print('=' * 74)
    print(f'  Updated : {len(done)}')
    print(f'  Failed  : {len(failed)}')
    for f in failed:
        print(f'    {f["animal"]}: {f["error"]}')

    log_path = os.path.join(out_dir, f'marker_audit_applied_{stamp}.json')
    with open(log_path, 'w') as f:
        json.dump({'run_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                   'updated': done, 'failed': failed,
                   'not_in_climb': not_found,
                   'blank_on_sheet': blank_sheet}, f, indent=2, default=str)
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
                         'marker_audit_error.txt')
        with open(p, 'w') as f:
            f.write(err)
        print(f'\nDetails written to: {p}')

    input('\nPress ENTER to exit...')
