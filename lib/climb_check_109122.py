"""
Checks whether the diagnostic PUT stripped cohort or housing data from 109122,
and restores the cohort if so.
"""
import json, os, sys, glob, time, traceback, requests

ANIMAL_NAME   = '109122'
EXPECTED_COHORT_KEY  = 221            # P56 2026_07_29
EXPECTED_COHORT_NAME = 'P56 2026_07_29'

script_dir = os.path.dirname(os.path.abspath(__file__))

try:
    cands = []
    for d in (script_dir, os.path.join(script_dir, 'lib'), os.path.dirname(script_dir)):
        cands += sorted(glob.glob(os.path.join(d, 'sing_climb*.py')), reverse=True)
    import importlib.util
    sp = importlib.util.spec_from_file_location('sc', cands[0])
    sc = importlib.util.module_from_spec(sp)
    sp.loader.exec_module(sc)

    def hdr():
        return {'Authorization':   f'Bearer {sc._get_token()}',
                'X-Workgroup-Key': sc._WORKGROUP_KEY,
                'Content-Type':    'application/json'}

    a = next((x for x in sc._get_all('/api/animals')
              if str(x.get('animalName','')).strip() == ANIMAL_NAME), None)
    if not a:
        raise SystemExit(f'{ANIMAL_NAME} not found')

    print('=' * 60)
    print(f' STATE OF {ANIMAL_NAME} AFTER THE DIAGNOSTIC')
    print('=' * 60)
    print(f'  marker        : {a.get("physicalMarker")}   (was 2R, expected "2R, S4")')
    print(f'  markerType    : {a.get("markerType")}')
    print(f'  cohortsCount  : {a.get("cohortsCount")}      (was 1)')
    print(f'  housingCount  : {a.get("housingCount")}      (was 3)')
    print(f'  genotypesCount: {a.get("genotypesCount")}      (was 1)')
    print(f'  studiesCount  : {a.get("studiesCount")}      (was 1)')
    print(f'  status        : {a.get("status")}')
    print(f'  dateBorn      : {a.get("dateBorn")}')
    print(f'  owner         : {a.get("owner")}')
    print()
    print('  cohorts now:')
    for c in (a.get('cohorts') or []):
        print(f'    {c.get("cohortKey")}  {c.get("name")}')
    if not a.get('cohorts'):
        print('    (none)')

    lost_cohort  = not any(c.get('cohortKey') == EXPECTED_COHORT_KEY
                           for c in (a.get('cohorts') or []))
    lost_housing = (a.get('housingCount') or 0) < 3

    print()
    print('=' * 60)
    if lost_cohort or lost_housing:
        print(' DATA WAS LOST')
        if lost_cohort:
            print(f'   - cohort {EXPECTED_COHORT_NAME} removed')
        if lost_housing:
            print(f'   - housingCount dropped to {a.get("housingCount")} (was 3)')
    else:
        print(' NOTHING LOST — cohort and housing intact')
    print('=' * 60)

    if lost_cohort:
        if input('\nType YES to restore the cohort: ').strip() == 'YES':
            def clean(v):
                if v is None or str(v).strip() in ('', 'None', 'NaT'):
                    return None
                import pandas as pd
                try:    return pd.to_datetime(v).strftime('%Y-%m-%d')
                except Exception: return v

            FULL = ['alternatePhysicalID','heldFor','citesNumber','lineKey','sexKey',
                    'generationKey','breedingStatusKey','dietKey','animalStatusKey',
                    'exitReasonKey','animalName','physicalMarker','dateBorn','dateExit',
                    'comments','commentStatus','owner','arrivalDate','animalUseKey',
                    'iacucprotocolKey','physicalMarkerTypeKey','materialOriginKey',
                    'externalIdentifier','microchipIdentifier']
            DATES = {'dateBorn','dateExit','arrivalDate'}
            p = {}
            for f in FULL:
                v = a.get(f)
                if f in DATES: v = clean(v)
                if v is not None: p[f] = v
            p['cohortKeys']            = [EXPECTED_COHORT_KEY]
            p['jobKeys']               = []
            p['housings']              = []
            p['animalCharacteristics'] = []

            aid = a.get('animalId') or a.get('animalID')
            r = requests.put(f'{sc._API_BASE}/api/animals/{aid}',
                             headers=hdr(), json=p, timeout=30)
            print(f'  {r.status_code}  {r.text[:200]}')
            if r.ok:
                print('  Restored. Re-run this script to confirm.')

except SystemExit as e:
    print(e)
except Exception:
    print(traceback.format_exc())

input('\nPress ENTER to exit...')
