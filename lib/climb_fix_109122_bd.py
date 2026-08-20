"""
Restores the birth date on animal 109122.

The diagnostic PUT rewrote dateBorn as date-only, which Climb stored as
00:00 UTC — 8pm Eastern the previous day — so the UI now shows 6/13/26
instead of 6/14/26.

Sets it back to 2026-06-14T16:00:00 (noon Eastern), which is how Climb
stores birth dates.
"""
import json, os, glob, traceback, requests

ANIMAL_NAME  = '109122'
CORRECT_BORN = '2026-06-14T16:00:00'

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

    print(f'  dateBorn now : {a.get("dateBorn")}')
    print(f'  should be    : {CORRECT_BORN}')
    print(f'  marker       : {a.get("physicalMarker")}   (leave as is)')

    if a.get('dateBorn') == CORRECT_BORN:
        print('\nAlready correct — nothing to do.')
        raise SystemExit(0)

    if input('\nType YES to fix the birth date: ').strip() != 'YES':
        raise SystemExit('Aborted.')

    FULL = ['alternatePhysicalID','heldFor','citesNumber','lineKey','sexKey',
            'generationKey','breedingStatusKey','dietKey','animalStatusKey',
            'exitReasonKey','animalName','physicalMarker','dateExit','comments',
            'commentStatus','owner','arrivalDate','animalUseKey',
            'iacucprotocolKey','physicalMarkerTypeKey','materialOriginKey',
            'externalIdentifier','microchipIdentifier']

    p = {f: a[f] for f in FULL if a.get(f) is not None}
    p['dateBorn']   = CORRECT_BORN
    p['cohortKeys'] = [c.get('cohortKey') for c in (a.get('cohorts') or [])
                       if c.get('cohortKey') is not None]
    p['jobKeys'] = p['housings'] = p['animalCharacteristics'] = []

    aid = a.get('animalId') or a.get('animalID')
    r = requests.put(f'{sc._API_BASE}/api/animals/{aid}',
                     headers=hdr(), json=p, timeout=30)
    print(f'\n  {r.status_code}  {r.text[:200]}')
    if r.ok:
        print('  Fixed — check the Climb UI shows 6/14/26.')

except SystemExit as e:
    if str(e) and not str(e).isdigit():
        print(e)
except Exception:
    print(traceback.format_exc())

input('\nPress ENTER to exit...')
