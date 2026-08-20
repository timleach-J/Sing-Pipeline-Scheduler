"""
Diagnoses why PUT /api/animals/{id} returns 400 "An internal error occurred".

Tries progressively different payloads on ONE animal to find what Climb
objects to. Read-only until a variant succeeds — and the first success is
the answer, so it stops there.

Set ANIMAL_NAME below to the animal you want to test with.
"""

import json, os, sys, glob, time, traceback, requests

ANIMAL_NAME = '109122'      # <-- change if needed
NEW_MARKER  = '2R, S4'      # <-- what we're trying to set

script_dir = os.path.dirname(os.path.abspath(__file__))

try:
    cands = []
    for d in (script_dir, os.path.join(script_dir, 'lib'),
              os.path.dirname(script_dir)):
        cands += sorted(glob.glob(os.path.join(d, 'sing_climb*.py')), reverse=True)
    import importlib.util
    sp = importlib.util.spec_from_file_location('sc', cands[0])
    sc = importlib.util.module_from_spec(sp)
    sp.loader.exec_module(sc)
    print(f'Loaded: {os.path.basename(cands[0])}\n')

    def hdr():
        return {'Authorization':   f'Bearer {sc._get_token()}',
                'X-Workgroup-Key': sc._WORKGROUP_KEY,
                'Content-Type':    'application/json'}

    print(f'Fetching {ANIMAL_NAME}...')
    animals = sc._get_all('/api/animals')
    a = next((x for x in animals
              if str(x.get('animalName','')).strip() == ANIMAL_NAME), None)
    if not a:
        raise SystemExit(f'{ANIMAL_NAME} not found')

    animal_id = a.get('animalId') or a.get('animalID')
    print(f'  animalId    : {animal_id}')
    print(f'  materialKey : {a.get("materialKey")}')
    print(f'  status      : {a.get("status")}  (key {a.get("animalStatusKey")})')
    print(f'  exitReason  : {a.get("exitReason")}  (key {a.get("exitReasonKey")})')
    print(f'  dateExit    : {a.get("dateExit")}')
    print(f'  marker      : {a.get("physicalMarker")}  type {a.get("markerType")}')
    print(f'  cohorts     : {a.get("cohortsCount")}')
    print()
    print('FULL RECORD:')
    print(json.dumps(a, indent=2, default=str))
    print()

    def try_put(label, payload, url=None):
        u = url or f'{sc._API_BASE}/api/animals/{animal_id}'
        time.sleep(0.15)
        r = requests.put(u, headers=hdr(), json=payload, timeout=30)
        ok = 'OK  ' if r.ok else 'FAIL'
        print(f'  [{ok}] {label}')
        print(f'         {r.status_code}  {r.text[:200]}')
        return r.ok

    def clean(v):
        if v is None or str(v).strip() in ('', 'None', 'NaT'):
            return None
        try:
            import pandas as pd
            return pd.to_datetime(v).strftime('%Y-%m-%d')
        except Exception:
            return v

    FULL = ['alternatePhysicalID','heldFor','citesNumber','lineKey','sexKey',
            'generationKey','breedingStatusKey','dietKey','animalStatusKey',
            'exitReasonKey','animalName','dateBorn','dateExit','comments',
            'commentStatus','owner','arrivalDate','animalUseKey',
            'iacucprotocolKey','physicalMarkerTypeKey','materialOriginKey',
            'externalIdentifier','microchipIdentifier']
    DATES = {'dateBorn','dateExit','arrivalDate'}

    def base(include_none=False):
        p = {}
        for f in FULL:
            v = a.get(f)
            if f in DATES:
                v = clean(v)
            if v is not None or include_none:
                p[f] = v
        return p

    print('=' * 70)
    print(' VARIANTS')
    print('=' * 70)

    # 1 — exactly what the audit script sent
    v1 = base(); v1['physicalMarker'] = NEW_MARKER
    if try_put('1. as the audit sends it', v1): raise SystemExit('\n-> variant 1 works')

    # 2 — include omitted fields explicitly as null
    v2 = base(include_none=True); v2['physicalMarker'] = NEW_MARKER
    if try_put('2. + omitted fields as null', v2): raise SystemExit('\n-> variant 2 works')

    # 3 — add the four array fields
    v3 = base(); v3['physicalMarker'] = NEW_MARKER
    v3.update({'jobKeys': [], 'cohortKeys': [], 'housings': [],
               'animalCharacteristics': []})
    if try_put('3. + empty arrays', v3): raise SystemExit('\n-> variant 3 works')

    # 4 — carry real cohort keys rather than empty
    v4 = base(); v4['physicalMarker'] = NEW_MARKER
    v4['cohortKeys'] = [c.get('cohortKey') for c in (a.get('cohorts') or [])
                        if c.get('cohortKey')]
    if try_put('4. + real cohortKeys', v4): raise SystemExit('\n-> variant 4 works')

    # 5 — drop exit fields (is it rejecting edits to an exited animal?)
    v5 = base(); v5['physicalMarker'] = NEW_MARKER
    v5.pop('dateExit', None); v5.pop('exitReasonKey', None)
    if try_put('5. without dateExit / exitReasonKey', v5): raise SystemExit('\n-> variant 5 works')

    # 6 — marker only
    if try_put('6. physicalMarker only', {'physicalMarker': NEW_MARKER}):
        raise SystemExit('\n-> variant 6 works (PATCH-like)')

    # 7 — marker + name
    if try_put('7. physicalMarker + animalName',
               {'physicalMarker': NEW_MARKER, 'animalName': ANIMAL_NAME}):
        raise SystemExit('\n-> variant 7 works')

    # 8 — no change at all: does ANY put succeed on this animal?
    v8 = base()
    if try_put('8. unchanged record (no edit)', v8):
        raise SystemExit('\n-> PUT works, but not with the new marker value')

    # 9 — the /unique/{materialKey} route instead
    v9 = base(); v9['physicalMarker'] = NEW_MARKER
    if try_put('9. via /api/animals/unique/{materialKey}', v9,
               url=f'{sc._API_BASE}/api/animals/unique/{a.get("materialKey")}'):
        raise SystemExit('\n-> use the unique/materialKey route')

    print('\nAll variants failed. Send this output back.')

except SystemExit as e:
    if str(e) and not str(e).isdigit():
        print(e)
except Exception:
    print(traceback.format_exc())

input('\nPress ENTER to exit...')
