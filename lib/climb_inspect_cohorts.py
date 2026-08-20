"""
Inspects Climb cohorts: how they're named, what a cohort record looks like,
what endpoints exist for them, and how animal->cohort membership is expressed.

Reads climb_swagger.json if present, else fetches it.
"""

import json, os, sys, glob, traceback, requests, time

script_dir = os.path.dirname(os.path.abspath(__file__))

try:
    sys.path.insert(0, script_dir)
    cands  = sorted(glob.glob(os.path.join(script_dir, 'sing_climb*.py')), reverse=True)
    cands += sorted(glob.glob(os.path.join(script_dir, 'lib', 'sing_climb*.py')), reverse=True)
    import importlib.util
    sp = importlib.util.spec_from_file_location('sc', cands[0])
    sc = importlib.util.module_from_spec(sp)
    sp.loader.exec_module(sc)
    print(f'Loaded: {os.path.basename(cands[0])}\n')

    hdr = {'Authorization': f'Bearer {sc._get_token()}',
           'X-Workgroup-Key': sc._WORKGROUP_KEY}

    # ---- 1. cohort endpoints in the spec --------------------------------
    spec_path = os.path.join(script_dir, 'climb_swagger.json')
    if os.path.exists(spec_path):
        swagger = json.load(open(spec_path))
    else:
        r = requests.get('https://api.climb.bio/swagger/v1/swagger.json',
                         headers=hdr, timeout=30)
        r.raise_for_status()
        swagger = r.json()
        json.dump(swagger, open(spec_path, 'w'), indent=2)

    print('=== Endpoints mentioning "cohort" ===')
    for path, methods in swagger.get('paths', {}).items():
        if 'cohort' in path.lower():
            for verb in methods:
                if verb in ('get', 'post', 'put', 'patch', 'delete'):
                    print(f'  {verb.upper():6} {path}')
    print()

    # ---- 2. a real cohort record ----------------------------------------
    print('=== Sample cohort records ===')
    cohorts = sc._get_all('/api/cohorts')
    print(f'{len(cohorts)} cohorts total\n')
    if cohorts:
        print('Full record for one cohort:')
        print(json.dumps(cohorts[0], indent=2, default=str))
        print()

    p56 = [c for c in cohorts
           if str(c.get('name','')).strip().upper().startswith('P56')]
    print(f'P56 cohorts: {len(p56)}')
    for c in sorted(p56, key=lambda x: str(x.get('name','')), reverse=True)[:10]:
        print(f"  key={c.get('cohortKey') or c.get('key')}  "
              f"{c.get('name')}  ({c.get('animalCount')} animals)")
    print()

    # ---- 3. how an animal expresses cohort membership -------------------
    print('=== Animal cohort fields ===')
    animals = sc._get_all('/api/animals')
    with_c  = [a for a in animals if a.get('cohortsCount')]
    print(f'{len(with_c)} animals belong to at least one cohort')
    if with_c:
        a = with_c[0]
        print(f"\nAnimal {a.get('animalName')}:")
        print(f"  cohortsCount : {a.get('cohortsCount')}")
        print(f"  cohorts      : {json.dumps(a.get('cohorts'), indent=2, default=str)}")

except Exception:
    print(traceback.format_exc())

input('\nPress ENTER to exit...')
