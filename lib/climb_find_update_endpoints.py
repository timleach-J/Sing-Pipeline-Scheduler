"""
Fetches the Climb swagger spec and lists PUT / PATCH endpoints,
then prints the request body schema for anything animal-related.

Self-contained — regenerates the spec, no saved file needed.
Also re-saves climb_swagger.json for future use.
"""

import json, os, sys, glob, traceback, requests, time

script_dir = os.path.dirname(os.path.abspath(__file__))

try:
    sys.path.insert(0, script_dir)
    cands = sorted(glob.glob(os.path.join(script_dir, 'sing_climb*.py')), reverse=True)
    cands += sorted(glob.glob(os.path.join(script_dir, 'lib', 'sing_climb*.py')), reverse=True)
    if not cands:
        raise FileNotFoundError('sing_climb*.py not found in this folder or lib\\')

    import importlib.util
    spec_l = importlib.util.spec_from_file_location('sc', cands[0])
    sc     = importlib.util.module_from_spec(spec_l)
    spec_l.loader.exec_module(sc)
    print(f'Loaded: {os.path.basename(cands[0])}')

    hdr = {
        'Authorization':   f'Bearer {sc._get_token()}',
        'X-Workgroup-Key': sc._WORKGROUP_KEY,
    }

    print('Fetching swagger spec...')
    r = requests.get('https://api.climb.bio/swagger/v1/swagger.json',
                     headers=hdr, timeout=30)
    r.raise_for_status()
    swagger = r.json()

    out_path = os.path.join(script_dir, 'climb_swagger.json')
    with open(out_path, 'w') as f:
        json.dump(swagger, f, indent=2)
    print(f'Saved: {os.path.basename(out_path)}\n')

    schemas = swagger.get('components', {}).get('schemas', {})
    def resolve(ref):
        return schemas.get(ref.split('/')[-1], {})

    print('=== PUT / PATCH endpoints ===')
    updates = []
    for path, methods in swagger.get('paths', {}).items():
        for verb in ('put', 'patch'):
            if verb in methods:
                updates.append((verb.upper(), path))
                print(f'  {verb.upper():6} {path}')
    if not updates:
        print('  (none)')

    print('\n=== Animal-related update bodies ===')
    found = False
    for verb, path in updates:
        if 'animal' not in path.lower():
            continue
        found = True
        print(f'\n{verb} {path}')
        op   = swagger['paths'][path][verb.lower()]
        body = op.get('requestBody', {}).get('content', {}).get('application/json', {})
        sch  = body.get('schema', {})
        if '$ref' in sch:
            sch = resolve(sch['$ref'])
        if sch.get('type') == 'array' and '$ref' in sch.get('items', {}):
            print('  (array of:)')
            sch = resolve(sch['items']['$ref'])
        props    = sch.get('properties', {})
        required = sch.get('required', [])
        if not props:
            print('  (no body schema)')
        for name, det in props.items():
            req = ' *REQUIRED*' if name in required else ''
            print(f'    {name}: {det.get("type","")}{req}')
        params = op.get('parameters', [])
        if params:
            print('  -- parameters --')
            for pm in params:
                print(f'    {pm.get("name")} ({pm.get("in")})'
                      f'{" *REQUIRED*" if pm.get("required") else ""}')
    if not found:
        print('  (no animal-related PUT/PATCH found)')

except Exception:
    print(traceback.format_exc())

input('\nPress ENTER to exit...')
