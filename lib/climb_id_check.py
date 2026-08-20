"""
climb_id_check.py  (v3)
Finds the first Alive + Sing Inventory animal and dumps every raw field.

Credentials are read from lib/credentials/sing_credentials.json — the canonical
location. The old lib/ path is still accepted so a half-finished move doesn't
break this, but it warns so the stray copy gets cleaned up.

Run it from the lib folder.
"""
import json, os, time, requests

# ── Credentials ───────────────────────────────────────────────────────────────
_here      = os.path.dirname(os.path.abspath(__file__))
_preferred = os.path.join(_here, 'credentials', 'sing_credentials.json')
_legacy    = os.path.join(_here, 'sing_credentials.json')

if os.path.exists(_preferred):
    cred_path = _preferred
    if os.path.exists(_legacy):
        print(f'  [!] A second copy of sing_credentials.json is still at {_legacy}')
        print( '      Using lib/credentials/. Delete the stray copy.')
elif os.path.exists(_legacy):
    cred_path = _legacy
    print(f'  [!] sing_credentials.json found at the old location: {_legacy}')
    print( '      Move it to lib/credentials/ — that folder is git-ignored.')
else:
    raise FileNotFoundError(
        f'Climb credentials file not found. Expected at:\n  {_preferred}'
    )

with open(cred_path, 'r', encoding='utf-8') as _f:
    creds = json.load(_f)

# ── Auth ──────────────────────────────────────────────────────────────────────
r = requests.post(
    'https://climb-admin.azurewebsites.net/token',
    data={
        'grant_type':    'client_credentials',
        'client_id':     creds['client_id'],
        'client_secret': creds['client_secret'],
    }, timeout=15
)
r.raise_for_status()
token = r.json()['access_token']
print('✓ Authenticated')

headers = {
    'Authorization':   f'Bearer {token}',
    'X-Workgroup-Key': creds['workgroup_key'],
}

# Page through until we find an Alive + Sing Inventory animal
target = None
page   = 1
while target is None:
    r = requests.get(
        'https://api.climb.bio/api/animals',
        headers=headers,
        params={'pageSize': 200, 'pageNumber': page},
        timeout=30
    )
    r.raise_for_status()
    data  = r.json().get('data', {})
    items = data.get('items', [])
    if not items:
        print('No more pages — SING animal not found.')
        break
    target = next(
        (a for a in items
         if a.get('status') == 'Alive' and a.get('use') == 'Sing Inventory'),
        None
    )
    total = data.get('totalItemCount', '?')
    print(f'  Page {page} ({len(items)} animals, total={total}) — '
          f'{"found SING animal" if target else "no SING animal yet"}')
    page += 1
    time.sleep(0.1)

if target:
    print(f"\nAnimal name : {target.get('animalName')}")
    print(f"{'Field':<45} Value")
    print('-' * 70)
    for k, v in sorted(target.items()):
        print(f"  {k!r:<43} {v!r}")

input('\nPress Enter to close...')
