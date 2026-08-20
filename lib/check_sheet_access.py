"""
check_sheet_access_20260819.py
Reports which SING Google Sheets the service account can reach,
and lists the tab names inside each one.

Read-only. Writes nothing.
"""
import sys

SHEETS = {
    'Sing Harvest Sheet':         '1eAYEEgB65wmOpmqtzpbVjNyidhJuxFhefC0aRN89jH8',
    'Animal and Sample Tracking': '1VTSB91dpedVsSMw_RcNq-u_odZ1NikYHZBBjXE0--g0',
    'PSU MERFISH Tracker':        '1YzzMxKIN16ujyO4j8-PXnsZEUv4dscjISSacRyBzRKY',
}

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

print("=" * 68)
print("  SING Sheet Access Check")
print("=" * 68)

try:
    import google.auth
    from googleapiclient.discovery import build
except ImportError as e:
    print(f"\n✗  Missing library: {e}")
    print("   Run: pip install google-auth google-auth-httplib2 google-api-python-client")
    input("\nPress Enter to close...")
    sys.exit(1)

try:
    creds, project = google.auth.default(scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    print(f"\n✓  Authenticated  (GCP project: {project})")
except Exception as e:
    print(f"\n✗  Authentication failed: {e}")
    print("   Run: gcloud auth application-default login "
          "--impersonate-service-account=svc-jax-kumar-sing-api-01-prod@"
          "jax-kumar-sing-api-01-prod.iam.gserviceaccount.com")
    input("\nPress Enter to close...")
    sys.exit(1)

# Report which identity the API calls are actually made as
try:
    sa_email = getattr(creds, 'service_account_email', None)
    if sa_email:
        print(f"   Acting as:    {sa_email}")
except Exception:
    pass

print()
ok_count = 0
for name, sheet_id in SHEETS.items():
    try:
        meta = service.spreadsheets().get(
            spreadsheetId=sheet_id,
            fields='properties.title,sheets.properties.title'
        ).execute()

        title = meta.get('properties', {}).get('title', '(untitled)')
        tabs = [s['properties']['title'] for s in meta.get('sheets', [])]

        print(f"✓  {name}")
        print(f"     Actual title: {title}")
        print(f"     Tabs ({len(tabs)}):")
        for t in tabs:
            print(f"       - {t}")
        print()
        ok_count += 1

    except Exception as e:
        msg = str(e)
        if '403' in msg:
            reason = "NOT SHARED with the service account"
        elif '404' in msg:
            reason = "sheet ID not found (check the ID)"
        else:
            reason = msg[:120]
        print(f"✗  {name}")
        print(f"     {reason}")
        print()

print("=" * 68)
print(f"  {ok_count} of {len(SHEETS)} sheets accessible")
print("=" * 68)

input("\nPress Enter to close...")
