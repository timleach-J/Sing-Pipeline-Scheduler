"""
test_google_connection.py
Run to confirm Application Default Credentials can reach both SING sheets.
Requires: gcloud auth application-default login  (one-time setup, already done)
"""
import sys

SHEETS = {
    'Sing Harvest Sheet':         '1eAYEEgB65wmOpmqtzpbVjNyidhJuxFhefC0aRN89jH8',
    'Animal and Sample Tracking': '1VTSB91dpedVsSMw_RcNq-u_odZ1NikYHZBBjXE0--g0',
}

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/drive.readonly',
]

print("=" * 60)
print("  SING Google Sheets Connection Test")
print("=" * 60)

# ── 1. Import libraries ───────────────────────────────────────────────────────
try:
    import google.auth
    from googleapiclient.discovery import build
    print("\n✓  google-auth libraries imported")
except ImportError as e:
    print(f"\n✗  Missing library: {e}")
    print("\n  Run:  pip install google-auth google-auth-httplib2 google-api-python-client")
    input("\nPress Enter to close...")
    sys.exit(1)

# ── 2. Authenticate ───────────────────────────────────────────────────────────
try:
    creds, project = google.auth.default(scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    print("✓  Authenticated via Application Default Credentials")
except Exception as e:
    print(f"\n✗  Authentication failed: {e}")
    print("\n  Make sure you have run:  gcloud auth application-default login")
    input("\nPress Enter to close...")
    sys.exit(1)

# ── 3. Test each sheet ────────────────────────────────────────────────────────
print()
all_ok = True
for name, sheet_id in SHEETS.items():
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range='A1:C3')
            .execute()
        )
        rows = result.get('values', [])
        print(f"✓  {name}")
        print(f"     First cell: {rows[0][0] if rows and rows[0] else '(empty)'}")
    except Exception as e:
        print(f"✗  {name}")
        print(f"     Error: {e}")
        print(f"     → Make sure you have Editor access to this sheet in Google Drive.")
        all_ok = False

# ── 4. Result ─────────────────────────────────────────────────────────────────
print()
print("=" * 60)
if all_ok:
    print("  ✓  All checks passed — Google Sheets API is working!")
    print("  You're ready to enable the Sheets integration in the pipeline.")
else:
    print("  ✗  One or more checks failed — see messages above.")
print("=" * 60)

input("\nPress Enter to close...")
