"""
sing_google.py
Google Sheets integration for the SING pipeline.

STATUS: Pending GCP provisioning (JAX ticket RITM0348414).
        Auth will work once google_credentials.json is in the scripts folder.

TWO WRITE PATHS — both guarded:

  1. Harvest Sheet sync (run_harvest_sync)
     snapshot → write raw harvest records → verify Summary Sheet delta

  2. Deliverables upload (upload_confirmed_deliverables)
     ONLY runs after a harvest is physically confirmed complete.
     Uploads to the shared Deliverables Google Sheet so collaborators
     (Penn State, NYU) only ever see samples that actually exist.
     Never called from the scheduler — only from a confirmed-harvest trigger.

If any step fails the caller gets a SyncError or VerificationError,
never a silent bad write.
"""

import os
import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# ── Sheet identifiers ─────────────────────────────────────────────────────────
HARVEST_SHEET_ID    = '1eAYEEgB65wmOpmqtzpbVjNyidhJuxFhefC0aRN89jH8'
WORKSHEET_TAB       = 'Harvest Worksheet'   # tab where raw harvest records live
SUMMARY_TAB         = 'Summary Sheet'       # tab with COUNTIF totals (cols 3-14)

# TODO: set this to the Deliverables Google Sheet ID once shared
DELIVERABLES_SHEET_ID  = 'REPLACE_WITH_DELIVERABLES_SHEET_ID'
DELIVERABLES_TAB       = 'SING Deliverables'   # TODO: confirm tab name with collaborators

# Column headers written to the Deliverables sheet (order matters)
DELIVERABLES_COLUMNS = [
    'Harvest Date', 'Animal Name', 'Strain', 'Sex', 'Genotype',
    'Timepoint', 'Harvest Type', 'Sample Number', 'Preservation',
    'Source', 'Housing ID', 'Birth Date', 'Age (Days)',
]

# Column indices in Summary Sheet (integer, 0-based, matching parse_requirements)
SUMMARY_COUNT_COLS = {
    'P14': {'Male':   {'Perfusion': 3, 'MERFISH': 7, 'RNAseq': 11},
            'Female': {'Perfusion': 4, 'MERFISH': 8, 'RNAseq': 12}},
    'P56': {'Male':   {'Perfusion': 5, 'MERFISH': 9, 'RNAseq': 13},
            'Female': {'Perfusion': 6, 'MERFISH': 10, 'RNAseq': 14}},
}

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]


# ── Exceptions ────────────────────────────────────────────────────────────────
class SyncError(RuntimeError):
    """Raised when a sheet write or verification step fails."""


class VerificationError(SyncError):
    """Raised when post-write delta does not match what was written."""


# ── Auth ─────────────────────────────────────────────────────────────────────
def _get_service():
    """
    Authenticate via Application Default Credentials and return a Google Sheets API client.
    Credentials are set once by running: gcloud auth application-default login
    Raises SyncError if auth fails or libraries are missing.
    """
    try:
        import google.auth
        from googleapiclient.discovery import build
    except ImportError as e:
        raise SyncError(
            "Google API libraries not installed. "
            "Run: pip install google-auth google-auth-httplib2 google-api-python-client"
        ) from e

    try:
        creds, project = google.auth.default(scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
        return service
    except Exception as e:
        raise SyncError(
            f"Google Sheets authentication failed: {e}\n"
            "Run: gcloud auth application-default login"
        ) from e


def is_available() -> bool:
    """Return True if credentials exist and the API is reachable."""
    try:
        _get_service()
        return True
    except SyncError:
        return False


# ── Snapshot ─────────────────────────────────────────────────────────────────
def snapshot_summary_counts(service=None) -> Dict[str, Dict]:
    """
    Read the current harvest counts from the Summary Sheet.
    Returns a nested dict:
        { 'STRAIN_UPPER': { 'P14': { 'Male': { 'Perfusion': N, ... }, ... }, ... } }

    This is called BEFORE and AFTER a write so the delta can be verified.
    Raises SyncError if the sheet cannot be read.
    """
    if service is None:
        service = _get_service()

    try:
        result = (
            service.spreadsheets().values()
            .get(spreadsheetId=HARVEST_SHEET_ID,
                 range=f'{SUMMARY_TAB}!A1:S300')
            .execute()
        )
    except Exception as e:
        raise SyncError(f"Could not read Summary Sheet: {e}") from e

    rows = result.get('values', [])
    if not rows:
        raise SyncError("Summary Sheet returned no data.")

    snapshot: Dict[str, Dict] = {}
    seen_first = False

    for row in rows:
        if not row:
            continue
        strain_raw = str(row[0]).strip()

        if strain_raw in ('Lines', 'Line'):
            if seen_first:
                break        # second header block — stop
            continue

        if not strain_raw or strain_raw.lower() in ('nan', ''):
            continue

        seen_first  = True
        strain_key  = strain_raw.upper()
        counts: Dict = {}

        for timepoint, sexes in SUMMARY_COUNT_COLS.items():
            counts[timepoint] = {}
            for sex, htypes in sexes.items():
                counts[timepoint][sex] = {}
                for htype, col_idx in htypes.items():
                    try:
                        val = int(float(row[col_idx])) if col_idx < len(row) and row[col_idx] else 0
                    except (ValueError, TypeError):
                        val = 0
                    counts[timepoint][sex][htype] = val

        snapshot[strain_key] = counts

    logger.info("snapshot_summary_counts: read %d strains", len(snapshot))
    return snapshot


# ── Delta computation ─────────────────────────────────────────────────────────
def compute_expected_delta(working_df: pd.DataFrame) -> Dict[str, Dict]:
    """
    Look at the animals in working_df that are being harvested THIS run and
    compute how many new records per (strain, timepoint, sex, harvest_type)
    should appear in the Summary Sheet after the write.

    Returns the same nested dict shape as snapshot_summary_counts().
    Animals with harvest type 'Extra', 'Do Not Schedule', or 'COMPLETE'
    are excluded — they don't add new records.
    """
    SKIP_TYPES = {'Extra', 'Do Not Schedule', 'COMPLETE (Quota Filled)', ''}

    delta: Dict[str, Dict] = {}

    if 'Harvest_Type' not in working_df.columns:
        return delta

    for _, row in working_df.iterrows():
        ht_raw    = str(row.get('Harvest_Type', '')).strip()
        timepoint = str(row.get('Assigned_Timepoint', '')).strip()
        sex       = str(row.get('Sex', '')).strip()
        strain    = str(row.get('Strain', row.get('Line (Short)', ''))).strip()

        if ht_raw in SKIP_TYPES or not strain or timepoint not in ('P14', 'P56'):
            continue

        # Strip NB suffix for counting purposes — NB animals still get harvested
        ht = ht_raw.replace(' NB', '').strip()
        if ht not in ('Perfusion', 'MERFISH', 'RNAseq'):
            continue

        sk = strain.upper()
        delta.setdefault(sk, {}).setdefault(timepoint, {}).setdefault(sex, {})
        delta[sk][timepoint][sex][ht] = delta[sk][timepoint][sex].get(ht, 0) + 1

    return delta


# ── Write (placeholder — fill in once GCP is provisioned) ────────────────────
def write_harvest_records(working_df: pd.DataFrame, service=None) -> None:
    """
    Append new harvest records from working_df to the Worksheet tab.

    TODO (post-GCP): implement the actual append logic here.
         - Build rows from working_df matching the Worksheet column order
         - Use spreadsheets().values().append() with valueInputOption='USER_ENTERED'
         - Wrap in a try/except and raise SyncError on any failure

    For now this is a no-op placeholder so the safeguard structure is in place.
    """
    # ── PLACEHOLDER ──────────────────────────────────────────────────────────
    logger.info("write_harvest_records: placeholder — no data written (GCP pending)")
    # ─────────────────────────────────────────────────────────────────────────


# ── Verification ─────────────────────────────────────────────────────────────
def verify_summary_delta(
    before:   Dict[str, Dict],
    after:    Dict[str, Dict],
    expected: Dict[str, Dict],
) -> Tuple[bool, List[str]]:
    """
    Compare Summary Sheet snapshots taken before and after a write.
    Checks that (after - before) == expected delta for every cell.

    Returns (all_ok: bool, issues: List[str]).
    Caller should raise VerificationError if all_ok is False.
    """
    issues: List[str] = []

    all_strains = set(expected.keys()) | set(before.keys()) | set(after.keys())

    for sk in sorted(all_strains):
        if sk not in expected:
            continue     # strain wasn't written — no expectation to check

        exp_strain  = expected.get(sk, {})
        bef_strain  = before.get(sk, {})
        aft_strain  = after.get(sk, {})

        for tp, sexes in exp_strain.items():
            for sex, htypes in sexes.items():
                for ht, exp_delta in htypes.items():
                    if exp_delta == 0:
                        continue

                    bef_val = bef_strain.get(tp, {}).get(sex, {}).get(ht, 0)
                    aft_val = aft_strain.get(tp, {}).get(sex, {}).get(ht, 0)
                    actual_delta = aft_val - bef_val

                    if actual_delta != exp_delta:
                        issues.append(
                            f"{sk} {tp} {sex} {ht}: "
                            f"expected +{exp_delta}, got +{actual_delta} "
                            f"(before={bef_val}, after={aft_val})"
                        )

    return (len(issues) == 0), issues


# ── Orchestrator — the full safeguarded sync ──────────────────────────────────
def run_harvest_sync(working_df: pd.DataFrame, dry_run: bool = False) -> bool:
    """
    Full safeguarded harvest sync:
        1. Check API is available
        2. Snapshot counts BEFORE writing
        3. Compute expected delta from working_df
        4. Write records (or skip if dry_run=True)
        5. Snapshot counts AFTER writing
        6. Verify delta matches expectation
        7. Raise VerificationError if anything is wrong

    Returns True on success.
    Set dry_run=True to run all steps except the actual write — useful for
    testing the snapshot/verify logic before going live.
    """
    logger.info("run_harvest_sync: starting (dry_run=%s)", dry_run)

    # ── 1. Auth check ─────────────────────────────────────────────────────────
    try:
        service = _get_service()
        logger.info("run_harvest_sync: authenticated OK")
    except SyncError as e:
        raise SyncError(f"Cannot sync — Google Sheets unavailable: {e}") from e

    # ── 2. Pre-write snapshot ──────────────────────────────────────────────────
    print("  [Sheets] Reading pre-write snapshot…")
    before = snapshot_summary_counts(service)
    print(f"  [Sheets] Snapshot: {len(before)} strains read")

    # ── 3. Expected delta ──────────────────────────────────────────────────────
    expected = compute_expected_delta(working_df)
    total_expected = sum(
        v for tp in tps.values()
        for sex in tp.values()
        for v in sex.values()
        for tps in [tp]  # just iterating
    )
    # cleaner count
    total_expected = sum(
        v
        for strain_data in expected.values()
        for tp_data in strain_data.values()
        for sex_data in tp_data.values()
        for v in sex_data.values()
    )
    print(f"  [Sheets] Expected delta: {total_expected} new records across "
          f"{len(expected)} strain(s)")

    # ── 4. Write ──────────────────────────────────────────────────────────────
    if dry_run:
        print("  [Sheets] dry_run=True — skipping write")
    else:
        print("  [Sheets] Writing harvest records to Worksheet…")
        write_harvest_records(working_df, service)
        print("  [Sheets] Write complete")

    # ── 5. Post-write snapshot ─────────────────────────────────────────────────
    print("  [Sheets] Reading post-write snapshot…")
    after = snapshot_summary_counts(service)

    # ── 6. Verify ─────────────────────────────────────────────────────────────
    if dry_run:
        print("  [Sheets] dry_run=True — skipping verification (no write occurred)")
        return True

    ok, issues = verify_summary_delta(before, after, expected)

    if ok:
        print(f"  ✓ [Sheets] Verification passed — all {total_expected} record(s) "
              f"confirmed in Summary Sheet")
        logger.info("run_harvest_sync: verification passed (%d records)", total_expected)
        return True
    else:
        msg = (
            f"Summary Sheet delta verification FAILED — "
            f"{len(issues)} discrepancy(s) detected:\n"
            + "\n".join(f"  • {i}" for i in issues)
        )
        print(f"\n  ✗ [Sheets] {msg}\n")
        logger.error("run_harvest_sync: %s", msg)
        raise VerificationError(msg)


# ── Deliverables upload (confirmed harvest only) ──────────────────────────────
#
# DESIGN INTENT:
#   This function is NEVER called during scheduling or forecasting.
#   It is called ONLY after a harvest is physically confirmed complete —
#   i.e. the user clicks "Confirm Harvest & Upload" in the pipeline GUI,
#   or runs this module directly with a confirmed harvest CSV.
#
#   Collaborators (Penn State, NYU) see the Deliverables sheet as the
#   authoritative list of samples coming to them. Uploading forecasts
#   caused confusion when harvests were missed. This enforces the rule:
#   nothing goes on that sheet until it physically exists.

def _check_deliverables_sheet_configured():
    """Raise SyncError if the Deliverables sheet ID hasn't been set yet."""
    if DELIVERABLES_SHEET_ID == 'REPLACE_WITH_DELIVERABLES_SHEET_ID':
        raise SyncError(
            "DELIVERABLES_SHEET_ID is not configured in sing_google.py. "
            "Add the Deliverables Google Sheet ID and confirm the tab name "
            f"(currently '{DELIVERABLES_TAB}') with your collaborators."
        )


def snapshot_deliverables_row_count(service=None) -> int:
    """
    Return the number of data rows currently in the Deliverables sheet.
    Used to verify that new rows were actually appended after a write.
    """
    _check_deliverables_sheet_configured()
    if service is None:
        service = _get_service()

    try:
        result = (
            service.spreadsheets().values()
            .get(spreadsheetId=DELIVERABLES_SHEET_ID,
                 range=f'{DELIVERABLES_TAB}!A:A')
            .execute()
        )
    except Exception as e:
        raise SyncError(f"Could not read Deliverables sheet row count: {e}") from e

    rows = result.get('values', [])
    # Subtract 1 for the header row (if any)
    return max(0, len(rows) - 1)


def _build_deliverables_rows(confirmed_df: pd.DataFrame) -> List[List]:
    """
    Convert the confirmed-harvest DataFrame into rows for the Deliverables sheet.
    Only includes animals with a real harvest type (not Extra, DNS, or COMPLETE).
    """
    SKIP_TYPES = {'Extra', 'Do Not Schedule', 'COMPLETE (Quota Filled)', ''}
    rows = []

    for _, row in confirmed_df.iterrows():
        ht = str(row.get('Harvest_Type', '')).strip()
        if ht in SKIP_TYPES:
            continue

        harvest_date = row.get('Harvest_Date', row.get('Harvest Date', ''))
        try:
            harvest_date = pd.to_datetime(str(harvest_date)).strftime('%Y-%m-%d')
        except Exception:
            harvest_date = str(harvest_date)

        birth_date = row.get('Birth_Date', row.get('Birth Date', ''))
        try:
            birth_date = pd.to_datetime(str(birth_date)).strftime('%Y-%m-%d')
        except Exception:
            birth_date = str(birth_date)

        rows.append([
            harvest_date,
            str(row.get('Animal_Name', row.get('Name', ''))).strip(),
            str(row.get('Strain', row.get('Line (Short)', ''))).strip(),
            str(row.get('Sex', '')).strip(),
            str(row.get('Genotype', '')).strip(),
            str(row.get('Assigned_Timepoint', '')).strip(),
            ht,
            str(row.get('Sample_Number', row.get('Sample Number', ''))).strip(),
            str(row.get('Preservation', '')).strip(),
            str(row.get('Source', '')).strip(),
            str(row.get('Housing ID', row.get('Housing_ID', ''))).strip(),
            birth_date,
            str(row.get('Age_Days', row.get('Age (Days)', ''))).strip(),
        ])

    return rows


def write_confirmed_deliverables(confirmed_df: pd.DataFrame, service=None) -> int:
    """
    Append confirmed-harvest rows to the Deliverables Google Sheet.
    Returns the number of rows written.

    IMPORTANT: Only call this after a harvest is physically confirmed complete.
               Never call from the scheduler or forecast path.

    Raises SyncError on any write failure.
    """
    _check_deliverables_sheet_configured()
    if service is None:
        service = _get_service()

    rows = _build_deliverables_rows(confirmed_df)
    if not rows:
        logger.info("write_confirmed_deliverables: no eligible rows to write")
        return 0

    body = {'values': rows}
    try:
        service.spreadsheets().values().append(
            spreadsheetId=DELIVERABLES_SHEET_ID,
            range=f'{DELIVERABLES_TAB}!A1',
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body=body,
        ).execute()
    except Exception as e:
        raise SyncError(f"Failed to write to Deliverables sheet: {e}") from e

    logger.info("write_confirmed_deliverables: wrote %d rows", len(rows))
    return len(rows)


def upload_confirmed_deliverables(confirmed_df: pd.DataFrame,
                                   dry_run: bool = False) -> bool:
    """
    Safeguarded upload to the Deliverables Google Sheet.
    Only call this AFTER the harvest is physically confirmed complete.

    Steps:
        1. Validate sheet is configured
        2. Auth check
        3. Count rows BEFORE upload
        4. Build and write rows (skipped if dry_run=True)
        5. Count rows AFTER upload
        6. Verify row count increased by exactly the number written
        7. Raise VerificationError if mismatch

    Returns True on success.
    """
    _check_deliverables_sheet_configured()

    logger.info("upload_confirmed_deliverables: starting (dry_run=%s)", dry_run)

    # ── 1. Auth ───────────────────────────────────────────────────────────────
    try:
        service = _get_service()
    except SyncError as e:
        raise SyncError(f"Cannot upload deliverables — API unavailable: {e}") from e

    # ── 2. Pre-upload row count ───────────────────────────────────────────────
    print("  [Deliverables] Reading pre-upload row count…")
    rows_before = snapshot_deliverables_row_count(service)
    print(f"  [Deliverables] Current rows: {rows_before}")

    # ── 3. Build rows & report ────────────────────────────────────────────────
    rows_to_write = _build_deliverables_rows(confirmed_df)
    n = len(rows_to_write)

    if n == 0:
        print("  [Deliverables] No eligible rows to upload (all Extra/DNS/COMPLETE).")
        return True

    strains_going = sorted({r[2] for r in rows_to_write})
    print(f"  [Deliverables] {n} confirmed harvest record(s) ready to upload:")
    for strain in strains_going:
        count = sum(1 for r in rows_to_write if r[2] == strain)
        print(f"    {strain}: {count} sample(s)")

    # ── 4. Write ──────────────────────────────────────────────────────────────
    if dry_run:
        print("  [Deliverables] dry_run=True — skipping write")
    else:
        print("  [Deliverables] Uploading…")
        written = write_confirmed_deliverables(confirmed_df, service)
        print(f"  [Deliverables] {written} row(s) written")

    # ── 5. Post-upload row count ──────────────────────────────────────────────
    if dry_run:
        print("  [Deliverables] dry_run=True — skipping verification")
        return True

    print("  [Deliverables] Verifying row count…")
    rows_after = snapshot_deliverables_row_count(service)

    # ── 6. Verify ─────────────────────────────────────────────────────────────
    actual_added = rows_after - rows_before
    if actual_added != n:
        msg = (
            f"Deliverables upload verification FAILED — "
            f"expected {n} new row(s), sheet shows {actual_added} new row(s) "
            f"(before={rows_before}, after={rows_after})."
        )
        print(f"\n  ✗ [Deliverables] {msg}\n")
        logger.error("upload_confirmed_deliverables: %s", msg)
        raise VerificationError(msg)

    print(f"  ✓ [Deliverables] Verified — {n} row(s) confirmed in sheet "
          f"({rows_before} → {rows_after})")
    logger.info("upload_confirmed_deliverables: verified %d rows added", n)
    return True


# ── Standalone test ───────────────────────────────────────────────────────────
if __name__ == '__main__':
    print("=" * 60)
    print("  sing_google.py — connection + snapshot test")
    print("=" * 60)

    if not is_available():
        print("\n✗  Google Sheets API not available.")
        print("   Check google_credentials.json is present and GCP APIs are enabled.")
        input("\nPress Enter to close...")
        raise SystemExit(1)

    print("\n✓  Authenticated")

    try:
        print("\nReading Summary Sheet snapshot…")
        snap = snapshot_summary_counts()
        print(f"✓  {len(snap)} strains read\n")

        # Show a few strains that have data
        shown = 0
        for sk, counts in sorted(snap.items()):
            total = sum(
                v
                for tp in counts.values()
                for sex in tp.values()
                for v in sex.values()
            )
            if total > 0:
                print(f"  {sk:<22} total harvested = {total}")
                shown += 1
            if shown >= 10:
                print("  … (showing first 10 with data)")
                break

        print("\n✓  snapshot_summary_counts working correctly")
        print("✓  Ready for write + verify once GCP provisioning is complete")

    except SyncError as e:
        print(f"\n✗  {e}")

    input("\nPress Enter to close...")
