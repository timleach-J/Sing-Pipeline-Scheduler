"""
TAILS_Label_Diagnostic_20260819.py
──────────────────────────────────
Works out WHY the Labels step produced no file.

Reproduces exactly what run_labels() does, step by step, and stops at the
first thing that would make it bail out — naming the check that failed.

READ-ONLY. Reads samples.csv and animals.csv from this folder. Writes nothing,
changes nothing, creates no sample numbers.

Drop next to sing_pipeline_v2_*.py and double-click.
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

def line(c="-", n=72):
    print(c * n)

print("=" * 72)
print("  TAILS — Label Step Diagnostic")
print("=" * 72)

try:
    import pandas as pd
except ImportError:
    print("\n[X] pandas not installed.  Run:  pip install pandas")
    input("\nPress Enter to close...")
    sys.exit(1)


# ── Step 1: files present ─────────────────────────────────────────────────────
print("\nSTEP 1 — Input files")
line()

s_path = os.path.join(HERE, 'samples.csv')
a_path = os.path.join(HERE, 'animals.csv')

for label, p in (('samples.csv', s_path), ('animals.csv', a_path)):
    if os.path.exists(p):
        import datetime
        mt = datetime.datetime.fromtimestamp(os.path.getmtime(p))
        print(f"  [OK] {label:14} last modified {mt:%Y-%m-%d %H:%M}")
    else:
        print(f"  [X]  {label:14} NOT FOUND in {HERE}")

if not os.path.exists(s_path):
    print("\n  >>> WOULD FAIL: 'No sample data — the Climb Samples module did not run.'")
    print("      Export samples from Climb and save as samples.csv here.")
    input("\nPress Enter to close...")
    sys.exit(0)
if not os.path.exists(a_path):
    print("\n  >>> animals.csv missing — load it via the pipeline's file picker,")
    print("      or place it here for this diagnostic.")
    input("\nPress Enter to close...")
    sys.exit(0)

s = pd.read_csv(s_path, dtype=str)
a = pd.read_csv(a_path, dtype=str)
print(f"\n  samples.csv : {len(s):>5} rows")
print(f"  animals.csv : {len(a):>5} rows")


# ── Step 2: required columns ──────────────────────────────────────────────────
print("\nSTEP 2 — Required columns")
line()

need_s = ['Name', 'Source', 'Preservation', 'Harvest Date']
missing_s = [c for c in need_s if c not in s.columns]
print(f"  samples.csv needs {need_s}")
if missing_s:
    print(f"  [X] MISSING: {missing_s}")
    print("      >>> WOULD FAIL: \"'Animal Name' not found in samples after rename\"")
    print("      Re-export from Climb with ALL columns selected.")
    input("\nPress Enter to close...")
    sys.exit(0)
print("  [OK] all present")

if 'Animal_Name' in a.columns:
    a_key = 'Animal_Name'
elif 'Name' in a.columns:
    a_key = 'Name'
else:
    print("  [X] animals.csv has neither 'Animal_Name' nor 'Name'")
    print("      >>> WOULD FAIL: \"'Animal Name' not found in animal data after rename\"")
    input("\nPress Enter to close...")
    sys.exit(0)
print(f"  [OK] animals.csv animal-id column is '{a_key}'")


# ── Step 3: the merge ─────────────────────────────────────────────────────────
print("\nSTEP 3 — Merge (this is where most runs die)")
line()

s_ids = set(s['Source'].fillna('').astype(str).str.strip()) - {''}
a_ids = set(a[a_key].fillna('').astype(str).str.strip()) - {''}
overlap = s_ids & a_ids

print(f"  distinct animals in samples.csv : {len(s_ids)}")
print(f"  distinct animals in animals.csv : {len(a_ids)}")
print(f"  OVERLAP                         : {len(overlap)}")

if not overlap:
    print("\n  >>> WOULD FAIL: 'No matching animal names between samples and animals'")
    print("      The two exports cover different animals.")
    only_a = sorted(a_ids)[:10]
    only_s = sorted(s_ids)[:10]
    print(f"\n      animals.csv has : {only_a}")
    print(f"      samples.csv has : {only_s}")
    print("\n      Re-export whichever file is older, filtered to the same animals.")
    input("\nPress Enter to close...")
    sys.exit(0)

print(f"  [OK] {len(overlap)} animal(s) matched: {sorted(overlap)[:10]}")


# ── Step 4: label routing ─────────────────────────────────────────────────────
print("\nSTEP 4 — Label routing per sample")
line()

def determine_label_type(preservation):
    p = str(preservation).strip().lower()
    if 'oct' in p and 'block' in p:              return 'skip', 0
    elif 'frozen' in p:                          return 'rna', 1
    elif 'pfa' in p or 'fixed' in p:             return 'perfusion', 2
    else:                                        return 'unknown', 0

matched = s[s['Source'].fillna('').astype(str).str.strip().isin(overlap)]
print(f"  {len(matched)} sample row(s) belong to matched animals\n")

counts = {}
unknown_rows = []
for _, r in matched.iterrows():
    lt, c = determine_label_type(r.get('Preservation'))
    counts[lt] = counts.get(lt, 0) + 1
    if lt == 'unknown':
        unknown_rows.append((r.get('Name'), r.get('Source'), r.get('Preservation')))

for k in ('perfusion', 'rna', 'skip', 'unknown'):
    if counts.get(k):
        print(f"    {k:10} {counts[k]:>4}")

if unknown_rows:
    print("\n  [!] Unrecognised Preservation — these get NO label:")
    for n, src, p in unknown_rows:
        print(f"      Sample {n}  Animal {src}  Preservation={p!r}")


# ── Verdict ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 72)
print("  VERDICT")
print("=" * 72)

perf = counts.get('perfusion', 0)
rna  = counts.get('rna', 0)

if perf == 0 and rna == 0:
    if counts.get('skip'):
        print("  >>> 'All samples are OCT Block — no labels needed.'")
        print("      OCT samples never get labels. Nothing is wrong.")
    else:
        print("  >>> 'No labels generated.'")
        print("      Every sample had an unrecognised Preservation (see above).")
else:
    print("  Labels SHOULD be produced:")
    if perf:
        print(f"    Labels_Mailmerge_sheet*.xlsx   {perf} samples -> {perf*2} labels")
    if rna:
        print(f"    Tube_Labeler_RNA_*.xlsx        {rna} tube labels")
    print(f"\n  They are written to:\n    {HERE}")
    print("\n  If no file is there, the run did not reach this point —")
    print("  send the console text printed after 'STEP 4: LABELS'.")

input("\nPress Enter to close...")
