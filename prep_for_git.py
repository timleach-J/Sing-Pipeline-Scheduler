"""
prep_for_git_20260820.py
────────────────────────
Run this before pushing to GitHub.

WHAT IT DOES
  Your working files carry dates and versions:   TAILS_20260820_v294.py
  The GitHub repo uses clean names:              TAILS.py

  This makes an undated copy of each script so git tracks a stable filename.
  Your dated originals are never touched, moved, or deleted.

  If several dated versions of the same script exist, the NEWEST wins.

WHAT IT DOES NOT DO
  - It does not run git. It prints the commands for you to paste.
  - It does not touch Obsolite\\, logs\\, or lib\\credentials\\.
  - It never deletes anything.

Double-click to run.
"""

import os
import re
import shutil
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))

# Folders to scan. Anything else (Obsolite, logs, credentials) is left alone.
SCAN_DIRS = [HERE, os.path.join(HERE, 'lib')]

SKIP_DIRS = {'obsolite', 'logs', 'credentials', '__pycache__', '.git'}

# Matches _YYYYMMDD and _vNNN anywhere in the stem:
#   TAILS_20260820_v294      -> TAILS
#   sing_climb_20260819      -> sing_climb
#   Label_generator_20260806 -> Label_generator
DATE_RE = re.compile(r'_(20\d{6})')
VER_RE  = re.compile(r'_v\d+', re.IGNORECASE)


def clean_name(stem):
    """Strip the _YYYYMMDD and _vNNN parts from a filename stem."""
    s = DATE_RE.sub('', stem)
    s = VER_RE.sub('', s)
    return s.strip('_')


def date_key(stem):
    """Sort key: the date in the name, then the version number. Newest wins."""
    d = DATE_RE.search(stem)
    v = re.search(r'_v(\d+)', stem, re.IGNORECASE)
    return (int(d.group(1)) if d else 0,
            int(v.group(1)) if v else 0)


def main():
    print('=' * 70)
    print('  Prepare files for GitHub')
    print(f'  {datetime.now():%Y-%m-%d %H:%M}')
    print('=' * 70)
    print(f'\n  Folder: {HERE}\n')

    # ── Find every dated script, grouped by its clean name ────────────────────
    groups = {}          # (dir, clean_filename) -> [full paths]
    for d in SCAN_DIRS:
        if not os.path.isdir(d):
            continue
        if os.path.basename(d).lower() in SKIP_DIRS:
            continue
        for fn in sorted(os.listdir(d)):
            if not fn.lower().endswith('.py'):
                continue
            stem, ext = os.path.splitext(fn)
            if not DATE_RE.search(stem):
                continue                      # already undated — leave it
            target = clean_name(stem) + ext
            groups.setdefault((d, target), []).append(os.path.join(d, fn))

    if not groups:
        print('  No dated scripts found. Nothing to do.')
        return

    # ── Work out what would be copied ─────────────────────────────────────────
    plan = []
    for (d, target), paths in sorted(groups.items()):
        newest = max(paths, key=lambda p: date_key(os.path.splitext(
            os.path.basename(p))[0]))
        dest = os.path.join(d, target)

        status = 'NEW'
        if os.path.exists(dest):
            try:
                with open(newest, 'rb') as a, open(dest, 'rb') as b:
                    status = 'unchanged' if a.read() == b.read() else 'UPDATE'
            except OSError:
                status = 'UPDATE'

        plan.append((newest, dest, status, len(paths)))

    rel = lambda p: os.path.relpath(p, HERE)

    print(f'  {"SOURCE (newest dated file)":<44} {"->":<3} {"REPO NAME":<32} STATUS')
    print('  ' + '-' * 96)
    for src, dest, status, n in plan:
        extra = f'  ({n} versions)' if n > 1 else ''
        print(f'  {rel(src)[:42]:<44} {"->":<3} {rel(dest)[:30]:<32} {status}{extra}')

    todo = [p for p in plan if p[2] != 'unchanged']
    if not todo:
        print('\n  Everything is already up to date. Nothing to copy.')
    else:
        print(f'\n  {len(todo)} file(s) would be created or updated.')
        answer = input('\n  Make these copies? (y/n): ').strip().lower()
        if answer not in ('y', 'yes'):
            print('  Cancelled. Nothing was changed.')
            return
        print()
        for src, dest, status, _ in todo:
            try:
                shutil.copy2(src, dest)
                print(f'  {status:<9}  {rel(dest)}')
            except Exception as e:
                print(f'  FAILED    {rel(dest)}: {e}')

    # ── Next steps ────────────────────────────────────────────────────────────
    print('\n' + '=' * 70)
    print('  NEXT — copy and paste these, one line at a time')
    print('=' * 70)
    print("""
  1. Check nothing secret is about to be committed.
     BOTH of these must print a line of text. If either prints
     nothing, STOP and say so.

       git check-ignore -v lib\\credentials\\sing_credentials.json
       git check-ignore -v animals.csv

  2. See what will be included:

       git add .
       git status

  3. If that looks right, commit and push:

       git commit -m "v2.9.4"
       git push
""")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'\n  Error: {e}')
    input('\nPress Enter to close...')
