# SING Pipeline Scheduler

Schedules mouse harvests for the SING project and generates everything that
comes out of a harvest day — labels, tube labelers, Envision tag sheets, Climb
sample records, and the collaborator deliverables sheet.

Animals and births are pulled live from the Climb API. Nothing needs exporting
from Google Sheets any more.

---

## Folder layout

Everything lives in one shared folder on the Z: drive:

```
Z:\kumarlab-new\Tim Leach\Scripts\API Pipeline\
    sing_pipeline_v2_20260817.py        <- double-click this
    Sing Harvest Sheet.xlsx             <- inputs you drop in
    Animal and sample tracking.xlsx
    MERFISH-RNASeq_SampleTracker.xlsx
    [all exports land here]

    lib\
        sing_climb_20260811.py
        sing_common.py
        sing_credentials.json
```

Support files go in `lib\`. Everything the pipeline produces lands in the main
folder next to the script.

**The drive letter doesn't matter.** All paths are resolved relative to wherever
the script itself is, so if the share is mapped as `Y:` on your machine — or you
open it by UNC path with no letter at all — everything works. `Z:` is used
throughout this document only because that's how it's commonly mapped.

If `lib\` doesn't exist the pipeline falls back to looking in its own folder, so
a flattened copy still runs.

---

## Setting up a new machine

Everyone runs the pipeline from the **same shared folder** on the Z: drive.
Nobody copies it locally. That means the only thing you install on your machine
is Python and three packages — the script, credentials and inputs are already
there and shared.

Budget about 15 minutes.

---

### Step 1 — Map the Z: drive

Open **File Explorer** and check whether you can reach:

```
Z:\kumarlab-new\Tim Leach\Scripts\API Pipeline
```

If Z: isn't mapped:

1. In File Explorer, right-click **This PC** → **Map network drive**
2. Drive letter: **Z:**
3. Folder: the `kumarlab-new` share path (ask IT or a labmate for the exact
   server path)
4. Tick **Reconnect at sign-in**
5. Click **Finish**

You must be on the JAX network or VPN. The pipeline also needs network access to
reach the Climb API.

---

### Step 2 — Install Python

Download Python **3.11 or newer** from **python.org/downloads**.

> Use the installer from python.org, **not** the Microsoft Store version. The
> Store build sandboxes file access and has trouble with mapped network drives.

In the installer:

1. **Tick "Add python.exe to PATH"** at the bottom of the first screen — this is
   the step people miss, and nothing works without it
2. Click **Install Now**
3. If you see **"Disable path length limit"** on the final screen, click it —
   the Z: paths are long

---

### Step 3 — Confirm Python installed

Open **Command Prompt** (press Start, type `cmd`, Enter) and run:

```
python --version
```

You should see something like `Python 3.13.1`.

**If you get "not recognized as an internal or external command":** PATH wasn't
set. Re-run the installer, choose **Modify**, and make sure **Add python.exe to
PATH** is ticked. Then close and reopen Command Prompt.

---

### Step 4 — Install the packages

In the same Command Prompt:

```
pip install pandas openpyxl requests
```

This takes a minute or two. It's per-machine, not per-user, and you only do it
once.

---

### Step 5 — Confirm the packages installed

```
python -c "import pandas, openpyxl, requests; print('all good')"
```

If it prints `all good`, you're set. Any `ModuleNotFoundError` means that
package didn't install — run the pip command again and read the output for the
failure.

---

### Step 6 — Test-run the pipeline

1. Open `Z:\kumarlab-new\Tim Leach\Scripts\API Pipeline`
2. Double-click `sing_pipeline_v2_20260817.py`

**Expected:** a dark window opens with six checkboxes.

To confirm Climb access without changing anything:

1. Leave **Schedule Harvest** ticked, untick the other five
2. Click through to the pre-flight screen
3. It should report the animal and birth counts pulled from Climb
4. **Close the window** at that point — don't continue to scheduling

Seeing the counts means Python, the packages, the network and the Climb
credentials are all working.

**If the window flashes and disappears:** Python is installed but `.py` files
aren't associated with it. Right-click the script → **Open with** → **Choose
another app** → **Python** → tick **Always use this app**.

**If nothing happens at all:** open Command Prompt and run it directly so you
can see the error:

```
cd /d "Z:\kumarlab-new\Tim Leach\Scripts\API Pipeline"
python sing_pipeline_v2_20260817.py
```

---

### You do not need to

- Copy the folder to your machine
- Set up credentials — `lib\sing_credentials.json` is shared and already there
- Install Git, an IDE, or anything else
- Have a personal Climb API account

---

### Working in a shared folder

**Only one person runs the pipeline at a time.** Every run overwrites
`animals.csv` and `births.csv` in the shared folder. Two people running at once
will clobber each other's data mid-run. Check with the lab before starting.

**Everything you generate is visible to everyone.** Output files land in the
shared folder, named by harvest date. Nothing is overwritten — a second run adds
` (1)` — so you won't destroy someone else's export, but the folder does fill up.

**Records created in Climb are attributed to the lab, not to you.** The shared
credentials are a Kumar Lab workgroup API account, so everything shows
`Created By: JAX - Kumar Lab via API` regardless of who ran it.

---

### Credentials reference

You shouldn't need to touch this — it's already set up in the shared folder.
Recorded here in case it needs replacing.

`lib\sing_credentials.json`:

```json
{
    "client_id":     "your-client-id-here",
    "client_secret": "your-client-secret-here",
    "workgroup_key": "428"
}
```

Copy `sing_credentials.json.template` and fill it in. Never commit this file to
Git.

Contact Instem (help@instem.com) for a `client_id` and `client_secret` on the
Kumar Lab workgroup.

> **Climb moves to OAuth-only authentication on 3 November 2026.** The pipeline
> already uses the `client_credentials` grant, so this should only mean new
> credential values — not a code change. Note that JCMS to Climb has its own
> separate copy of the credentials file that will need the same update.

---

## Running it

Double-click the script.

### Screen 1 — Pick your modules

Six checkboxes, all on by default. Untick anything you don't need.

| Module | What it does | Data source |
|---|---|---|
| **Schedule Harvest** | Pull animals from Climb, assign to harvest dates | Climb API |
| **Generate Labels** | MRI, MERFISH and RNA-Seq label sheets + tube labelers | `animals.csv` |
| **Create Climb Samples** | Register samples in Climb for scheduled animals | `animals.csv` |
| **Climb to Envision Translation** | Envision-formatted sheet for tag attachment | `animals.csv` |
| **Export Deliverables Sheet** | Collaborator deliverables workbook | `animals.csv` |
| **Sing Sanity** | Cross-check Harvest Worksheet against the trackers | Tracker XLSXs |

Your selection decides what happens next:

- **Schedule ticked** → pre-flight, then a live Climb pull
- **Schedule unticked** → file check for `animals.csv` in the script folder

### Screen 2 — Pre-flight / file check

**With Schedule:** confirms `Sing Harvest Sheet.xlsx` is present, then pulls
`animals.csv` and `births.csv` fresh from Climb.

**Without Schedule:** looks for `animals.csv` in the script folder. Drop your
targeted export there first — this is a specific list for one run, not the whole
Climb inventory.

### Screen 3 — Harvest dates

Reads the Harvest Worksheet directly and shows every upcoming harvest date with
its P14 and Adult counts. There's no capacity cap.

### Screen 4 — Harvest assignment review

Every animal with its suggested harvest type, colour-coded by type. Change any
assignment before confirming.

Sorted by timepoint, then date, then strain, then **sex** — so cage groups stay
together the way they're housed.

### Screen 5 — Progress, then summary

Live log while it runs, then a list of everything written.

---

## Input files

| File | Needed for | Where it comes from |
|---|---|---|
| `Sing Harvest Sheet.xlsx` | Schedule, Labels, Deliverables, Sanity | Download from Google Sheets |
| `Animal and sample tracking.xlsx` | Sing Sanity | Download from Google Sheets |
| `MERFISH-RNASeq_SampleTracker.xlsx` | Sing Sanity | Download from Google Sheets |
| `animals.csv` | Any module when Schedule is off | Export from Climb |
| `births.csv` | *(automatic)* | Climb API |

When Schedule runs, `animals.csv` and `births.csv` are overwritten with a fresh
Climb pull every time. Only Sing Inventory animals are kept — the Use field is
matched exactly.

---

### Exporting `animals.csv` from Climb

Only needed when you're running **without** Schedule Harvest — a targeted list
for one run, not the whole inventory.

1. In Climb, filter the animal grid down to the animals for this run
2. **Select all columns** in the column picker
3. Export to CSV
4. Save it into the pipeline folder as `animals.csv`, overwriting the old one

**Select all columns.** It's one click and it guarantees nothing is missing.
Column order does not matter — the pipeline matches on header names, so however
Climb happens to order them is fine.

These columns must be present or the run fails with a message naming what's
missing:

| Column | Used for |
|---|---|
| `Name` | Animal ID throughout |
| `Birth Date` | Age and timepoint calculations |
| `Sex` | Cage grouping, Envision groups |
| `Line (Short)` | Strain, Envision Group label |
| `Line` | Full strain nomenclature on Envision sheets |
| `Genotype` | Genotype filtering and canonicalisation |
| `Use` | Filtered to Sing Inventory |
| `Status` | Alive filter |
| `Birth ID` | Litter and cage grouping |
| `Housing ID` | Cage number on Envision sheets |
| `Marker Type` | Ear notch identification |

Nine of these are checked up front. `Line` and `Housing ID` are checked by the
Envision module when it runs, so a CSV missing them will pass the file check and
then fail partway through — another reason to just select all columns.

---

## Output files

Everything is written next to the script.

| File | From |
|---|---|
| `Complete_Schedule_YYYYMMDD_HHMMSS.xlsx` | Schedule |
| `Harvest_Sheet_Import_YYYYMMDD_HHMMSS.xlsx` | Schedule |
| `Envision_YYYY_MM_DD.xlsx` | Envision |
| `Labels_Mailmerge_sheet1_YYYY_MM_DD.xlsx` | Labels |
| `Tube_Labeler_RNA_YYYY_MM_DD.xlsx` | Labels |
| `Lab_Data_Export_YYYY_MM_DD.xlsx` | Deliverables |

### One file per harvest date

Envision, Labels and Tube Labeler files are named for the **harvest date**, not
the time they were generated. A run covering two harvest dates produces two of
each:

```
Envision_2026_08_19.xlsx
Envision_2026_08_26.xlsx
```

Nothing is ever overwritten. A second run adds ` (1)`, then ` (2)`, and so on.

### Labels always start at position 1

There's no prompt asking where you are on the label sheet. Each dated file is
self-contained and starts from the first label — print it on a fresh sheet.

---

## Genotypes

Every genotype is normalised to one of seven labels:

| Label | Means |
|---|---|
| `Wild` | `+/+`, WT |
| `Het` | `+/-`, `-/+` |
| `Hom` | `-/-` |
| `Hemi` | `-/Y`, `tg/+` |
| `Inbred` | B6 background strains |
| `Blank` | No genotype on record — never genotyped |
| `Inconclusive` | Genotyped, but the assay gave no usable call |

`Blank` and `Inconclusive` are **separate**. Both block scheduling, but they're
handled differently:

- Blank → *Blank Genotype — Genotype Needed*
- Inconclusive → *Inconclusive Genotype — Released to Available*

`Inconclusive` matches Climb's genotype symbol of the same name. It is a TGS
term and cannot arrive from anywhere else.

Neither is re-genotyped. After scheduling, Wild and Inconclusive animals have
their Use set to **Available**, which takes them out of the Sing pool — except
on the Shank3, Bcl11b and Scn1a (Dravet) lines, which keep theirs. See
`RELEASE_EXCLUDE_LINES` in the script.

Blank animals are *not* released — they've never been genotyped, so they stay
pending a result.

---

## Scheduling rules

| Setting | Value |
|---|---|
| Cage size | 3 |
| Wednesday capacity | No cap |
| P14 harvest days | Monday–Friday |
| P56 behavior window | Day 42–49, Wednesdays |
| P56 harvest | 14 days after behavior |
| Harvest targets per group | 5 Perfusion, 1 MERFISH, 1 RNAseq |

Adjust these in the `CONFIG` block near the top of the script.

---

## Known limitations

**Sing Sanity** is a stub inside the pipeline. The module runs but prints a
message telling you to use the standalone `Sing_Sanity.py`. Porting the logic in
is outstanding.

**Google Sheets** writes are blocked. JAX enforces
`iam.disableServiceAccountKeyCreation`, which prevents downloading a service
account key, and Workspace policy blocks `gcloud auth application-default login`
with Sheets/Drive scopes. `sing_google.py` is written and waiting. Contact Alex
Berger (Kumar Lab, Slack UGJ9F86TE).

**Deliverables tab filtering** derives the Preservation column from Protocol when
it isn't in the source data, so each tracker tab only gets its own animals. This
needs testing against a wider range of real exports.

---

## Troubleshooting

**Window closes instantly**
The pipeline writes `pipeline_error.log` to its folder on a crash.

**Window flashes and disappears immediately**
`.py` files aren't associated with Python. Right-click the script → **Open with**
→ **Choose another app** → **Python** → tick **Always use this app**.

**"python is not recognized"**
Python was installed without **Add python.exe to PATH**. Re-run the installer,
choose **Modify**, tick that box, then reopen Command Prompt.

**"ModuleNotFoundError: No module named 'pandas'"**
Run `pip install pandas openpyxl requests` in Command Prompt.

**Can't reach the Z: drive**
Check you're on the JAX network or VPN, then re-map the drive (Step 1).

**Someone else's data appeared mid-run**
Two people ran at once and overwrote each other's `animals.csv`. Wait until the
other run finishes, then start again from the beginning.

**"sing_climb not found"**
The error names both folders it searched. Put `sing_climb*.py` in `lib\`.

**"Could not fetch animals from Climb"**
Check you're on the JAX network, then check `lib\sing_credentials.json` exists
and hasn't been emptied.

**A module produced no file**
Check the log for that module's section. The most common cause is a missing
column in `animals.csv` — the error names which ones.

**Confirm button does nothing**
Fixed in the 2026-08-12 build. If you're on an older copy, update.

---

## Version history

| Version | Date | Summary |
|---|---|---|
| v2.6.0 | 2026-08-17 | `lib\` subfolder for support files; `Inconclusive` split from `Blank` as its own genotype label |
| v2.5.0 | 2026-08-14 | Per-harvest-date output naming with collision-safe suffixes; label offset prompt removed; Envision Group uses Line (Short) |
| v2.4.5 | 2026-08-12 | Module selector screen; partial runs read a local CSV; dark theme; Harvest Sheet read directly from XLSX — no more Google Sheets CSV export |
| v2.4.0 | 2026-07-21 | Climb API integration — live data fetch, credentials in `sing_credentials.json` |
| v2.3.0 | 2026-06-04 | Colony rotation screen + standalone tracker |
| v2.2.x | 2026-05-28 | Genotype validation, Shank3 Het scheduling fix |

---

## Related

**JCMS to Climb** lives in its own folder with its own copy of `sing_climb` and
credentials. See `README_JCMS_to_Climb.docx`.

**Repo:** `github.com/timleach-J/Sing-Pipeline-Scheduler` (private)
