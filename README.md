# SING Pipeline Scheduler

**Kumar Lab — The Jackson Laboratory**
Scalable and Systematic Neurobiology of NPD Risk Genes (SING)
NIH-funded, 2024–2029

---

## Overview

The SING Pipeline Scheduler is a Python/tkinter GUI tool that automates harvest scheduling, Envision tagging prep, sample tracking, and output file generation for the SING project. It processes animal colony data exported from Climb/Envision and produces a complete set of Excel files ready for the harvest workflow.

Built and maintained by Tim Leach, Research Technician, Kumar Lab.

---

## Files

| File | Version | Status | Notes |
|---|---|---|---|
| `sing_pipeline.py` | v1.7 | Stable | Previous production version — fallback if needed |
| `sing_pipeline_v2.py` | v2.0 | Current | Active production version |

---

## Requirements

```
Python 3.9+
pandas
openpyxl
tqdm (optional — gracefully falls back if not installed)
tkinter (included with standard Python)
```

Install dependencies:
```bash
pip install pandas openpyxl tqdm
```

---

## Running the Pipeline

Double-click `sing_pipeline_v2.py`, or run from the command line:

```bash
python sing_pipeline_v2.py
```

The GUI walks through four screens:

1. **File Setup** — select input files (auto-detected from the script folder)
2. **Wednesday Capacity** — enter how many behavior slots are already booked per Wednesday
3. **Harvest Assignment Review** — review and adjust harvest type per animal, then confirm
4. **Running** — pipeline executes with live log output
5. **Summary** — output files listed with file sizes

---

## Input Files

Place these in the same folder as the script before running:

| File | Description |
|---|---|
| `animals.csv` | Alive animal export from Climb |
| `Sing Harvest Sheet - Summary Sheet.csv` | Tracking sheet with completed harvest counts per strain |
| `births.csv` | Birth records export from Climb |
| `harvest_overrides.csv` | Auto-generated after each run — shows confirmed assignments for reference |

---

## Output Files

All outputs are saved to the same directory as the script, timestamped:

| Output | Description |
|---|---|
| `Complete_Schedule_*.xlsx` | Master harvest schedule |
| `Harvest_Sheet_Import_*.xlsx` | Per-date harvest sheets |
| `Climb_Sample_Import_*.xlsx` | Sample import file for Climb |
| `Lab_Data_Export_*.xlsx` | Deliverables tracker (Perfusion, MERFISH, RNAseq sheets) |
| `Envision_*.xlsx` | Climb-to-Envision tag import file |
| `Labels_Mailmerge_*.xlsx` | Printable sample labels |
| `harvest_overrides.csv` | Confirmed harvest type assignments from the GUI review |
| `logs/scheduler_*.log` | Full run log with diagnostics |

---

## Harvest Assignment Review

The review screen shows every schedulable animal with its auto-suggested harvest type. You can:

- Change any animal's harvest type using the dropdown
- Mark animals as **Do Not Schedule** to exclude them from this run
- Mark animals as **Extra** to include them outside the quota
- Use **Reset to Suggested** to revert all changes
- Use **Skip / Use Auto-Assignments** to bypass the review entirely

Row colors indicate the current harvest type assignment. The color key is shown at the bottom of the animal list.

The Quota Comparison panel on the right shows how your selections track against the remaining needs from the tracking sheet. A warning will appear if any harvest type is assigned more times than needed (over quota).

---

## Key Scheduling Logic

- **P14 harvest**: Birth date + 14 days. Must fall Mon–Fri. Has a strict 2-hour collection window — must be scheduled in advance.
- **P56 behavior**: First Wednesday falling in the P42–P49 window (age in days).
- **P56 harvest**: Behavior date + 14 days (always exactly 2 weeks later).
- **Envision tagging**: Always exactly 2 weeks before the harvest date.
- **Capacity**: Wednesday behavior sessions capped at 18 animals per Wednesday.
- **Toe clip animals**: Excluded from P56/behavior (gait effects). Ear-notched animals used for P56.

### P56 Behavior-Complete Strains

These strains have completed behavior and are blocked from new P56 scheduling:

`CDKL5, C3, GRN, FMR1, KCND3, FBN1, SHANK3, CNTNAP2, CACNA1A`

---

## Configuration

All tunable parameters live in the `CONFIG` dict near the top of the script. Key settings:

```python
'WEDNESDAY_CAPACITY': 18,       # Max animals per behavior Wednesday
'CAGE_SIZE': 3,                 # Animals per cage
'P14_VALID_DAYS': [0,1,2,3,4], # Mon–Fri
'HARVEST_TARGETS': {            # Per-strain per-sex targets
    'Perfusion': 5,
    'MERFISH': 1,
    'RNAseq': 1
},
```

---

## Version History

### v2.0 — 2026-03-27 (`sing_pipeline_v2.py`)

**GUI**
- Harvest Assignment Review: fixed selection capture — user choices now reliably carry through to outputs
- Harvest Assignment Review: date of harvest/behavior now shown per animal
- Harvest Assignment Review: row colors update when harvest type is changed
- Harvest Assignment Review: color key legend added
- Harvest Assignment Review: quota warning only fires when over quota (not under)
- Harvest Assignment Review: Do Not Schedule correctly excludes animals from the run
- `harvest_overrides.csv` always written after GUI confirmation, reflecting actual selections
- Wednesday capacity screen: cleaner layout with text status indicators
- Screen 1: auto-detects input files from script folder

**Outputs**
- Genotype column in deliverables now shows the raw Climb genotype string, not the canonical label
- Identification column in harvest sheet reads from `Marker` column (not `Marker Type`)
- MERFISH sample tracker: column order updated to match submission format
- Wean Date for P14 animals set to harvest date across all tracker sheets

**Code quality (Python Expert refactor)**
- Narrowed `warnings.filterwarnings` scope
- Fixed bare `except` clauses
- Removed redundant `date as date_type` alias and unused `timezone` import
- Added docstrings to key functions
- Replaced ~30 `len(df) == 0` checks with `df.empty`
- Vectorized `filter_animals_by_use`
- Fixed Windows Unicode crash from emoji in log output

### v1.7 (`sing_pipeline.py`)
Previous production version — kept as fallback.

---

## Project Context

SING spans three institutions — JAX (Kumar Lab), Penn State (Paul Lab), and NYU — and tracks 8 active strains out of 114 total over the project lifetime. Animals are housed in rooms B6 and F29.

**Key collaborators:** Marina Santos (OFA behavior), Tuan Nguyen (data analysis), Fionna Kennedy (Envision tagging and animal entry), Sean Deats (harvester).

---

*Questions or issues: contact Tim Leach, Kumar Lab, JAX.*
