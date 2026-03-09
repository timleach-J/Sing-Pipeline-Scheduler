# Sing Animal Scheduler

A Python script that handles animal scheduling and harvest processing for the Kumar Lab's contribution to the SING Project. It runs as a full GUI application — no terminal interaction required after launch.

---

## What It Does

The pipeline runs in two phases:

**Phase 1 — Scheduling**
- Reads the animal inventory, harvest tracking sheet, and births file
- Checks eligibility for P14 and P56 timepoints
- Assigns animals to harvest types (Perfusion, MERFISH, RNAseq, Extra)
- Outputs a complete schedule Excel file

**Phase 2 — Harvest Pipeline**
- Reads the schedule output in memory
- Produces the Harvest Worksheet, Deliverables, Envision plate reader file, and Labels

---

## Input Files

Place these files in the same folder as `unified_pipeline_2.py`:

| File | Required | Purpose |
|------|----------|---------|
| `animals.csv` | ✅ Yes | Main colony inventory |
| `Sing Harvest Sheet - Summary Sheet.csv` | Optional | Quota tracking by strain/type |
| `births.csv` | Optional | Improves P14 scheduling accuracy |
| `harvest_overrides.csv` | Optional | Pin specific animals to specific harvest types (auto-generated on first run) |

---

## How to Run

```
python unified_pipeline_2.py
```

Or just double-click the file. The GUI will open automatically.

**Screen 1 — File Setup:** Confirm which input files are available. The script auto-detects files in the same folder.

**Screen 2 — Wednesday Capacity:** Enter how many behavior slots are already booked for the next 6 Wednesdays.

**Screen 3 — Pipeline Running:** The pipeline runs in the background. A Harvest Assignment Review window will appear mid-run — review and confirm or adjust the harvest type for each animal.

**Screen 4 — Summary:** Lists all output files produced with their sizes.

---

## Output Files

| File | Description |
|------|-------------|
| `Complete_Schedule_{timestamp}.xlsx` | Full schedule — P14, P56, capacity, strain summary, requirements status |
| `Harvest_Sheet_Import_{timestamp}.xlsx` | Harvest worksheet import file |
| `Climb_Sample_Import_{timestamp}.xlsx` | Sample import for Climb |
| `Lab_Data_Export_{timestamp}.xlsx` | Lab data (4 sheets) |
| `Envision_{timestamp}.xlsx` | Envision plate reader file |
| `Labels_Mailmerge_{timestamp}_sheet*.xlsx` | Perfusion label mail-merge sheets |
| `Tube_Labeler_RNA_{timestamp}.xlsx` | RNA tube labeler file (Sides + Tops tabs) |
| `harvest_overrides.csv` | Auto-generated on first run — edit to override harvest assignments |
| `logs/scheduler_{timestamp}.log` | Full run log |
| `backup_{timestamp}/` | CSV backups |

---

## Harvest Type Override

To manually pin animals to a specific harvest type before running, edit `harvest_overrides.csv` (auto-generated on first run). Change the `Harvest_Type` column for any animal. Leave blank to auto-assign.

Valid values: `Perfusion`, `MERFISH`, `RNAseq`, `Extra`

---

## Requirements

- Python 3.8+
- pandas
- openpyxl
- tkinter (included with standard Python on Windows)

Install dependencies:
```
pip install pandas openpyxl
```

---

## Notes

- The script is designed for the Sing Lab colony at JAX
- Extra animals attend Wednesday behavior but are not harvested and do not count toward quota
- Do Not Schedule removes an animal from the run entirely
