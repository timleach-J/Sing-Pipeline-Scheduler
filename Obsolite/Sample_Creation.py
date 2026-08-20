import pandas as pd
import sys
import os
import re
from datetime import datetime

# ── Genotype validation ───────────────────────────────────────────────────────
# If the gene name extracted from the genotype string does NOT match the
# Line (Short), the animal is flagged as a potential mismatch.
#
# Add known exceptions here — strains where the genotype gene name legitimately
# differs from the Line (Short) label.
# Format: { 'line_short_lowercase': ['acceptable_gene_name_lowercase', ...] }
GENOTYPE_EXCEPTIONS = {
    'dravet': ['scn1a'],   # Dravet lines carry Scn1a genotype
}

def get_starting_sample_number():
    """
    Ask user for the last sample number used.
    Returns the next sample number to use.
    """
    while True:
        try:
            last_sample = input("What was the last sample number used? ")
            last_sample_num = int(last_sample)
            next_sample = last_sample_num + 1
            confirm = input(f"Next sample will start at {next_sample}. Is this correct? (y/n): ")
            if confirm.lower() in ['y', 'yes']:
                return next_sample
        except ValueError:
            print("Please enter a valid number.")

def get_sample_count(protocol):
    """
    Determine how many samples to generate based on the protocol.
    Returns (count, suffixes) where suffixes is a list of suffixes to add to sample names.
    """
    protocol = str(protocol).strip()
    
    if protocol == "8 Weeks - 20mL PBS 25mL 4%PFA (Plus 20mL Storage) - 6mL/min":
        return (1, [""])
    elif protocol == "P14 - 15mL PBS 20mL 4%PFA (Plus 20mL Storage) - 4mL/min":
        return (1, [""])
    elif protocol == "MERFISH - OCT":
        return (2, ["", ""])
    elif protocol == "RNA-Seq":
        return (8, ["-0", "-1", "-2", "-3", "-4", "-5", "-6", "-C"])
    elif protocol == "Extra - Sex & Timepoint Full":
        return (0, [])
    else:
        # Default to 1 sample with no suffix for unknown protocols
        print(f"Warning: Unknown protocol '{protocol}'. Defaulting to 1 sample.")
        return (1, [""])

def get_preservation_method(protocol):
    """
    Determine preservation method based on protocol.
    Returns: "Flash Frozen", "4% PFA Fixed", or "OCT Block"
    """
    protocol = str(protocol).strip()
    
    if "MERFISH - OCT" in protocol:
        return "OCT Block"
    elif "RNA-Seq" in protocol:
        return "Flash Frozen"
    elif "PFA" in protocol:
        return "4% PFA Fixed"
    else:
        return ""

def is_rna_or_merfish(protocol):
    """Check if protocol is RNA-Seq or MERFISH"""
    protocol = str(protocol).strip()
    return protocol == "RNA-Seq" or protocol == "MERFISH - OCT"

def combine_sample_numbers(sample_list):
    """
    Combine sample numbers into range format.
    Example: [1-0, 2-1, 3-2, 4-3, 5-4, 6-5, 7-6, 8-C] -> "1-8"
    Example: [100, 101] -> "100-101"
    """
    if not sample_list:
        return ""
    
    # Extract base numbers (remove suffixes)
    base_numbers = []
    for sample in sample_list:
        sample_str = str(sample)
        # Remove suffix (everything after and including the dash)
        if '-' in sample_str:
            base_num = sample_str.split('-')[0]
        else:
            base_num = sample_str
        try:
            base_numbers.append(int(base_num))
        except:
            base_numbers.append(sample_str)
    
    if len(base_numbers) == 1:
        return str(base_numbers[0])
    else:
        # Return as range
        first = min(base_numbers)
        last = max(base_numbers)
        return f"{first}-{last}"

def load_animal_data(animals_csv_path):
    """
    Load animals.csv and return a dict mapping Name -> {id, genotype, line_short}.
    Also returns a plain name->id lookup for backwards compatibility.
    """
    empty = ({}, {})
    if not os.path.exists(animals_csv_path):
        print(f"  ⚠ Animals file not found: {animals_csv_path} — will use Animal Names")
        return empty

    try:
        if animals_csv_path.endswith('.xlsx') or animals_csv_path.endswith('.xls'):
            animals_df = pd.read_excel(animals_csv_path, dtype=str)
        else:
            animals_df = pd.read_csv(animals_csv_path, dtype=str, encoding='utf-8-sig')

        id_lookup   = {}   # name -> id string
        full_lookup = {}   # name -> {id, genotype, line_short}

        for _, row in animals_df.iterrows():
            name = None
            for col in ['Name', 'Animal Name', 'Mouse Name', 'Animal', 'MouseName']:
                if col in row.index and pd.notna(row.get(col)):
                    name = str(row.get(col)).strip()
                    break
            if not name:
                continue

            animal_id = None
            for col in ['ID', 'Animal ID', 'AnimalID', 'Mouse ID']:
                if col in row.index and pd.notna(row.get(col)):
                    try:
                        animal_id = str(int(float(str(row.get(col)))))
                    except (ValueError, TypeError):
                        animal_id = str(row.get(col)).strip()
                    break

            genotype   = str(row.get('Genotype',    '') or '').strip()
            line_short = str(row.get('Line (Short)', '') or '').strip()

            if animal_id:
                id_lookup[name] = animal_id
            full_lookup[name] = {
                'id':         animal_id or name,
                'genotype':   genotype,
                'line_short': line_short,
            }

        print(f"  ✓ Loaded animal data — {len(full_lookup)} animals")
        return id_lookup, full_lookup
    except Exception as e:
        print(f"  ⚠ Error loading animals file: {e}")
        return empty


# Keep old name as alias for any callers that use it directly
def load_animal_lookup(animals_csv_path):
    id_lookup, _ = load_animal_data(animals_csv_path)
    return id_lookup


def extract_gene_name(genotype_str: str):
    """
    Pull the gene name out of a CLIMB genotype string.
    'Cacna1a<em1Khor> -/+' → 'cacna1a'
    'Shank3<tm2Gfng> -/+'  → 'shank3'
    'Blank' / '' / '+/+'   → None  (WT or ungenotyped — skip check)
    """
    s = str(genotype_str).strip()
    if not s or s.lower() in ('', 'nan', 'blank', '+/+', 'wt'):
        return None
    m = re.match(r'^([A-Za-z][A-Za-z0-9]+)', s)
    return m.group(1).lower() if m else None


def validate_genotypes(harvest_df, full_lookup: dict, mouse_name_col: str) -> list:
    """
    For each animal in the harvest worksheet, check that the gene name in its
    genotype matches its Line (Short).  Returns a list of mismatch dicts.
    """
    mismatches = []
    for _, row in harvest_df.iterrows():
        name = str(row.get(mouse_name_col, '') or '').strip()
        if not name:
            continue
        info = full_lookup.get(name)
        if not info:
            continue   # animal not in animals.csv — skip, ID lookup handles it

        genotype   = info.get('genotype',   '')
        line_short = info.get('line_short', '').lower()

        gene = extract_gene_name(genotype)
        if gene is None:
            continue   # blank/WT — nothing to check

        # Is this gene an accepted match for the strain?
        # Strip common colony-naming suffixes (KO, KI, HET, etc.) before comparing
        ls_stripped = re.sub(r'\s+(ko|ki|cre|null|tg)\s*$', '', line_short).strip()
        allowed = [line_short, ls_stripped] + [g.lower() for g in GENOTYPE_EXCEPTIONS.get(line_short, [])]
        if gene not in allowed:
            mismatches.append({
                'animal':     name,
                'line_short': info.get('line_short', ''),
                'genotype':   genotype,
                'gene_found': gene,
            })

    return mismatches

def fill_template(csv_path, template_path, animals_path, output_path, combined_output_path, timestamp):
    """
    Main function to read CSV and fill CSV template.
    Creates three output files:
    1. Individual samples (output_path)
    2. Combined samples per animal (combined_output_path)
    3. Updated harvest worksheet with sample numbers filled in
    """
    # Check if files exist
    if not os.path.exists(csv_path):
        print(f"Error: Could not find input file: {csv_path}")
        print(f"Current directory: {os.getcwd()}")
        print(f"Files in current directory: {os.listdir('.')}")
        return
    
    if not os.path.exists(template_path):
        print(f"Error: Could not find template file: {template_path}")
        print(f"Current directory: {os.getcwd()}")
        print(f"Files in current directory: {os.listdir('.')}")
        return
    
    # Load animal data (IDs + genotypes + Line Short for validation)
    animal_lookup, full_animal_data = load_animal_data(animals_path)
    if not animal_lookup:
        print("\n  ✗ ERROR: No animal IDs were loaded from animals.csv.")
        print("          The file must have an 'ID' column containing Climb animalId values.")
        print("          Export animals.csv using sing_climb.py, not a manual Climb export.")
        input("\nPress Enter to close...")
        return
    
    # Read the input file
    try:
        # Try reading as Excel first
        if csv_path.endswith('.xlsx') or csv_path.endswith('.xls'):
            df = pd.read_excel(csv_path, sheet_name="Harvest Worksheet")
            print(f"Successfully read Excel file with {len(df)} rows")
        else:
            # Try as CSV
            df = pd.read_csv(csv_path)
            print(f"Successfully read CSV file with {len(df)} rows")
    except Exception as e:
        print(f"Error reading input file: {e}")
        return
    
    # Print column names to help debug
    print(f"  Columns: {list(df.columns)}")

    # Try to find the correct column name for Mouse Name
    mouse_name_col = None
    for col in ['Mouse Name', 'Name', 'Animal Name', 'Animal', 'MouseName']:
        if col in df.columns:
            mouse_name_col = col
            print(f"  Using '{col}' as the mouse name column")
            break

    if not mouse_name_col:
        print("  ✗ ERROR: Could not find mouse name column!")
        print(f"  Available columns: {list(df.columns)}")
        return

    # ── Genotype validation ───────────────────────────────────────────
    print("\nChecking genotypes against strains...")
    mismatches = validate_genotypes(df, full_animal_data, mouse_name_col)
    if mismatches:
        print(f"\n{'!'*60}")
        print(f"  GENOTYPE MISMATCH — {len(mismatches)} animal(s) flagged")
        print(f"{'!'*60}")
        for m in mismatches:
            print(f"  ✗  {m['animal']:<20}  "
                  f"Strain: {m['line_short']:<12}  "
                  f"Gene in genotype: {m['gene_found']:<12}  "
                  f"Full genotype: {m['genotype']}")
        print(f"{'!'*60}")
        confirm = input("\nGenotype mismatches found. Continue anyway? (y/n): ")
        if confirm.strip().lower() not in ('y', 'yes'):
            print("Aborted. Correct the genotypes in Climb and re-run.")
            return
    else:
        print("  ✓ All genotypes match their strains")

    # Get the starting sample number from user
    next_sample_num = get_starting_sample_number()
    print(f"Starting with sample number: {next_sample_num}")
    
    # Load the template CSV
    try:
        template_df = pd.read_csv(template_path)
        print(f"Template loaded successfully with {len(template_df)} existing rows")
        print(f"Template columns: {list(template_df.columns)}")
    except Exception as e:
        print(f"Error loading template: {e}")
        return
    
    # Create list to store new rows (for individual file)
    new_rows = []
    
    # Create list to store combined rows (for combined file)
    combined_rows = []
    
    # Track sample assignments for updating harvest worksheet
    sample_assignments = {}  # Maps row index to sample number
    
    # Process each row in the CSV
    samples_added = 0
    rows_processed = 0
    
    for idx, row in df.iterrows():
        rows_processed += 1
        
        # Get values from the CSV row
        protocol = row.get('Protocol', '')
        mouse_name = row.get(mouse_name_col, '')
        harvest_date = row.get('Harvest Date', '')
        sample_number = row.get('Sample Number', '')
        
        # Check if mouse name is empty
        if pd.isna(mouse_name) or str(mouse_name).strip() == '':
            continue

        # Check if Sample Number field has a value
        if pd.notna(sample_number) and str(sample_number).strip() != '':
            continue

        # Get sample count and suffixes for this protocol
        count, suffixes = get_sample_count(protocol)

        # Get preservation method
        preservation = get_preservation_method(protocol)

        # Look up Animal ID — must come from animals.csv (ID column = animalId from Climb)
        mouse_name_str = str(mouse_name).strip()
        animal_id = animal_lookup.get(mouse_name_str)
        if animal_id is None:
            print(f"  ✗ ERROR: No Climb Animal ID found for '{mouse_name_str}'.")
            print(f"         Make sure animals.csv was exported from sing_climb.py")
            print(f"         (it must have an 'ID' column containing the Climb animalId).")
            print(f"         Skipping this animal — fix animals.csv and re-run.")
            rows_processed += 1
            continue

        print(f"  Row {rows_processed}: {mouse_name_str} — {count} sample(s) ({protocol[:30]}...)" if len(str(protocol)) > 30 else f"  Row {rows_processed}: {mouse_name_str} — {count} sample(s) ({protocol})")

        # Track samples for this animal (for combined file)
        animal_samples = []
        start_sample_num = next_sample_num
        
        # Generate samples
        for i in range(count):
            sample_name = f"{next_sample_num}{suffixes[i]}"
            
            # Create new row as dictionary
            new_row = {
                'Sample Name': sample_name,
                'Type': 'Brain',
                'Status': 'Available',
                'Preservation Method': preservation,
                'Date Harvest': harvest_date,
                'Date Expiration': '',
                'Description': '',
                'Source AnimalID': animal_id,
                'Source SampleID': '',
                'Volume': '',
                'Volume Units': '',
                'Project': '',
                'Notes': ''
            }
            
            new_rows.append(new_row)
            animal_samples.append(sample_name)
            print(f"    → {sample_name} ({preservation})")
            
            next_sample_num += 1
            samples_added += 1
        
        if count > 0:
            combined_sample_name = combine_sample_numbers(animal_samples)
            combined_row = {
                'Order': idx,
                'Animal Number': mouse_name_str,
                'Protocol': protocol,
                'Sample Numbers': combined_sample_name,
                'Date Harvest': harvest_date
            }
            combined_rows.append(combined_row)
            sample_assignments[idx] = combined_sample_name
    
    # === Save individual samples file ===
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        combined_df = pd.concat([template_df, new_df], ignore_index=True)
    else:
        combined_df = template_df
    
    try:
        combined_df.to_csv(output_path, index=False)
        print(f"\n{'='*50}")
        print(f"INDIVIDUAL SAMPLES FILE SAVED")
        print(f"{'='*50}")
        print(f"Processed {rows_processed} rows from input file")
        print(f"Added {samples_added} samples to the template")
        print(f"Total rows in output: {len(combined_df)}")
        print(f"Output saved to: {output_path}")
        print(f"Full path: {os.path.abspath(output_path)}")
    except Exception as e:
        print(f"Error saving individual samples file: {e}")
        import traceback
        traceback.print_exc()
    
    # === Save combined samples file ===
    if combined_rows:
        combined_animals_df = pd.DataFrame(combined_rows)
        combined_animals_df = combined_animals_df.sort_values('Order').drop('Order', axis=1)
        
        try:
            combined_animals_df.to_csv(combined_output_path, index=False)
            print(f"\n{'='*50}")
            print(f"COMBINED SAMPLES FILE SAVED")
            print(f"{'='*50}")
            print(f"Total animals: {len(combined_animals_df)}")
            print(f"Output saved to: {combined_output_path}")
            print(f"Full path: {os.path.abspath(combined_output_path)}")
        except Exception as e:
            print(f"Error saving combined samples file: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("No combined rows to save - no samples were generated")
    
    print(f"\n{'='*50}")
    print(f"UPDATING HARVEST WORKSHEET")
    print(f"{'='*50}")
    print(f"  Updating {len(sample_assignments)} rows with sample numbers")
    
    # Update the original dataframe using the stored assignments
    for idx, sample_number in sample_assignments.items():
        df.at[idx, 'Sample Number'] = sample_number
    
    # Save the updated harvest worksheet
    updated_harvest_path = f"updated_harvest_{timestamp}.csv"
    try:
        if csv_path.endswith('.xlsx') or csv_path.endswith('.xls'):
            # If input was Excel, save as Excel
            df.to_excel(updated_harvest_path.replace('.csv', '.xlsx'), 
                       sheet_name="Harvest Worksheet", index=False)
            updated_harvest_path = updated_harvest_path.replace('.csv', '.xlsx')
        else:
            df.to_csv(updated_harvest_path, index=False)
        
        print(f"\n{'='*50}")
        print(f"UPDATED HARVEST WORKSHEET SAVED")
        print(f"{'='*50}")
        print(f"Updated {len(sample_assignments)} rows with sample numbers")
        print(f"Updated harvest worksheet saved to: {updated_harvest_path}")
        print(f"Full path: {os.path.abspath(updated_harvest_path)}")
    except Exception as e:
        print(f"Error saving updated harvest worksheet: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("="*50)
    print("CSV Template Filler")
    print("="*50)
    
    # Get current date and time for filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"Timestamp: {timestamp}")
    
    # Auto-detect latest Harvest_Sheet_Import file from pipeline
    CSV_PATH      = "Sing Harvest Sheet - Harvest Worksheet.csv"
    TEMPLATE_PATH = "add sample.csv"
    ANIMALS_PATH  = "animals.csv"
    OUTPUT_PATH          = f"filled_template_{timestamp}.csv"
    COMBINED_OUTPUT_PATH = f"combined_samples_{timestamp}.csv"
    import glob
    harvest_imports = sorted(glob.glob("Harvest_Sheet_Import_*.xlsx"), reverse=True)
    if harvest_imports:
        CSV_PATH = harvest_imports[0]
        print(f"Auto-detected harvest file: {CSV_PATH}")
    elif not os.path.exists(CSV_PATH):
        # Try .xlsx extension
        excel_path = CSV_PATH.replace('.csv', '.xlsx')
        if os.path.exists(excel_path):
            CSV_PATH = excel_path
        elif os.path.exists("Sing Harvest Sheet - Harvest Worksheet.xlsx"):
            CSV_PATH = "Sing Harvest Sheet - Harvest Worksheet.xlsx"
    
    # Check for template variations
    if not os.path.exists(TEMPLATE_PATH):
        if os.path.exists("add sample"):
            TEMPLATE_PATH = "add sample"
    
    # Check for animals file variations
    if not os.path.exists(ANIMALS_PATH):
        if os.path.exists("animals.xlsx"):
            ANIMALS_PATH = "animals.xlsx"
        elif os.path.exists("Animals.csv"):
            ANIMALS_PATH = "Animals.csv"
        elif os.path.exists("Animals.xlsx"):
            ANIMALS_PATH = "Animals.xlsx"
    
    # You can also use command line arguments (but timestamp is always added)
    if len(sys.argv) >= 4:
        CSV_PATH = sys.argv[1]
        TEMPLATE_PATH = sys.argv[2]
        ANIMALS_PATH = sys.argv[3]
        # Still add timestamp to output files
        if len(sys.argv) >= 5:
            base_output = sys.argv[4]
            OUTPUT_PATH = f"{base_output.replace('.csv', '')}_{timestamp}.csv"
        if len(sys.argv) >= 6:
            base_combined = sys.argv[5]
            COMBINED_OUTPUT_PATH = f"{base_combined.replace('.csv', '')}_{timestamp}.csv"
    
    print(f"\nInput file: {CSV_PATH}")
    print(f"Template file: {TEMPLATE_PATH}")
    print(f"Animals file: {ANIMALS_PATH}")
    print(f"Individual samples output: {OUTPUT_PATH}")
    print(f"Combined samples output: {COMBINED_OUTPUT_PATH}\n")
    
    fill_template(CSV_PATH, TEMPLATE_PATH, ANIMALS_PATH, OUTPUT_PATH, COMBINED_OUTPUT_PATH, timestamp)
    
    # Keep window open
    input("\nPress Enter to close...")