import pandas as pd
from collections import defaultdict
import re
import os
from datetime import datetime

def clean_genotype_base(genotype, strain):
    """Remove <content>, Probe, Generic LacZ tg/0, and zygosity markers including -/Y"""
    if pd.isna(genotype):
        return ""
    
    # Handle C57BL/6 strains specially
    if pd.notna(strain):
        strain_str = str(strain).strip()
        if strain_str == 'C57BL/6NJ':
            return 'B6NJ'
        elif strain_str == 'C57BL/6J':
            return 'B6J'
    
    result = str(genotype)
    
    # Remove content between ALL types of brackets/angle characters
    # Standard angle brackets
    result = re.sub(r'<[^>]*>', '', result)
    # Single angle quotation marks
    result = re.sub(r'‹[^›]*›', '', result)
    # Curly quotes/angle brackets
    result = re.sub(r'â€¹[^â€º]*â€º', '', result)
    # Square brackets
    result = re.sub(r'\[[^\]]*\]', '', result)
    # Parentheses (sometimes used)
    result = re.sub(r'\([^\)]*\)', '', result)
    
    # Remove any remaining bracket/angle characters
    result = result.replace('<', '').replace('>', '')
    result = result.replace('‹', '').replace('›', '')
    result = result.replace('â€¹', '').replace('â€º', '')
    result = result.replace('[', '').replace(']', '')
    result = result.replace('(', '').replace(')', '')
    
    # Remove Probe
    result = re.sub(r'Probe\s*', '', result)
    # Remove Generic LacZ tg/0,
    result = re.sub(r'Generic LacZ tg/0,\s*', '', result)
    # Normalize Climb numbering style before removing zygosity
    result = re.sub(r'\bHET\d*\b', 'Het', result, flags=re.IGNORECASE)
    result = re.sub(r'\bHOM\d*\b', 'Hom', result, flags=re.IGNORECASE)

    # Remove zygosity markers (including -/Y for X-linked hemizygous males)
    result = result.replace('-/-', '').replace('-/+', '').replace('+/-', '').replace('-/Y', '').replace('+/Y', '')
    result = result.replace('Inbred', '').replace('Het', '').replace('Hom', '')
    
    # Clean up extra spaces
    result = re.sub(r'\s+', ' ', result)
    
    return result.strip()

def clean_genotype(genotype):
    """Convert any Climb genotype string to standard display symbol."""
    if pd.isna(genotype):
        return ''
    raw = str(genotype).strip()
    if not raw or raw.lower() in ('nan', 'none', 'n/a', '-'):
        return ''
    s = raw.lower()
    s = re.sub(r'[‹<][^›>]*[›>]', '', s)
    s = re.sub(r'\[[^\]]*\]', '', s)
    s = re.sub(r'probe\s*', '', s)
    s = ' '.join(s.split())
    if 'inconclusive' in s:
        return '+/+'   # treated as wild — not usable for harvest
    if any(k in s for k in ('pending', 'failed', 'no call')):
        return 'Blank'
    if re.search(r'\bhom\d*\b|-/-', s):
        return '-/-'
    if re.search(r'\bhet\d*\b|-/\+|\+/-', s):
        return '+/-'
    if re.search(r'hem[i]?|tg/\+|\+/tg|-/y', s):
        return '-/Y'
    if re.search(r'\+/\+|\bwt\b|wild.?type', s):
        return '+/+'
    if 'inbred' in s:
        return 'Inbred'
    return 'Blank'

def natural_sort_key(name):
    """
    Create a sort key that handles numbers naturally.
    E.g., "Mouse2" comes before "Mouse10"
    """
    if pd.isna(name):
        return []
    
    # Split the name into text and number parts
    parts = re.split(r'(\d+)', str(name))
    # Convert numeric parts to integers for proper sorting
    return [int(part) if part.isdigit() else part.lower() for part in parts]

def assign_ear_tags_by_strain_sex(df):
    """
    Assign S4, S3, S2 in repeating pattern AFTER sorting by:
    1. Strain (Line)
    2. Sex
    3. Animal Name (natural sort)
    
    This ensures animals are grouped by strain and sex, then smallest names get ear tags first.
    Each group of 3 within the same strain/sex combination gets S4, S3, S2.
    """
    # Create a copy to avoid modifying during iteration
    df_sorted = df.copy()
    
    # Sort using a custom key function instead of adding a column
    # First, create a list of tuples: (index, Line, Sex, sort_key)
    sort_data = []
    for idx, row in df_sorted.iterrows():
        sort_data.append((
            idx,
            row.get('Line (Short)', row['Line']),
            row['Sex'],
            natural_sort_key(row['Name'])
        ))

    # Sort by Line (Short), then Sex, then natural sort key
    sort_data.sort(key=lambda x: (x[1], x[2], x[3]))

    # Get the sorted indices
    sorted_indices = [item[0] for item in sort_data]

    # Reorder the dataframe
    df_sorted = df_sorted.loc[sorted_indices].reset_index(drop=True)

    # Now assign ear tags in order after sorting
    # Within each strain/sex group, assign S4, S3, S2, S4, S3, S2...
    tags = []
    current_strain = None
    current_sex = None
    counter_within_group = 0

    for idx, row in df_sorted.iterrows():
        strain = row.get('Line (Short)', row['Line'])
        sex = row['Sex']
        
        # Check if we've moved to a new strain/sex combination
        if strain != current_strain or sex != current_sex:
            current_strain = strain
            current_sex = sex
            counter_within_group = 0
        
        # Assign ear tag based on position within this strain/sex group
        position = (counter_within_group % 3) + 1
        if position == 1:
            tags.append('S4')
        elif position == 2:
            tags.append('S3')
        else:  # position == 3
            tags.append('S2')
        
        counter_within_group += 1
    
    df_sorted['Envision Ear Tag'] = tags
    
    return df_sorted

def group_animals_by_housing(df):
    """
    Group animals with same Group ID, ensuring groups of 3 from same housing
    are labeled as 1, and next groups of 3 as 2, etc.
    """
    # Track group counts by Group name and Housing ID
    group_housing_counts = defaultdict(lambda: defaultdict(list))
    
    # First pass: organize by Group and Housing ID
    for idx, row in df.iterrows():
        group_name = row['Group_base']
        housing_id = row['Housing ID']
        group_housing_counts[group_name][housing_id].append(idx)
    
    # Second pass: assign suffixes
    group_suffixes = {}
    
    for group_name, housing_dict in group_housing_counts.items():
        total_count = sum(len(indices) for indices in housing_dict.values())
        
        if total_count <= 3:
            # 3 or fewer animals - no suffix needed
            for housing_id, indices in housing_dict.items():
                for idx in indices:
                    group_suffixes[idx] = group_name
        else:
            # More than 3 animals - need to add suffixes
            suffix_num = 1
            assigned_count = 0
            
            # Sort housing IDs for consistent ordering
            sorted_housing = sorted(housing_dict.items())
            
            for housing_id, indices in sorted_housing:
                for idx in indices:
                    # Determine which group of 3 this belongs to
                    current_suffix = ((assigned_count // 3) + 1)
                    group_suffixes[idx] = f"{group_name}{current_suffix}"
                    assigned_count += 1
    
    return group_suffixes

def create_envision_translation(input_file):
    """
    Main function to create Envision translation from animals CSV
    
    Parameters:
    input_file (str): Path to CSV file with animal data
    """
    try:
        # Get script directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Build full path for input
        input_path = os.path.join(script_dir, input_file)
        
        print("=" * 80)
        print("ENVISION TRANSLATION SCRIPT")
        print("=" * 80)
        print(f"Script location: {script_dir}")
        print(f"Looking for input: {input_path}")
        print("-" * 80)
        
        # Check if input file exists
        if not os.path.exists(input_path):
            print(f"\n❌ ERROR: Input file does not exist!")
            print(f"Expected: {input_path}")
            print(f"\nPlease make sure '{input_file}' is in the same folder as this script.")
            return None
        
        print(f"\n✓ Input file found!")
        file_size = os.path.getsize(input_path)
        print(f"  File size: {file_size:,} bytes")
        
        # Read the CSV file - try different encodings
        print("\nReading CSV file...")
        try:
            df = pd.read_csv(input_path, encoding='utf-8')
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(input_path, encoding='latin-1')
                print("  (Using latin-1 encoding)")
            except:
                df = pd.read_csv(input_path, encoding='cp1252')
                print("  (Using cp1252 encoding)")
        
        print(f"✓ Successfully read {len(df)} rows")
        print(f"  Columns: {list(df.columns)}")
        
        # Check for required columns
        required_columns = ['Genotype', 'Sex', 'Housing ID', 'Name', 'Line', 'Birth Date', 'Cohort']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"\n❌ ERROR: Missing required columns: {missing_columns}")
            print(f"Available columns: {list(df.columns)}")
            return None
        
        print(f"✓ All required columns present")
        
        # Get cohort name for filename
        cohort_name = df['Cohort'].iloc[0]
        if pd.isna(cohort_name):
            cohort_name = "Unknown"
        else:
            cohort_name = str(cohort_name).strip()
        
        # Sanitize cohort name for filename (remove invalid characters)
        cohort_name_clean = re.sub(r'[<>:"/\\|?*]', '_', cohort_name)
        
        # Create output filename with cohort, date and time
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"{cohort_name_clean}_{timestamp}.xlsx"
        output_path = os.path.join(script_dir, output_filename)
        
        print(f"\nOutput will be: {output_path}")
        print(f"Cohort: {cohort_name}")
        
        # Show first row as sample
        print("\nSample data (first row):")
        print(f"  Cohort: {df['Cohort'].iloc[0]}")
        print(f"  Name: {df['Name'].iloc[0]}")
        print(f"  Strain: {df['Line'].iloc[0]}")
        print(f"  Genotype: {df['Genotype'].iloc[0]}")
        print(f"  Sex: {df['Sex'].iloc[0]}")
        
        print("\n" + "-" * 80)
        print("PROCESSING DATA...")
        print("-" * 80)
        
        # Create genotype base for grouping (passing strain info)
        df['genotype_base'] = df.apply(lambda row: clean_genotype_base(row['Genotype'], row['Line']), axis=1)
        print("✓ Cleaned genotype base")
        if len(df) > 0:
            print(f"  Example: Strain='{df['Line'].iloc[0]}', Genotype='{df['Genotype'].iloc[0]}' → '{df['genotype_base'].iloc[0]}'")

        # ── SHANK3 split: rename Line (Short) to SHANK3-Het / SHANK3-Hom ──────
        # Mirrors the pipeline's check_eligibility behaviour so standalone output
        # matches the main schedule.
        _HETXHET_BASE = {'SHANK3'}   # add others here if more het×het strains are added
        def _split_line_short(row):
            ls = str(row.get('Line (Short)', '') or '').strip()
            if ls.upper() not in _HETXHET_BASE:
                return ls
            geno = str(row.get('Genotype', '') or '').strip().lower()
            if re.search(r'-/-|\bhom\d*\b', geno):
                return f"{ls}-Hom"
            if re.search(r'-/\+|\+/-|\bhet\d*\b', geno):
                return f"{ls}-Het"
            return ls

        if 'Line (Short)' in df.columns:
            df['Line (Short)'] = df.apply(_split_line_short, axis=1)
        # ─────────────────────────────────────────────────────────────────────

        # Get first letter of sex (capitalized)
        df['sex_initial'] = df['Sex'].str[0].str.upper()
        print("✓ Extracted sex initial")

        # Create base Group column — use Line (Short) so the strain name appears
        # in the group label (e.g. SHANK3-Hom-F3) rather than just the genotype.
        if 'Line (Short)' in df.columns:
            line_col = df['Line (Short)'].fillna(df['genotype_base'])
        else:
            line_col = df['genotype_base']
        df['Group_base'] = line_col + '-' + df['sex_initial']
        print("✓ Created base groups")
        
        # Show unique base groups
        unique_groups = df['Group_base'].unique()
        print(f"  Unique base groups: {list(unique_groups)}")
        
        # Assign group suffixes based on housing
        group_suffixes = group_animals_by_housing(df)
        df['Group'] = df.index.map(group_suffixes)
        print("✓ Assigned group numbers")
        
        # **MODIFIED: Sort by strain, sex, and name, THEN assign ear tags**
        print("\n🔄 Sorting animals by Strain → Sex → Name...")
        df = assign_ear_tags_by_strain_sex(df)
        print("✓ Sorted and assigned ear tags")
        
        # Show example of sorting
        if len(df) > 0:
            print(f"\n  First 10 animals after sorting:")
            for i in range(min(10, len(df))):
                print(f"    {i+1}. Strain: {df.iloc[i]['Line']:<30} Sex: {df.iloc[i]['Sex']:<8} Name: {df.iloc[i]['Name']:<15} Ear Tag: {df.iloc[i]['Envision Ear Tag']} Group: {df.iloc[i]['Group']}")
        
        # Clean genotype for output (keep original genotype, just clean brackets)
        df['Genotype_clean'] = df['Genotype'].apply(clean_genotype)
        print("\n✓ Cleaned genotypes for output")
        if len(df) > 0:
            print(f"  Example: '{df['Genotype'].iloc[0]}' → '{df['Genotype_clean'].iloc[0]}'")
        
        # Create output dataframe with exact column order
        output_df = pd.DataFrame({
            'Group': df['Group'],
            'Cage': df['Housing ID'],
            'Animal ID': df['Name'],
            'Envision Ear Tag': df['Envision Ear Tag'],
            'Strain': df['Line'],
            'Coat Color': '',
            'Genotype': df['Genotype_clean'],
            'Additional Detail': '',
            'Sex': df['Sex'],
            'Birth Date': df['Birth Date'],
            'Ear notch': '',
            'Metal ear tag': '',
            'Other ID': '',
            'RapID code': '',
            'RapID tag color': '',
            'RFID Tail Tattoo': ''
        })
        
        print(f"✓ Created output dataframe ({len(output_df)} rows, {len(output_df.columns)} columns)")
        
        print("\n" + "-" * 80)
        print("SAVING OUTPUT FILE...")
        print("-" * 80)
        
        # Save to Excel file
        output_df.to_excel(output_path, index=False, sheet_name='template_csv_v1.0', engine='openpyxl')
        print(f"✓ Excel file written")
        
        # Verify the file was created
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            print(f"✓ File verified: {output_path}")
            print(f"  File size: {file_size:,} bytes")
        else:
            print(f"❌ ERROR: File was not created!")
            return None
        
        # Print summary
        print("\n" + "=" * 80)
        print("GROUP SUMMARY")
        print("=" * 80)
        group_counts = output_df.groupby(['Group', 'Cage']).size().reset_index(name='Count')
        print(group_counts.to_string(index=False))
        
        # Show first few rows
        print("\n" + "=" * 80)
        print("SAMPLE OUTPUT (First 12 rows - showing sorted order by Strain→Sex→Name)")
        print("=" * 80)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        print(output_df[['Animal ID', 'Envision Ear Tag', 'Strain', 'Sex', 'Genotype', 'Group']].head(12).to_string(index=False))
        
        print("\n" + "=" * 80)
        print("✓✓✓ SUCCESS! ✓✓✓")
        print("=" * 80)
        print(f"Output file created: {output_filename}")
        print(f"Location: {output_path}")
        print(f"Total animals: {len(output_df)}")
        print(f"Sheet name: template_csv_v1.0")
        print(f"Note: Animals sorted by Strain → Sex → Name before ear tag assignment")
        print(f"      Each group of 3 within same Strain/Sex gets S4, S3, S2")
        print("=" * 80)
        
        return output_df
        
    except Exception as e:
        print("\n" + "=" * 80)
        print("❌ ERROR OCCURRED")
        print("=" * 80)
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        print("\nFull traceback:")
        import traceback
        traceback.print_exc()
        print("=" * 80)
        return None

# Main execution
if __name__ == "__main__":
    # Input file is always "animals.csv"
    input_filename = "animals.csv"

    print("\n" + "=" * 80)
    print(" ENVISION TRANSLATION")
    print("=" * 80)

    # ── Filtered CSV check ────────────────────────────────────────────────────
    print("""
This script expects a FILTERED animals.csv exported from Climb —
containing only the animals for a specific behavior session cohort.

Running it on the full Climb export (all Alive + Sing Inventory animals)
will produce an Envision file for every animal in the colony, not just
the ones scheduled for a given Wednesday.
""")
    while True:
        answer = input("Is your animals.csv filtered for a specific behavior cohort? (y/n): ").strip().lower()
        if answer in ('y', 'yes'):
            print("✓ Great — proceeding.\n")
            break
        elif answer in ('n', 'no'):
            print("""
⚠  Warning: You are about to run Envision translation on the full
   animals export. The output will include ALL colony animals, not
   just a single session cohort, and ear tag numbering will not
   reflect a real behavior session.

   To get a filtered export from Climb:
     1. Open the Animals list in Climb
     2. Filter to the cohort/behavior date you need
     3. Export that filtered view as animals.csv
     4. Place it in the same folder as this script and re-run.
""")
            proceed = input("Continue anyway? (y/n): ").strip().lower()
            if proceed in ('y', 'yes'):
                print("⚠  Proceeding with unfiltered data.\n")
                break
            else:
                print("Exiting. Re-run once you have the filtered CSV.")
                input("\nPress ENTER to close...")
                raise SystemExit(0)
        else:
            print("Please enter y or n.")
    # ─────────────────────────────────────────────────────────────────────────

    print(f"Looking for: {input_filename}")
    print(f"Output will be: [COHORT]_YYYYMMDD_HHMMSS.xlsx")

    # Wait for user to confirm
    input("\nPress ENTER to start processing...")

    result = create_envision_translation(input_filename)

    if result is not None:
        print("\n✓ Script completed successfully!")
        print("✓ Check the same folder as this script for the output file.")
    else:
        print("\n❌ Script failed. Please read the error messages above.")

    # Keep window open
    input("\nPress ENTER to exit...")