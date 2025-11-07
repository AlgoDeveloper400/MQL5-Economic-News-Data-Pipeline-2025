import pandas as pd
from pathlib import Path
import re
from pandas.tseries.offsets import DateOffset
from datetime import datetime
import numpy as np
import os
import gc

# ==== CONFIG - Using Environment Variables ====
main_batch_folder = Path(os.getenv('MAIN_BATCH_FOLDER', '/app/data/main_batch'))
monthly_batches_root = Path(os.getenv('MONTHLY_BATCH_FOLDER', '/app/data/monthly_batch'))
output_folder = Path(os.getenv('OUTPUT_FOLDER', '/app/data/merged_batch'))
output_folder.mkdir(parents=True, exist_ok=True)

print(f"📁 Using folders:")
print(f"   Main Batch: {main_batch_folder}")
print(f"   Monthly Batch: {monthly_batches_root}")
print(f"   Output: {output_folder}")

# Expected schema
EXPECTED_COLUMNS = ['Date', 'Time', 'Currency', 'Event', 'Impact', 'Actual', 'Forecast', 'Previous', 'IsHoliday', 'WeekRange']

# Optimize data types for memory efficiency
COLUMN_TYPES = {
    'Date': 'object',
    'Time': 'object', 
    'Currency': 'category',
    'Event': 'object',
    'Impact': 'category',
    'Actual': 'object',
    'Forecast': 'object',
    'Previous': 'object',
    'IsHoliday': 'category',
    'WeekRange': 'object'
}

def clear_output_folder():
    """Remove all files from the output folder to ensure only 1 file exists after saving"""
    for file_path in output_folder.glob("*"):
        if file_path.is_file():
            try:
                file_path.unlink()
            except Exception:
                pass

def optimize_dataframe(df):
    """Optimize DataFrame memory usage"""
    print("🔄 Optimizing DataFrame memory usage...")
    
    original_memory = df.memory_usage(deep=True).sum() / 1024**2  # MB
    
    # Convert columns to optimal types
    for col in df.columns:
        if col in COLUMN_TYPES:
            if COLUMN_TYPES[col] == 'category' and df[col].nunique() < len(df[col]) * 0.5:
                df[col] = df[col].astype('category')
            else:
                df[col] = df[col].astype(COLUMN_TYPES[col])
    
    optimized_memory = df.memory_usage(deep=True).sum() / 1024**2  # MB
    memory_saved = original_memory - optimized_memory
    
    print(f"💾 Memory usage: {original_memory:.2f}MB -> {optimized_memory:.2f}MB (saved {memory_saved:.2f}MB)")
    
    return df

def load_csv_with_memory_optimization(csv_path):
    """Load CSV with memory optimization and chunking if needed"""
    file_size = csv_path.stat().st_size / 1024**2  # Size in MB
    
    print(f"📦 Loading {csv_path.name} ({file_size:.2f} MB)")
    
    if file_size > 100:  # If file is larger than 100MB, use chunking
        print("   Using chunked loading for large file...")
        chunks = []
        for chunk in pd.read_csv(csv_path, chunksize=10000, low_memory=False):
            chunks.append(optimize_dataframe(chunk))
        df = pd.concat(chunks, ignore_index=True)
    else:
        df = pd.read_csv(csv_path, low_memory=False)
        df = optimize_dataframe(df)
    
    return df

def detect_and_fix_broken_rows(df):
    """Detect and fix broken rows where data is corrupted or missing columns"""
    broken_rows = []
    fixed_rows = []
    
    # Check each row for issues
    for idx, row in df.iterrows():
        row_issues = []
        
        # Check if Date column has incomplete date (missing year)
        if 'Date' in df.columns:
            date_val = str(row['Date']).strip()
            if is_broken_date(date_val):
                row_issues.append(f"Broken date: '{date_val}'")
        
        # Check if WeekRange is missing
        weekrange_col = 'WeekRange' if 'WeekRange' in df.columns else df.columns[-1]
        if pd.isna(row[weekrange_col]) or str(row[weekrange_col]).strip() == '':
            row_issues.append("Missing WeekRange")
        
        # Check if too many NaN values in a row
        nan_count = row.isna().sum()
        if nan_count > 3:
            row_issues.append(f"Too many missing values: {nan_count}")
        
        if row_issues:
            broken_rows.append((idx, row_issues))
    
    # Fix the broken rows
    for idx, issues in broken_rows:
        original_row = df.loc[idx].copy()
        
        # Fix broken date
        if any("Broken date" in issue for issue in issues):
            fixed_date = fix_broken_date(df, idx)
            df.loc[idx, 'Date'] = fixed_date
        
        # Fix missing WeekRange
        if any("Missing WeekRange" in issue for issue in issues):
            fixed_weekrange = generate_weekrange_from_date(df.loc[idx, 'Date'])
            weekrange_col = 'WeekRange' if 'WeekRange' in df.columns else df.columns[-1]
            df.loc[idx, weekrange_col] = fixed_weekrange
        
        fixed_rows.append(idx)
    
    if broken_rows:
        print(f"🔧 Fixed {len(broken_rows)} broken rows")
    
    return df

def is_broken_date(date_str):
    """Check if date string is broken (missing year)"""
    if pd.isna(date_str) or date_str == '':
        return True
    
    # Check for patterns that indicate broken dates
    broken_patterns = [
        r'^\d{1,2}\s+\w+,?\s*\w*day\s*$',
        r'^\w*day,?\s*\d{1,2}\s+\w+\s*$',
        r'^\d{1,2}\s+\w+\s*$',
        r'^\w+\s+\d{1,2}\s*$'
    ]
    
    for pattern in broken_patterns:
        if re.match(pattern, str(date_str).strip(), re.IGNORECASE):
            return True
    
    # Check if no 4-digit year is present
    if not re.search(r'\b\d{4}\b', str(date_str)):
        return True
        
    return False

def fix_broken_date(df, broken_idx):
    """Fix a broken date by finding nearby complete dates and imputing the year"""
    broken_date = str(df.loc[broken_idx, 'Date']).strip()
    
    # Extract month and day from broken date
    month_day = extract_month_day_from_broken_date(broken_date)
    if not month_day:
        return broken_date
    
    # Search for nearby complete dates to get the year
    search_range = 20
    start_idx = max(0, broken_idx - search_range)
    end_idx = min(len(df), broken_idx + search_range + 1)
    
    nearby_years = []
    
    for i in range(start_idx, end_idx):
        if i == broken_idx:
            continue
        
        nearby_date = str(df.loc[i, 'Date']).strip()
        
        # Extract year from nearby complete dates
        year_match = re.search(r'\b(\d{4})\b', nearby_date)
        if year_match:
            year = int(year_match.group(1))
            nearby_years.append(year)
    
    if nearby_years:
        # Use the most common year
        most_common_year = max(set(nearby_years), key=nearby_years.count)
        fixed_date = f"{month_day} {most_common_year}"
        return fixed_date
    
    # Fallback: use current year or a reasonable default
    fallback_year = 2024
    fixed_date = f"{month_day} {fallback_year}"
    return fixed_date

def extract_month_day_from_broken_date(broken_date):
    """Extract month and day from broken date string"""
    # Remove weekday names and extra commas
    cleaned = re.sub(r'\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b,?\s*', '', broken_date, flags=re.IGNORECASE)
    cleaned = cleaned.strip(' ,')
    
    # Try different patterns to extract month and day
    patterns = [
        r'(\d{1,2})\s+(\w+)',
        r'(\w+)\s+(\d{1,2})'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, cleaned)
        if match:
            if match.group(1).isdigit():
                day, month = match.group(1), match.group(2)
            else:
                month, day = match.group(1), match.group(2)
            
            # Convert month name to proper format
            month = month.capitalize()
            return f"{day} {month}"
    
    return None

def generate_weekrange_from_date(date_str):
    """Generate WeekRange from a date string"""
    try:
        # Try to parse the date
        parsed_date = None
        
        # Try different date formats
        date_formats = [
            '%d %B %Y',
            '%d %b %Y',
            '%B %d %Y',
            '%b %d %Y',
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%m/%d/%Y'
        ]
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(str(date_str).strip(), fmt)
                break
            except ValueError:
                continue
        
        if parsed_date:
            # Calculate week range (Monday to Sunday)
            days_since_monday = parsed_date.weekday()
            week_start = parsed_date - pd.Timedelta(days=days_since_monday)
            week_end = week_start + pd.Timedelta(days=6)
            
            # Format: "5 - 11 Oct, 2020"
            if week_start.month == week_end.month:
                week_range = f"{week_start.day} - {week_end.day} {week_end.strftime('%b, %Y')}"
            else:
                week_range = f"{week_start.strftime('%d %b')} - {week_end.strftime('%d %b, %Y')}"
            
            return week_range
    
    except Exception:
        pass
    
    return ""

def fix_csv_structure(csv_path):
    """Fix CSV structure and detect broken rows by parsing raw lines"""
    # Read the raw file to detect structure issues
    with open(csv_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Check if we have the right number of columns
    if len(lines) < 2:
        return pd.read_csv(csv_path)
    
    header_line = lines[0].strip()
    expected_cols = len(EXPECTED_COLUMNS)
    
    fixed_lines = []
    broken_line_count = 0
    
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
        
        # Count commas to estimate columns
        comma_count = line.count(',')
        estimated_cols = comma_count + 1
        
        if i == 0:  # Header
            if estimated_cols != expected_cols:
                # Fix header if needed
                if estimated_cols < expected_cols:
                    missing_cols = expected_cols - estimated_cols
                    line += ',' + ','.join([''] * missing_cols)
            fixed_lines.append(line)
        
        else:  # Data rows
            if estimated_cols < expected_cols:
                broken_line_count += 1
                # This is likely a broken row missing WeekRange
                missing_cols = expected_cols - estimated_cols
                fixed_line = line + ',' + ','.join([''] * missing_cols)
                fixed_lines.append(fixed_line)
            
            elif estimated_cols > expected_cols:
                broken_line_count += 1
                # Too many columns, try to merge some
                parts = line.split(',')
                if len(parts) > expected_cols:
                    # Keep first expected_cols-1 parts, merge the rest as the last column
                    fixed_parts = parts[:expected_cols-1]
                    merged_last = ','.join(parts[expected_cols-1:])
                    fixed_parts.append(merged_last)
                    fixed_line = ','.join(fixed_parts)
                    fixed_lines.append(fixed_line)
                else:
                    fixed_lines.append(line)
            
            else:
                fixed_lines.append(line)
    
    # Write fixed content to a temporary file and read it
    temp_file = csv_path.parent / f"temp_{csv_path.name}"
    with open(temp_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(fixed_lines))
    
    try:
        df = pd.read_csv(temp_file)
        temp_file.unlink()
        return df
    except Exception:
        temp_file.unlink()
        return pd.read_csv(csv_path)

# ==== IMPROVED DECEMBER BUG FIX FUNCTION ====
def fix_december_week_overlap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix December dates where the year is incorrectly set to the following year
    ONLY for the last week that actually overlaps into January of the next year.
    """
    # Store examples for output, organized by year
    correction_examples_by_year = {}
    
    # Iterate through rows and fix dates where needed
    for idx, row in df.iterrows():
        try:
            date_str = str(row['Date']).strip()
            week_range = str(row['WeekRange']).strip()
            
            # Skip rows without proper WeekRange
            if not week_range or ' - ' not in week_range:
                continue
            
            # Parse the WeekRange to get the start and end parts
            week_parts = week_range.split(' - ')
            if len(week_parts) != 2:
                continue
            
            start_part = week_parts[0].strip()
            end_part = week_parts[1].strip()
            
            # Extract years from both parts of WeekRange
            start_year_match = re.search(r',\s*(\d{4})$', start_part)
            end_year_match = re.search(r',\s*(\d{4})$', end_part)
            
            if not start_year_match or not end_year_match:
                continue
            
            start_year = int(start_year_match.group(1))
            end_year = int(end_year_match.group(1))
            
            # Check if this is a cross-year week (December to January)
            is_cross_year_week = (end_year > start_year)
            
            if is_cross_year_week:
                # Parse the current date
                current_date = pd.to_datetime(date_str, dayfirst=True, errors='coerce')
                if pd.isna(current_date):
                    continue
                
                # Extract month and day from current date
                current_month = current_date.month
                current_day = current_date.day
                current_year = current_date.year
                
                # Only fix December dates that have the wrong year
                if current_month == 12 and current_year == end_year:
                    # This is a December date that incorrectly has the next year
                    # Correct it to use the start year from WeekRange
                    corrected_date = current_date.replace(year=start_year)
                    corrected_date_str = corrected_date.strftime('%d %B %Y')
                    
                    # Update the DataFrame
                    df.at[idx, 'Date'] = corrected_date_str
                    
                    # Store example for output (limit to 5 per year)
                    year_key = start_year
                    if year_key not in correction_examples_by_year:
                        correction_examples_by_year[year_key] = []
                    
                    if len(correction_examples_by_year[year_key]) < 5:
                        correction_examples_by_year[year_key].append({
                            'index': idx,
                            'original_date': date_str,
                            'corrected_date': corrected_date_str,
                            'week_range': week_range,
                            'currency': row.get('Currency', 'N/A'),
                            'event': row.get('Event', 'N/A')[:50]  # Truncate long event names
                        })
        
        except Exception:
            continue
    
    # Print final output showing December fixes by year
    if correction_examples_by_year:
        print("\n" + "="*80)
        print("DECEMBER WEEK OVERLAP CORRECTION SUMMARY")
        print("="*80)
        
        for year in sorted(correction_examples_by_year.keys()):
            examples = correction_examples_by_year[year]
            print(f"\nYEAR {year} - {len(examples)} corrections shown:")
            print("-" * 60)
            
            for i, example in enumerate(examples, 1):
                print(f"Example {i}:")
                print(f"  Row Index: {example['index']}")
                print(f"  Currency/Event: {example['currency']} - {example['event']}")
                print(f"  Week Range: {example['week_range']}")
                print(f"  BEFORE: {example['original_date']}")
                print(f"  AFTER:  {example['corrected_date']}")
                print()
    
    return df

# ==== MAIN EXECUTION ====
def main():
    print("🚀 Starting CSV processing with broken row detection and fixing...")
    
    # ==== CLEAR OUTPUT FOLDER FIRST ====
    clear_output_folder()
    
    # ==== LOAD MAIN BATCH ====
    main_csv_files = list(main_batch_folder.glob("*.csv"))
    if not main_csv_files:
        raise FileNotFoundError(f"No CSV found in {main_batch_folder}")
    main_csv_path = main_csv_files[0]

    print(f"📁 Loading main batch: {main_csv_path.name}")
    
    # Fix CSV structure first
    merged_df = fix_csv_structure(main_csv_path)
    merged_df.columns = merged_df.columns.str.strip()
    
    # Ensure we have the right columns
    if len(merged_df.columns) != len(EXPECTED_COLUMNS):
        if len(merged_df.columns) < len(EXPECTED_COLUMNS):
            for i in range(len(merged_df.columns), len(EXPECTED_COLUMNS)):
                merged_df[EXPECTED_COLUMNS[i]] = ''
        merged_df.columns = EXPECTED_COLUMNS[:len(merged_df.columns)]
    
    # Optimize memory usage
    merged_df = optimize_dataframe(merged_df)
    
    # Detect and fix broken rows
    merged_df = detect_and_fix_broken_rows(merged_df)

    # ==== REMOVE DUPLICATES IN MAIN BATCH ====
    before = len(merged_df)
    merged_df.drop_duplicates(inplace=True)
    after = len(merged_df)
    if before != after:
        print(f"⚠️ Removed {before - after} duplicates from main batch.")

    # ==== MERGE ALL MONTHLY BATCHES ====
    monthly_csv_files = sorted([f for folder in monthly_batches_root.glob("* Batch") for f in folder.glob("*.csv")])
    latest_monthly_csv = None

    for csv_file in monthly_csv_files:
        print(f"📁 Processing monthly batch: {csv_file.name}")
        
        # Fix CSV structure first
        batch_df = fix_csv_structure(csv_file)
        batch_df.columns = batch_df.columns.str.strip()
        
        # Ensure columns match
        if len(batch_df.columns) != len(EXPECTED_COLUMNS):
            if len(batch_df.columns) < len(EXPECTED_COLUMNS):
                for i in range(len(batch_df.columns), len(EXPECTED_COLUMNS)):
                    batch_df[EXPECTED_COLUMNS[i]] = ''
            batch_df.columns = EXPECTED_COLUMNS[:len(batch_df.columns)]
        
        # Optimize memory usage
        batch_df = optimize_dataframe(batch_df)
        
        # Detect and fix broken rows in monthly batch
        batch_df = detect_and_fix_broken_rows(batch_df)
        
        # Align columns with merged_df
        batch_df = batch_df.reindex(columns=merged_df.columns, fill_value='')

        # Merge without duplicates
        combined = pd.concat([merged_df, batch_df], ignore_index=True)
        new_rows_mask = ~combined.duplicated(keep='first')
        new_rows_df = combined[new_rows_mask].iloc[len(merged_df):]

        if len(new_rows_df) > 0:
            print(f"✅ {len(new_rows_df)} new rows merged from {csv_file.name}")
            merged_df = pd.concat([merged_df, new_rows_df], ignore_index=True)
            
            # Force garbage collection after large operations
            gc.collect()
        else:
            print(f"⚠️ No new rows to merge from {csv_file.name}")

        latest_monthly_csv = csv_file

    # ==== APPLY DECEMBER BUG FIX ====
    print("🗓️ Applying December week overlap fix...")
    if 'Date' in merged_df.columns and 'WeekRange' in merged_df.columns:
        merged_df = fix_december_week_overlap(merged_df)
    else:
        print("❌ Cannot apply December fix: 'Date' or 'WeekRange' column missing")

    # ==== FINAL CHECK FOR ANY REMAINING BROKEN ROWS ====
    merged_df = detect_and_fix_broken_rows(merged_df)

    # ==== CONSTRUCT MERGED FILENAME ====
    main_stem = main_csv_path.stem
    if latest_monthly_csv:
        monthly_stem = latest_monthly_csv.stem
        monthly_first_part = monthly_stem.split("_to_")[0]
        main_second_part = "_to_".join(main_stem.split("_to_")[1:])
        merged_filename = f"{monthly_first_part}_to_{main_second_part}"
    else:
        merged_filename = main_stem

    # Remove any illegal Windows filename characters
    merged_filename = re.sub(r'[<>:"/\\|?*]', '', merged_filename)

    # Save CSV
    output_file = output_folder / f"{merged_filename}.csv"
    merged_df.to_csv(output_file, index=False)
    print(f"💾 Merged file saved: {output_file}")

    # ==== SUMMARY REPORT ====
    print(f"\n📊 PROCESSING SUMMARY")
    print(f"Total rows in final dataset: {len(merged_df)}")
    
    # Check for any remaining issues
    broken_dates = merged_df['Date'].apply(lambda x: is_broken_date(str(x))).sum()
    missing_weekranges = merged_df['WeekRange'].isna().sum() + (merged_df['WeekRange'] == '').sum()
    
    print(f"Remaining broken dates: {broken_dates}")
    print(f"Remaining missing WeekRanges: {missing_weekranges}")
    print("✅ Processing completed successfully!")

if __name__ == "__main__":
    main()