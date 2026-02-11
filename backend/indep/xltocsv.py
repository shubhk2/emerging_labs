import pandas as pd
import os
import numpy as np
import re
import glob

def convert_period(col):
    """Convert period strings like 'Mar-22' to date format '2022-03-31'"""
    if pd.isna(col) or not isinstance(col, str):
        return col
    
    # Clean the column name - remove any non-alphanumeric characters except dash
    col = str(col).strip()
    
    # Check if it's already in date format
    if re.match(r'\d{4}-\d{2}-\d{2}', col):
        return col
    
    try:
        # Handle format like "Mar-22" or "Mar-2022"
        if '-' in col:
            month, year = col.split('-')
            # Add 20 prefix if year is just 2 digits
            if len(year) == 2:
                year = '20' + year
            # Convert month abbreviation to month number
            month_num = pd.to_datetime(month, format='%b').month
            # Create date string with last day of month
            month_last_day = pd.Period(f'{year}-{month_num}', freq='M').end_time.strftime('%Y-%m-%d')
            return month_last_day
        else:
            # Try direct conversion for other formats
            return pd.to_datetime(col).strftime('%Y-%m-%d')
    except Exception as e:
        # If conversion fails, return the original but print warning
        print(f"Warning: Could not convert '{col}' to date. Error: {str(e)}")
        return col

def process_excel_to_long_format(file_path, sheet_name="Data Sheet", section="Profit & Loss", start_row=14, end_row=None):
    """
    Process a section of Excel file into long format
    
    Parameters:
    - file_path: Path to Excel file
    - sheet_name: Name of the sheet containing data
    - section: Section name (e.g., "Profit & Loss", "Balance Sheet", "Cashflow")
    - start_row: Row index where section header is located (0-based)
    - end_row: End row index (exclusive) or None to process until blank row
    """
    company_name = os.path.splitext(os.path.basename(file_path))[0]
    print(f"Processing {company_name} - {section} section...")
    
    # Read the Excel file
    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    
    # Print some info to debug
    print(f"Excel shape: {df_raw.shape}")
    
    # Find the actual header row (which might be start_row or start_row-1 depending on format)
    header_row = df_raw.iloc[start_row].tolist()
    
    # Debug: Print the header row to see what we're getting
    print(f"Header row for {section}: {header_row}")
    
    if end_row is None:
        # Find end by looking for consecutive empty rows
        data_slice = df_raw.iloc[(start_row + 1):].copy()
        # Find first row where all cells are empty or NaN
        for i, row in data_slice.iterrows():
            if row.isna().all() or (row.astype(str).str.strip() == '').all():
                end_row = i
                break
        
        # For PL section, if we can't find clear end, use a fixed range to ensure we don't go too far
        if section == "Profit & Loss" and (end_row is None or end_row > start_row + 25):
            end_row = start_row + 25  # Typical PL section is around 15-20 rows
    
    data_rows = df_raw.iloc[(start_row + 1):end_row].copy()
    
    # Set columns to header row
    data_rows.columns = header_row
    
    # Rename first column to Parameter
    data_rows.rename(columns={data_rows.columns[0]: "Parameter"}, inplace=True)
    
    # Clean up the dataframe
    data_rows.dropna(how='all', inplace=True)
    data_rows.dropna(axis=1, how='all', inplace=True)
    
    # Clean parameter names
    data_rows["Parameter"] = data_rows["Parameter"].astype(str).str.strip()
    
    # Convert header to proper date columns
    new_cols = []
    for col in data_rows.columns:
        if col == "Parameter":
            new_cols.append(col)
        else:
            # Handle NaN/None headers by replacing with placeholder
            if pd.isna(col):
                new_col = "Unknown_Date"
                print(f"Warning: Found NaN column header in {section}")
            else:
                new_col = convert_period(col)
                print(f"Converted '{col}' to '{new_col}'")
            new_cols.append(new_col)
    
    # Check if we have proper date columns (for debugging)
    print(f"New column headers: {new_cols}")
    date_cols = [col for col in new_cols if col != "Parameter" and col != "Unknown_Date"]
    print(f"Found {len(date_cols)} date columns for {section}")
    
    data_rows.columns = new_cols
    
    # Drop row where Parameter == "Report Date"
    data_rows = data_rows[data_rows["Parameter"].str.lower() != "report date"]
    
    # Replace empty strings with NaN for proper handling
    data_rows = data_rows.replace('', np.nan)
    
    # Melt dataframe to long format
    df_melted = data_rows.melt(id_vars=["Parameter"], var_name="report_date", value_name="value")
    
    # Remove rows where value is NaN
    df_melted = df_melted.dropna(subset=['value'])
    
    # Add metadata
    df_melted["section"] = section
    df_melted["company_name"] = company_name
    
    # Debug: Check if we have any empty report dates
    empty_dates = df_melted["report_date"].isna().sum()
    if empty_dates > 0:
        print(f"Warning: Found {empty_dates} rows with empty report dates in {section}")
    
    return df_melted

def process_sections_from_file(file_path, output_dir="processed_data"):
    """
    Process all sections (P&L, Balance Sheet, Cashflow) from an Excel file
    
    Parameters:
    - file_path: Path to Excel file
    - output_dir: Directory to save CSV output files
    
    Returns:
    - List of paths to created CSV files
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    company_name = os.path.splitext(os.path.basename(file_path))[0]
    print(f"Processing all sections for {company_name}...")
    
    outputs = []
    
    # Try to detect where dates are in the Excel file by checking header rows
    df_peek = pd.read_excel(file_path, sheet_name="Data Sheet", nrows=20, header=None)
    
    # Look for date headers in rows 13, 14, and 15 (common positions for P&L section)
    # pl_start_row = None
    # for test_row in [13, 14, 15]:
    #     header_candidate = df_peek.iloc[test_row].tolist()
    #     # Check if this row has potential date values (e.g., contains '-' which is common in date formats)
    #     date_like_cells = [str(cell) for cell in header_candidate[1:] if isinstance(cell, str) and '-' in str(cell)]
    #     if len(date_like_cells) > 0:
    #         pl_start_row = test_row
    #         print(f"Found potential P&L header row at index {pl_start_row} with date cells: {date_like_cells}")
    #         break
    #
    # # Use detected row or fall back to default (14)
    # if pl_start_row is None:
    #     pl_start_row = 14
    #     print(f"Using default P&L start row of {pl_start_row}")
    
    # Process Profit & Loss section with detected row and explicit end row
    df_pl = process_excel_to_long_format(file_path, section="Profit & Loss", start_row=15, end_row=31)
    pl_output_path = os.path.join(output_dir, f"{company_name}_PL.csv")
    df_pl.to_csv(pl_output_path, index=False)
    outputs.append(pl_output_path)
    print(f"P&L data saved to {pl_output_path} with {len(df_pl)} rows")
    
    # Process Balance Sheet section (approximately rows 55-72)
    df_bs = process_excel_to_long_format(file_path, section="Balance Sheet", start_row=55, end_row=72)
    bs_output_path = os.path.join(output_dir, f"{company_name}_BS.csv")
    df_bs.to_csv(bs_output_path, index=False)
    outputs.append(bs_output_path)
    print(f"Balance Sheet data saved to {bs_output_path} with {len(df_bs)} rows")
    
    # Process Cashflow section (approximately rows 80-85)
    df_cf = process_excel_to_long_format(file_path, section="Cashflow", start_row=80, end_row=85)
    cf_output_path = os.path.join(output_dir, f"{company_name}_CF.csv")
    df_cf.to_csv(cf_output_path, index=False)
    outputs.append(cf_output_path)
    print(f"Cashflow data saved to {cf_output_path} with {len(df_cf)} rows")
    
    return outputs

def batch_process_files(directory_path, pattern="*.xlsx", output_dir="processed_data"):
    """
    Process all Excel files in a directory
    
    Parameters:
    - directory_path: Directory containing Excel files
    - pattern: File pattern to match
    - output_dir: Directory to save output CSV files
    """
    file_paths = glob.glob(os.path.join(directory_path, pattern))
    print(f"Found {len(file_paths)} files to process")
    
    all_outputs = []
    for file_path in file_paths:
        try:
            outputs = process_sections_from_file(file_path, output_dir)
            all_outputs.extend(outputs)
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
    
    print(f"Batch processing complete. Created {len(all_outputs)} CSV files.")
    return all_outputs

# If running as main script
if __name__ == "__main__":
    # Example usage to process a single file
    # process_sections_from_file("../NIFTY-100/Adani Ports.xlsx")
    
    # Example usage to process all files in a directory
    batch_process_files("../NIFTY-100")
