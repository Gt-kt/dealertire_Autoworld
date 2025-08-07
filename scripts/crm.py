# /scripts/crm.py
import pandas as pd
import re
import io

# --- Helper Functions (Copied from original script) ---

def clean_tirepick_id(text):
    """Extracts digits from 'tirepick' strings."""
    text = str(text)
    match = re.search(r'(\d+)', text)
    return match.group(1) if match else None

def format_phone_number(num):
    """Formats phone numbers to ensure they start with 010."""
    if pd.isna(num):
        return ""
    s_num = str(num).replace('.0', '').strip()
    s_num = re.sub(r'\D', '', s_num)
    if len(s_num) == 10 and s_num.startswith('1'):
        return '0' + s_num
    return s_num

def find_user_id_column(df):
    """Tries to find the user_id column in a DataFrame."""
    for col in df.columns:
        cleaned_col = str(col).replace('"', '').strip().lower()
        if 'user_id' in cleaned_col:
            return col
    if len(df.columns) >= 2: return df.columns[1]
    if len(df.columns) >= 1: return df.columns[0]
    return None

def try_read_csv(filepath, separator):
    """Attempts to read a CSV/TSV file with multiple encodings."""
    encodings_to_try = ['utf-8', 'cp949', 'euc-kr', 'latin-1']
    for enc in encodings_to_try:
        try:
            # Reset stream position for each attempt
            filepath.seek(0)
            df = pd.read_csv(filepath, sep=separator, quotechar='"', on_bad_lines='warn',
                             engine='python', encoding=enc, dtype={'고객전화번호': str})
            return df
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue
    return None

# --- Main Processing Logic ---

def process_files(file1_stream, file2_stream):
    """
    Processes the two datasets from in-memory streams to find matches,
    extracts data, and returns the result as an in-memory Excel file.
    """
    # --- Process Dataset 1 (Amplitude) ---
    df1 = None
    try:
        # Try reading as Excel first
        df1 = pd.read_excel(file1_stream, usecols=[1], header=None)
        df1.columns = ['user_id_raw']
    except Exception:
        # If Excel fails, try as CSV/TSV
        file1_stream.seek(0) # Reset stream for reading again
        df1 = try_read_csv(file1_stream, ',')
        if df1 is None:
            file1_stream.seek(0)
            df1 = try_read_csv(file1_stream, '\t')
    
    if df1 is None:
        raise ValueError("Could not read Dataset 1. Please check if it is a valid Excel, CSV, or TSV file.")

    user_id_col_name = find_user_id_column(df1)
    if not user_id_col_name:
        raise ValueError("Could not determine the 'user_id' column in Dataset 1.")
    
    # Ensure the column is named correctly for processing
    if 'user_id_raw' not in df1.columns:
        df1 = df1[[user_id_col_name]].rename(columns={user_id_col_name: 'user_id_raw'})
        
    df1['user_id'] = df1['user_id_raw'].apply(clean_tirepick_id)
    df1_ids = df1.dropna(subset=['user_id'])[['user_id']].drop_duplicates()
    
    if df1_ids.empty:
        raise ValueError("No valid 'tirepick' IDs could be extracted from Dataset 1.")

    # --- Process Dataset 2 (Customer) ---
    df2 = None
    try:
        df2 = pd.read_excel(file2_stream, dtype={'고객전화번호': str})
    except Exception:
        file2_stream.seek(0)
        df2 = try_read_csv(file2_stream, ',')
        if df2 is None:
            file2_stream.seek(0)
            df2 = try_read_csv(file2_stream, '\t')

    if df2 is None:
        raise ValueError("Could not read Dataset 2. Please check if it is a valid Excel, CSV, or TSV file.")

    required_cols = ['고객id', '푸시수신동의', '이메일', '고객전화번호']
    if not all(col in df2.columns for col in required_cols):
        raise ValueError(f"Dataset 2 is missing one or more required columns. It needs: {required_cols}")

    df2['고객전화번호'] = df2['고객전화번호'].apply(format_phone_number)
    df2_filtered = df2[df2['푸시수신동의'] == 'O'].copy()
    df2_filtered['고객id'] = df2_filtered['고객id'].astype(str)

    # --- Merge, Extract, and Save ---
    merged_df = pd.merge(df1_ids, df2_filtered, left_on='user_id', right_on='고객id', how='inner')
    result_df = merged_df[['이메일', '고객전화번호']].drop_duplicates()
    
    # Rename columns for the final output
    result_df.rename(columns={'이메일': '식별자', '고객전화번호': '수신자번호'}, inplace=True)

    # Save result to an in-memory buffer
    output_buffer = io.BytesIO()
    result_df.to_excel(output_buffer, index=False)
    output_buffer.seek(0)
    
    return output_buffer
