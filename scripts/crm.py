# /scripts/crm.py
"""CRM data extraction utilities.

This module reads two datasets and extracts a list of unique contacts
matching user IDs from dataset 1 with consenting customers from dataset 2.
It is designed to be used by the Flask application where two files are
uploaded and an Excel workbook is returned in memory.
"""

import io
import re
import pandas as pd


def clean_tirepick_id(text: str) -> str | None:
    """Return the numeric portion of a *tirepick* identifier.

    Parameters
    ----------
    text: str
        Raw identifier text containing digits.
    """

    text = str(text)
    match = re.search(r"(\d+)", text)
    return match.group(1) if match else None


def format_phone_number(num) -> str:
    """Normalize phone numbers so they always start with ``010``.

    Empty or NaN values return an empty string. Non‑digit characters are
    removed. Numbers beginning with ``1`` and ten digits long are prefixed
    with ``0``.
    """

    if pd.isna(num):
        return ""
    s_num = str(num).replace(".0", "").strip()
    s_num = re.sub(r"\D", "", s_num)
    if len(s_num) == 10 and s_num.startswith("1"):
        return "0" + s_num
    return s_num


def find_user_id_column(df: pd.DataFrame) -> str | None:
    """Locate a column containing a ``user_id`` field.

    If no explicit ``user_id`` column exists, fall back to the second column
    (or the only column) in the DataFrame.
    """

    for col in df.columns:
        cleaned_col = str(col).replace('"', "").strip().lower()
        if "user_id" in cleaned_col:
            return col
    if len(df.columns) >= 2:
        return df.columns[1]
    if len(df.columns) == 1:
        return df.columns[0]
    return None


def try_read_csv(file_bytes: bytes, separator: str) -> pd.DataFrame | None:
    """Attempt to read CSV/TSV data using multiple encodings.

    Parameters
    ----------
    file_bytes: bytes
        Raw file content.
    separator: str
        Column separator, e.g. `,` or ``\t``.
    """

    encodings_to_try = ["utf-8", "cp949", "euc-kr", "latin-1"]
    for enc in encodings_to_try:
        try:
            return pd.read_csv(
                io.BytesIO(file_bytes),
                sep=separator,
                quotechar='"',
                on_bad_lines="warn",
                engine="python",
                encoding=enc,
                dtype={"고객전화번호": str},
            )
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue
    return None


def process_files(file1, file2) -> io.BytesIO:
    """Process two uploaded files and return an Excel workbook in memory.

    Parameters
    ----------
    file1, file2: werkzeug.datastructures.FileStorage
        Uploaded files representing the two datasets.

    Returns
    -------
    io.BytesIO
        In-memory Excel file containing the merged contacts list.
    """

    # Read raw bytes once so we can retry with different loaders.
    file1_bytes = file1.read()
    file2_bytes = file2.read()

    # --- Dataset 1 ---
    df1 = None
    try:
        df1 = pd.read_excel(io.BytesIO(file1_bytes), usecols=[1], header=None)
        df1.columns = ["user_id_raw"]
    except Exception:
        df1 = try_read_csv(file1_bytes, ",")
        if df1 is None:
            df1 = try_read_csv(file1_bytes, "\t")
        if df1 is None:
            raise ValueError("Dataset 1 could not be read as Excel, CSV, or TSV.")

    user_id_col = find_user_id_column(df1)
    if not user_id_col:
        raise ValueError("Could not determine the 'user_id' column in Dataset 1.")
    if "user_id_raw" not in df1.columns:
        df1 = df1[[user_id_col]].rename(columns={user_id_col: "user_id_raw"})

    df1["user_id"] = df1["user_id_raw"].apply(clean_tirepick_id)
    df1_ids = df1.dropna(subset=["user_id"])[["user_id"]].drop_duplicates()
    if df1_ids.empty:
        raise ValueError("No valid 'tirepick' IDs found in Dataset 1.")

    # --- Dataset 2 ---
    df2 = None
    try:
        df2 = pd.read_excel(io.BytesIO(file2_bytes), dtype={"고객전화번호": str})
    except Exception:
        df2 = try_read_csv(file2_bytes, ",")
        if df2 is None:
            df2 = try_read_csv(file2_bytes, "\t")
        if df2 is None:
            raise ValueError("Dataset 2 could not be read as Excel, CSV, or TSV.")

    required_cols = ["고객id", "푸시수신동의", "이메일", "고객전화번호"]
    if not all(col in df2.columns for col in required_cols):
        raise ValueError(
            f"Dataset 2 missing required columns. Needs: {required_cols}"
        )

    df2["고객전화번호"] = df2["고객전화번호"].apply(format_phone_number)
    df2_filtered = df2[df2["푸시수신동의"] == "O"].copy()
    df2_filtered["고객id"] = df2_filtered["고객id"].astype(str)

    # --- Merge and extract ---
    merged_df = pd.merge(
        df1_ids, df2_filtered, left_on="user_id", right_on="고객id", how="inner"
    )
    result_df = merged_df[["이메일", "고객전화번호"]].copy()
    result_df["이메일"] = result_df["이메일"].astype(str).str.strip().str.lower()
    result_df.drop_duplicates(subset=["고객전화번호"], keep="first", inplace=True)
    result_df.rename(
        columns={"이메일": "식별자", "고객전화번호": "수신자번호"}, inplace=True
    )

    output = io.BytesIO()
    result_df.to_excel(output, index=False)
    output.seek(0)
    return output

