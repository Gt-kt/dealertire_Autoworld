"""Quick delivery data processor.

This module merges logistics and admin order datasets to produce
quick-delivery summaries. It returns an Excel workbook in memory
containing the original admin data, the fully processed dataset,
filtered quick-delivery records, and a pivot table summarising
costs by region.
"""

from __future__ import annotations

import io
import re
import pandas as pd


def _extract_address_parts(address: str | None) -> tuple[str, str, str]:
    """Split a Korean address into province, district and road name.

    Parameters
    ----------
    address: str | None
        Full address string possibly containing a postal code in brackets.
    """

    if pd.isna(address):
        return "", "", ""
    address_no_zip = re.sub(r"\[\d+\]\s*", "", str(address)).strip()
    parts = address_no_zip.split()
    province = parts[0] if len(parts) > 0 else ""
    district = parts[1] if len(parts) > 1 else ""
    road = parts[2] if len(parts) > 2 else ""
    return province, district, road



def process_files(logistics_file, admin_file) -> io.BytesIO:
    """Process uploaded logistics and admin Excel files.

    Parameters
    ----------
    logistics_file, admin_file: werkzeug.datastructures.FileStorage
        Uploaded Excel files.

    Returns
    -------
    io.BytesIO
        In-memory Excel workbook with multiple summary sheets.
    """

    # --- Load datasets ---
    logistics_df = pd.read_excel(logistics_file, dtype={"자체 관리코드": str})
    admin_df = pd.read_excel(admin_file, dtype={"주문번호": str})
    original_admin_df = admin_df.copy()

    if "자체 관리코드" in logistics_df.columns:
        logistics_df["자체 관리코드"] = (
            logistics_df["자체 관리코드"].str.replace(r"\.0$", "", regex=True).str.strip()
        )
        logistics_df.dropna(subset=["자체 관리코드"], inplace=True)

    if "주문번호" in admin_df.columns:
        admin_df["주문번호"] = (
            admin_df["주문번호"].str.replace(r"\.0$", "", regex=True).str.strip()
        )
        admin_df.dropna(subset=["주문번호"], inplace=True)

    # --- Merge and enrich ---
    logistics_subset = logistics_df[["자체 관리코드", "합계비용"]].copy()
    merged_df = pd.merge(
        admin_df,
        logistics_subset,
        left_on="주문번호",
        right_on="자체 관리코드",
        how="left",
    )

    matched_mask = merged_df["자체 관리코드"].notna()
    merged_df.loc[matched_mask, "배송비"] = merged_df.loc[matched_mask, "합계비용"]
    merged_df.rename(columns={"합계비용": "퀵비용"}, inplace=True)
    merged_df.drop(columns=["자체 관리코드"], inplace=True)

    address_parts = merged_df["배송주소"].apply(_extract_address_parts)
    merged_df[["시/도", "시/군/구", "도로명"]] = pd.DataFrame(
        address_parts.tolist(), index=merged_df.index
    )

    processed_admin_df = merged_df.copy()

    # --- Filter quick deliveries ---
    quick_df = processed_admin_df[processed_admin_df["배송방법"] == "퀵배송"].copy()

    # --- Pivot summarisation ---
    pivot_df = pd.DataFrame(
        columns=[
            "시/도",
            "지역구",
            "주문 건수 (Order Count)",
            "총 퀵비용 (Total Quick Fee)",
            "평균 퀵비용 (Avg. Quick Fee)",
        ]
    )

    pivot_base = quick_df.dropna(subset=["퀵비용"])
    if not pivot_base.empty:
        pivot_df = (
            pivot_base.pivot_table(
                index=["시/도", "시/군/구"],
                values=["주문번호", "퀵비용"],
                aggfunc={"주문번호": "count", "퀵비용": ["sum", "mean"]},
            )
            .reset_index()
        )
        pivot_df.columns = [
            "시/도",
            "지역구",
            "주문 건수 (Order Count)",
            "평균 퀵비용 (Avg. Quick Fee)",
            "총 퀵비용 (Total Quick Fee)",
        ]
        pivot_df = pivot_df[
            [
                "시/도",
                "지역구",
                "주문 건수 (Order Count)",
                "총 퀵비용 (Total Quick Fee)",
                "평균 퀵비용 (Avg. Quick Fee)",
            ]
        ]
        total_orders = pivot_df["주문 건수 (Order Count)"].sum()
        total_fee = pivot_df["총 퀵비용 (Total Quick Fee)"].sum()
        overall_avg_fee = pivot_base["퀵비용"].mean()
        total_row = pd.DataFrame(
            {
                "시/도": ["합계 (Total)"],
                "지역구": [""],
                "주문 건수 (Order Count)": [total_orders],
                "총 퀵비용 (Total Quick Fee)": [total_fee],
                "평균 퀵비용 (Avg. Quick Fee)": [overall_avg_fee],
            }
        )
        pivot_df = pd.concat([pivot_df, total_row], ignore_index=True)
        pivot_df["총 퀵비용 (Total Quick Fee)"] = (
            pivot_df["총 퀵비용 (Total Quick Fee)"].fillna(0).astype(int)
        )
        pivot_df["평균 퀵비용 (Avg. Quick Fee)"] = (
            pivot_df["평균 퀵비용 (Avg. Quick Fee)"].fillna(0).astype(int)
        )

    # --- Export to Excel in memory ---
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        original_admin_df.to_excel(
            writer, sheet_name="주문관리_원본 (Original_Admin)", index=False
        )
        processed_admin_df.to_excel(
            writer, sheet_name="전체 데이터 (Full_Data_Modified)", index=False
        )
        if not quick_df.empty:
            quick_df.to_excel(
                writer, sheet_name="퀵배송_데이터 (Quick_Data)", index=False
            )
        if not pivot_df.empty:
            pivot_df.to_excel(
                writer, sheet_name="지역구별_요약 (District_Summary)", index=False
            )

    output.seek(0)
    return output
