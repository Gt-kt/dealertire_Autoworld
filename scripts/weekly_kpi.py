# /scripts/weekly_kpi.py
import pandas as pd
import io

def process_file(input_file):
    """
    Processes the uploaded Excel file in memory.
    
    Args:
        input_file: A file-like object (from the web upload).
        
    Returns:
        An in-memory BytesIO object containing the processed Excel data.
    """
    try:
        # --- Read the uploaded file from the memory stream ---
        df = pd.read_excel(input_file)

        # --- Columns to delete ---
        columns_to_delete = [
            '년도', '월', '주', '년월', '기획전', '상품정보', '배송사', '송장번호',
            '공급가', '부가세', '최초결제금액', '환불금액', '취소금액', '미수금액',
            '결제번호', '계좌번호', '요청사항', '거래처유형', '멤버십', '멤버십가입일',
            '타임세일할인', '준비중시간', '배송일시', '배송완료일시', '구매확정일시',
            '수령확인시간', '취소요청시간', '취소시간', '판매가유형', '도서산간'
        ]
        
        # --- Delete the specified columns ---
        existing_columns_to_drop = [col for col in columns_to_delete if col in df.columns]
        df.drop(columns=existing_columns_to_drop, inplace=True)

        # --- Format '주문일자' column to YYYY-MM-DD ---
        if '주문일자' in df.columns:
            df['주문일자'] = pd.to_datetime(df['주문일자'], errors='coerce').dt.strftime('%Y-%m-%d')

        # --- In '상태' column, change '취소' to '단순취소(제외)' ---
        if '상태' in df.columns:
            df.loc[df['상태'] == '취소', '상태'] = '단순취소(제외)'

        # --- Ensure '주문번호' is treated as text ---
        if '주문번호' in df.columns:
            df['주문번호'] = df['주문번호'].astype(str)

        # --- Save the modified DataFrame to an in-memory buffer ---
        output_buffer = io.BytesIO()
        df.to_excel(output_buffer, index=False, sheet_name="Modified_Data")
        output_buffer.seek(0) # Rewind the buffer to the beginning

        return output_buffer

    except Exception as e:
        print(f"An error occurred: {e}")
        return None