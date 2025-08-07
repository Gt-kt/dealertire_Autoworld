# /scripts/tirepick_daily.py
import pandas as pd
import io

def analyze_sales_data(file_stream, date_input):
    """
    Excel 파일 스트림에서 판매 데이터를 읽어와서 분석하고 결과를 DataFrame으로 반환합니다.
    
    Args:
        file_stream: 업로드된 Excel 파일의 in-memory stream.
        date_input (str): 'YYYYMMDD' 형식의 분석할 날짜.
        
    Returns:
        pandas.DataFrame: 분석 결과가 담긴 DataFrame.
    """
    product_type_to_filter = '타이어'

    # --- 파일 읽기 및 유효성 검사 ---
    try:
        df = pd.read_excel(file_stream)
    except Exception as e:
        raise ValueError(f"Excel 파일을 읽는 중 오류가 발생했습니다: {e}")

    required_cols = ['상품타입', '주문일', '주문수량', '주문채널', '주문번호']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"입력 파일에 필수 컬럼이 없습니다: '{col}'. Excel 파일의 열 이름을 확인해주세요.")

    # --- 데이터 필터링 및 처리 ---
    # '상품타입'으로 필터링
    df_filtered_product = df[df['상품타입'] == product_type_to_filter].copy()
    if df_filtered_product.empty:
        return pd.DataFrame() # 필터링 후 데이터가 없으면 빈 DataFrame 반환

    # '주문일'을 문자열로 변환하여 정확한 비교 보장
    df_filtered_product['주문일'] = df_filtered_product['주문일'].astype(str)

    # 입력된 날짜로 필터링
    df_date_filtered = df_filtered_product[df_filtered_product['주문일'] == date_input].copy()
    if df_date_filtered.empty:
        return pd.DataFrame()

    # '주문수량'을 숫자형으로 변환
    df_date_filtered['주문수량'] = pd.to_numeric(df_date_filtered['주문수량'], errors='coerce')

    # --- 피벗 테이블 생성 ---
    pivot_table = df_date_filtered.groupby('주문채널').agg(
        total_orders=('주문번호', 'nunique'),  # 고유 주문번호 개수
        total_quantity=('주문수량', 'sum')    # 주문수량 합계
    ).reset_index()

    return pivot_table
