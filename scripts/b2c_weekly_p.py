# /scripts/b2c_weekly_p.py
import pandas as pd
from datetime import date
import calendar
import io

def process_file(input_stream):
    """
    Reads an Excel file stream, performs historical and predictive analysis,
    and returns a new Excel file with the results in memory.
    """
    try:
        df = pd.read_excel(input_stream, engine='openpyxl')

        # --- ADDED: Comprehensive column validation ---
        required_cols = [
            '상품타입', '브랜드', '주문채널', '주문상품', '주문수량', 
            '상품주문금액', '실결제금액', '장착비', '주문일', '주문번호', '고객id'
        ]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            # Raise a clear error if any columns are missing
            raise ValueError(f"입력 파일에 필수 컬럼이 없습니다: {', '.join(missing_cols)}. 열 이름을 확인해주세요.")
        # --- END ADDED ---

        # --- 1. Data Cleaning and Preparation ---
        for col in ['상품타입', '브랜드', '주문채널', '주문상품']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        numeric_cols = ['주문수량', '상품주문금액', '실결제금액', '장착비']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        df['주문일'] = pd.to_datetime(df['주문일'], format='%Y%m%d', errors='coerce')
        df.dropna(subset=['주문일'], inplace=True)
        
        # Define South Korean public holidays for 2025
        holidays_2025 = [
            date(2025, 1, 1), date(2025, 1, 28), date(2025, 1, 29), date(2025, 1, 30),
            date(2025, 3, 1), date(2025, 5, 5), date(2025, 5, 6), date(2025, 6, 6),
            date(2025, 8, 15), date(2025, 10, 3), date(2025, 10, 6), date(2025, 10, 7),
            date(2025, 10, 8), date(2025, 10, 9), date(2025, 12, 25)
        ]
        
        def get_day_type(d):
            d_date = d.date()
            return 'Weekend' if d_date in holidays_2025 or d.weekday() >= 5 else 'Weekday'
        
        df['Day_Type'] = df['주문일'].apply(get_day_type)

    except Exception as e:
        raise ValueError(f"File Read/Clean Error: {e}")

    # --- 2. Perform All Historical Analysis Tasks ---
    df_tire = df[(df['상품타입'] == '타이어') & (df['브랜드'] != '기타')].copy()
    result1_base = df_tire.groupby('주문채널').agg(주문수량=('주문수량', 'sum'), 상품주문금액=('상품주문금액', 'sum'), 실결제금액=('실결제금액', 'sum')).reset_index()
    sum1 = result1_base.sum(numeric_only=True)
    result1 = pd.concat([result1_base, pd.DataFrame([{'주문채널': '합계', **sum1}])], ignore_index=True) if not result1_base.empty else result1_base
    
    product_types_2 = ['배터리', '세차권', '와이퍼']
    df_etc = df[df['상품타입'].isin(product_types_2)].copy()
    result2_base = df_etc.groupby('상품타입').agg(주문수량=('주문수량', 'sum'), 상품주문금액=('상품주문금액', 'sum'), 장착비=('장착비', 'sum'), 실결제금액=('실결제금액', 'sum')).reset_index()
    sum2 = result2_base.sum(numeric_only=True)
    result2 = pd.concat([result2_base, pd.DataFrame([{'상품타입': '합계', **sum2}])], ignore_index=True) if not result2_base.empty else result2_base

    df_oil_filtered = df[(df['상품타입'] == '엔진오일') & (df['주문상품'].str.contains('오일필터', na=False))].copy()
    result3 = pd.DataFrame({'개수(count)': [df_oil_filtered['주문번호'].nunique()], '주문수량': [df_oil_filtered['주문수량'].sum()], '상품주문금액': [df_oil_filtered['상품주문금액'].sum()], '실결제금액': [df_oil_filtered['실결제금액'].sum()]})

    val_1 = sum1.get('실결제금액', 0) - sum1.get('상품주문금액', 0)
    val_2 = sum2.get('실결제금액', 0) - sum2.get('상품주문금액', 0) - sum2.get('장착비', 0)
    val_3 = df_oil_filtered['주문수량'].sum() * 25000
    result4 = pd.DataFrame({'구분': ['타이어 용역가치 (1)', '기타상품 용역가치 (2)', '엔진오일 용역가치 (3)', '총 용역가치'], '금액': [val_1, val_2, val_3, val_1 + val_2 + val_3]}); result4['금액'] = result4['금액'].apply(lambda x: f"{x:,.0f}")
    
    result5_base = df_tire.groupby('주문채널')['고객id'].nunique().reset_index().rename(columns={'고객id': '고유고객수'})
    sum5 = result5_base.sum(numeric_only=True)
    result5 = pd.concat([result5_base, pd.DataFrame([{'주문채널': '합계', **sum5}])], ignore_index=True) if not result5_base.empty else result5_base

    result6_base = df_tire[df_tire['브랜드'] != '기타'].groupby('브랜드').agg(주문수량=('주문수량', 'sum'), 상품주문금액=('상품주문금액', 'sum'), 실결제금액=('실결제금액', 'sum')).reset_index()
    sum6 = result6_base.sum(numeric_only=True)
    result6 = pd.concat([result6_base, pd.DataFrame([{'브랜드': '합계', **sum6}])], ignore_index=True) if not result6_base.empty else result6_base

    df_alignment = df[df['상품타입'] == '휠얼라인먼트'].copy()
    alignment_quantity = df_alignment['주문수량'].sum()
    result7 = pd.DataFrame({'상품타입': ['휠얼라인먼트'], '주문수량 합계': [alignment_quantity], '계산결과 (수량*3000)': [f"{alignment_quantity * 3000:,.0f}"]})
    
    historical_results = [(result1, "1. 타이어 판매 현황 (by 주문채널)"), (result2, "2. 기타 상품 판매 현황"), (result3, "3. 엔진오일(오일필터) 주문 내역 (집계)"), (result4, "4. 용역 가치 분석"), (result5, "5. 타이어 구매 고객 분석"), (result6, "6. 타이어 판매 현황 (by 브랜드)"), (result7, "7. 휠얼라이먼트 분석")]

    # --- 3. Perform Detailed Prediction Analysis ---
    if df['주문일'].empty:
        latest_date_in_data = date.today()
        remaining_weekdays, remaining_weekends = 0, 0
    else:
        latest_date_in_data = df['주문일'].max().date()
        _, num_days_in_month = calendar.monthrange(latest_date_in_data.year, latest_date_in_data.month)
        start_day_for_prediction = latest_date_in_data.day + 1
        
        remaining_weekdays = sum(1 for i in range(start_day_for_prediction, num_days_in_month + 1) if date(latest_date_in_data.year, latest_date_in_data.month, i).weekday() < 5 and date(latest_date_in_data.year, latest_date_in_data.month, i) not in holidays_2025)
        remaining_weekends = sum(1 for i in range(start_day_for_prediction, num_days_in_month + 1) if date(latest_date_in_data.year, latest_date_in_data.month, i).weekday() >= 5 or date(latest_date_in_data.year, latest_date_in_data.month, i) in holidays_2025)

    prediction_blocks = []
    def generate_grouped_prediction(df_filtered, group_by_col, agg_dict, title, numeric_cols):
        df_weekday = df_filtered[df_filtered['Day_Type'] == 'Weekday']; weekdays_in_data = df_weekday['주문일'].nunique(); weekday_pred_df = pd.DataFrame()
        if weekdays_in_data > 0:
            hist_wd = df_weekday.groupby(group_by_col).agg(agg_dict)
            if not hist_wd.empty:
                avg_wd = hist_wd[numeric_cols].div(weekdays_in_data); pred_wd = avg_wd.multiply(remaining_weekdays); total_wd = hist_wd[numeric_cols].add(pred_wd); total_wd.loc['합계'] = total_wd.sum(); weekday_pred_df = total_wd
        
        df_weekend = df_filtered[df_filtered['Day_Type'] == 'Weekend']; weekends_in_data = df_weekend['주문일'].nunique(); weekend_pred_df = pd.DataFrame()
        if weekends_in_data > 0:
            hist_we = df_weekend.groupby(group_by_col).agg(agg_dict)
            if not hist_we.empty:
                avg_we = hist_we[numeric_cols].div(weekends_in_data); pred_we = avg_we.multiply(remaining_weekends); total_we = hist_we[numeric_cols].add(pred_we); total_we.loc['합계'] = total_we.sum(); weekend_pred_df = total_we
        prediction_blocks.append({'title': title, 'weekday_df': weekday_pred_df, 'weekend_df': weekend_pred_df, 'wd_count': weekdays_in_data, 'we_count': weekends_in_data})

    def generate_scalar_prediction(df_filtered, agg_dict, title, multipliers={}):
        df_weekday = df_filtered[df_filtered['Day_Type'] == 'Weekday']; weekdays_in_data = df_weekday['주문일'].nunique(); weekday_pred_df = pd.DataFrame()
        if weekdays_in_data > 0:
            hist = df_weekday.agg(agg_dict); avg = hist / weekdays_in_data; pred = avg * remaining_weekdays; total = hist + pred
            for col, mult in multipliers.items(): total[col] *= mult
            weekday_pred_df = pd.DataFrame(total).T
        
        df_weekend = df_filtered[df_filtered['Day_Type'] == 'Weekend']; weekends_in_data = df_weekend['주문일'].nunique(); weekend_pred_df = pd.DataFrame()
        if weekends_in_data > 0:
            hist = df_weekend.agg(agg_dict); avg = hist / weekends_in_data; pred = avg * remaining_weekends; total = hist + pred
            for col, mult in multipliers.items(): total[col] *= mult
            weekend_pred_df = pd.DataFrame(total).T
        prediction_blocks.append({'title': title, 'weekday_df': weekday_pred_df, 'weekend_df': weekend_pred_df, 'wd_count': weekdays_in_data, 'we_count': weekends_in_data})

    generate_grouped_prediction(df_tire, '주문채널', {'주문수량': 'sum', '상품주문금액': 'sum', '실결제금액': 'sum'}, "1. 타이어 판매 현황 (by 주문채널) - 예측", ['주문수량', '상품주문금액', '실결제금액'])
    generate_grouped_prediction(df_etc, '상품타입', {'주문수량': 'sum', '상품주문금액': 'sum', '장착비': 'sum', '실결제금액': 'sum'}, "2. 기타 상품 판매 현황 - 예측", ['주문수량', '상품주문금액', '장착비', '실결제금액'])
    generate_scalar_prediction(df_oil_filtered, {'주문수량': 'sum', '상품주문금액':'sum', '실결제금액':'sum'}, '3. 엔진오일(오일필터) 주문 내역 (집계) - 예측')
    generate_grouped_prediction(df_tire, '주문채널', {'고객id': 'nunique'}, "5. 타이어 구매 고객 분석 - 예측", ['고객id'])
    generate_grouped_prediction(df_tire[df_tire['브랜드'] != '기타'], '브랜드', {'주문수량': 'sum', '상품주문금액': 'sum', '실결제금액': 'sum'}, "6. 타이어 판매 현황 (by 브랜드) - 예측", ['주문수량', '상품주문금액', '실결제금액'])
    generate_scalar_prediction(df_alignment, {'주문수량': 'sum'}, "7. 휠얼라이먼트 분석 - 예측", multipliers={'주문수량': 3000})

    # --- 4. Save All Results to a new Excel file in memory ---
    output_buffer = io.BytesIO()
    with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
        # Write Historical Analysis Sheet
        current_row = 0
        for df_to_write, title in historical_results:
            if not df_to_write.empty:
                pd.DataFrame([title]).to_excel(writer, sheet_name='Analysis_Results', startrow=current_row, header=False, index=False)
                df_to_write.to_excel(writer, sheet_name='Analysis_Results', startrow=current_row + 2, index=False)
                current_row += len(df_to_write) + 5
        
        # Write Prediction Analysis Sheet
        pred_row = 0
        for block in prediction_blocks:
            pd.DataFrame([block['title']]).to_excel(writer, sheet_name='Prediction_Analysis', startrow=pred_row, header=False, index=False)
            pred_row += 2
            
            if not block['weekday_df'].empty:
                pd.DataFrame([f"평일 최종 예측 (데이터 {block['wd_count']}일, 남은 평일 {remaining_weekdays}일)"]).to_excel(writer, sheet_name='Prediction_Analysis', startrow=pred_row, header=False, index=False)
                block['weekday_df'].to_excel(writer, sheet_name='Prediction_Analysis', startrow=pred_row + 1, index=True)
                pred_row += len(block['weekday_df']) + 3
                
            if not block['weekend_df'].empty:
                pd.DataFrame([f"주말 최종 예측 (데이터 {block['we_count']}일, 남은 주말 {remaining_weekends}일)"]).to_excel(writer, sheet_name='Prediction_Analysis', startrow=pred_row, header=False, index=False)
                block['weekend_df'].to_excel(writer, sheet_name='Prediction_Analysis', startrow=pred_row + 1, index=True)
                pred_row += len(block['weekend_df']) + 3

            # Calculate and Write the Combined Total
            wd_df, we_df = block['weekday_df'], block['weekend_df']
            if wd_df.empty and we_df.empty:
                pred_row += 2
                continue

            combined_df = wd_df.add(we_df, fill_value=0) if not wd_df.empty and not we_df.empty else (wd_df.copy() if not wd_df.empty else we_df.copy())
            total_to_write = combined_df.loc[['합계']] if '합계' in combined_df.index else combined_df
            total_to_write.index = ['평일+주말 총합계']
            
            pd.DataFrame(["▶ 평일+주말 통합 예측 결과"]).to_excel(writer, sheet_name='Prediction_Analysis', startrow=pred_row, header=False, index=False)
            total_to_write.to_excel(writer, sheet_name='Prediction_Analysis', startrow=pred_row + 1, index=True)
            pred_row += len(total_to_write) + 4
            
    output_buffer.seek(0)
    return output_buffer
