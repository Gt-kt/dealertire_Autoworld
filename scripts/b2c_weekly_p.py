# /scripts/b2c_weekly_p.py
import pandas as pd
import numpy as np
import io
from datetime import date
import calendar

def process_file(input_stream):
    """
    Reads an Excel file stream, performs historical and predictive analysis,
    and returns a new Excel file with the results in memory.
    """
    output_sheet_name = 'Analysis_Results'
    prediction_sheet_name = 'Prediction_Analysis'

    try:
        df = pd.read_excel(input_stream, engine='openpyxl')

        # --- 1. Data Cleaning and Preparation ---
        for col in ['상품타입', '브랜드', '패턴', '주문채널', '주문상품']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        numeric_cols = ['주문수량', '상품주문금액', '실결제금액', '장착비']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(float)

        df['주문일'] = pd.to_datetime(df['주문일'], format='%Y%m%d', errors='coerce')
        df.dropna(subset=['주문일'], inplace=True)

        holidays_2025 = [
            date(2025, 1, 1), date(2025, 1, 28), date(2025, 1, 29), date(2025, 1, 30),
            date(2025, 3, 1), date(2025, 5, 5), date(2025, 5, 6), date(2025, 6, 3), 
            date(2025, 6, 6), date(2025, 8, 15), date(2025, 10, 3), date(2025, 10, 6), 
            date(2025, 10, 7), date(2025, 10, 8), date(2025, 10, 9), date(2025, 12, 25)
        ]

        def get_day_type(d):
            d_date = d.date()
            return 'Weekend' if d_date in holidays_2025 or d.weekday() >= 5 else 'Weekday'

        df['Day_Type'] = df['주문일'].apply(get_day_type)

    except Exception as e:
        raise ValueError(f"File Read/Clean Error: {e}")

    # --- 2. Perform All Historical Analysis Tasks ---
    df_tire = df[(df['상품타입'] == '타이어') & (df['브랜드'] != '기타')].copy()
    df_alignment = df[df['상품타입'] == '휠얼라인먼트'].copy()

    if '패턴' in df_tire.columns and '브랜드' in df_tire.columns:
        df_tire['Analysis_Brand'] = df_tire['브랜드']
        is_goodyear = df_tire['브랜드'] == '굿이어'
        contains_cooper = df_tire['패턴'].str.contains('쿠퍼', na=False)
        df_tire.loc[is_goodyear & contains_cooper, 'Analysis_Brand'] = '굿이어 (쿠퍼)'
        df_tire.loc[is_goodyear & ~contains_cooper, 'Analysis_Brand'] = '굿이어 (기타)'
    else:
        df_tire['Analysis_Brand'] = df_tire['브랜드'] if '브랜드' in df_tire.columns else 'Unknown'

    def adjust_for_vat(df, cols):
        df_adj = df.copy()
        for col in cols:
            if col in df_adj.columns:
                df_adj[col] = df_adj[col] / 1.1
        return df_adj

    r_cols_financial = ['상품주문금액', '실결제금액', '장착비']

    # 1. Tire Sales by Channel
    result1_base = df_tire.groupby('주문채널').agg(주문수량=('주문수량', 'sum'), 상품주문금액=('상품주문금액', 'sum'), 실결제금액=('실결제금액', 'sum')).reset_index()
    cols1_to_process = [col for col in r_cols_financial if col in result1_base.columns]
    result1_adj = adjust_for_vat(result1_base, cols1_to_process)
    result1_adj[cols1_to_process] = result1_adj[cols1_to_process].round(-3)
    sum1 = result1_adj.sum(numeric_only=True)
    result1 = pd.concat([result1_adj, pd.DataFrame([{'주문채널': '합계', **sum1}])], ignore_index=True) if not result1_adj.empty else result1_adj

    # 2. Other Product Sales
    product_types_2 = ['배터리', '세차권', '와이퍼']
    df_etc = df[df['상품타입'].isin(product_types_2)].copy()
    result2_base = df_etc.groupby('상품타입').agg(주문수량=('주문수량', 'sum'), 상품주문금액=('상품주문금액', 'sum'), 장착비=('장착비', 'sum'), 실결제금액=('실결제금액', 'sum')).reset_index()
    cols2_to_process = [col for col in r_cols_financial if col in result2_base.columns]
    result2_adj = adjust_for_vat(result2_base, cols2_to_process)
    result2_adj[cols2_to_process] = result2_adj[cols2_to_process].round(-3)
    sum2 = result2_adj.sum(numeric_only=True)
    result2 = pd.concat([result2_adj, pd.DataFrame([{'상품타입': '합계', **sum2}])], ignore_index=True) if not result2_adj.empty else result2_adj

    # 3. Engine Oil Sales
    df_oil_filtered = df[(df['상품타입'] == '엔진오일') & (df['주문상품'].str.contains('오일필터', na=False))].copy()
    result3_base = pd.DataFrame({'개수(count)': [df_oil_filtered['주문번호'].nunique()], '주문수량': [df_oil_filtered['주문수량'].sum()], '상품주문금액': [df_oil_filtered['상품주문금액'].sum()], '실결제금액': [df_oil_filtered['실결제금액'].sum()]})
    cols3_to_process = [col for col in r_cols_financial if col in result3_base.columns]
    result3 = adjust_for_vat(result3_base, cols3_to_process)
    result3[cols3_to_process] = result3[cols3_to_process].round(-3)

    # 4. Service Value Analysis
    unrounded_sum1 = adjust_for_vat(result1_base, cols1_to_process).sum(numeric_only=True)
    unrounded_sum2 = adjust_for_vat(result2_base, cols2_to_process).sum(numeric_only=True)
    val_1 = unrounded_sum1.get('실결제금액', 0) - unrounded_sum1.get('상품주문금액', 0)
    val_2 = unrounded_sum2.get('실결제금액', 0) - unrounded_sum2.get('상품주문금액', 0) - unrounded_sum2.get('장착비', 0)
    val_3 = df_oil_filtered['주문수량'].sum() * 25000
    total_val = val_1 + val_2 + val_3
    result4 = pd.DataFrame({
        '구분': ['타이어 용역가치 (1)', '기타상품 용역가치 (2)', '엔진오일 용역가치 (3)', '총 용역가치'],
        '금액': [np.round(val_1, -3), np.round(val_2, -3), np.round(val_3, -3), np.round(total_val, -3)]
    })
    result4['금액'] = result4['금액'].apply(lambda x: f"{x:,.0f}")

    # 5. Customer Analysis
    result5_base = df_tire.groupby('주문채널')['고객id'].nunique().reset_index().rename(columns={'고객id': '고유고객수'})
    sum5 = result5_base.sum(numeric_only=True)
    result5 = pd.concat([result5_base, pd.DataFrame([{'주문채널': '합계', **sum5}])], ignore_index=True) if not result5_base.empty else result5_base

    # 6. Tire Sales by Brand
    result6_base = df_tire.groupby('Analysis_Brand').agg(주문수량=('주문수량', 'sum'), 상품주문금액=('상품주문금액', 'sum'), 실결제금액=('실결제금액', 'sum')).reset_index()
    cols6_to_process = [col for col in r_cols_financial if col in result6_base.columns]
    result6_adj = adjust_for_vat(result6_base, cols6_to_process)
    result6_adj[cols6_to_process] = result6_adj[cols6_to_process].round(-3)
    result6_adj = result6_adj.rename(columns={'Analysis_Brand': '브랜드'})
    sum6 = result6_adj.sum(numeric_only=True)
    result6 = pd.concat([result6_adj, pd.DataFrame([{'브랜드': '합계', **sum6}])], ignore_index=True) if not result6_adj.empty else result6_adj

    # 7. Alignment Analysis
    alignment_quantity = df_alignment['주문수량'].sum()
    alignment_value = alignment_quantity * 3000
    result7 = pd.DataFrame({'상품타입': ['휠얼라인먼트'], '주문수량 합계': [alignment_quantity], '계산결과 (수량*3000)': [f"{np.round(alignment_value, -3):,.0f}"]})

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

    def generate_grouped_prediction(df_filtered, group_by_col, agg_dict, title, numeric_cols, cols_to_divide=[], cols_to_round=[]):
        df_weekday = df_filtered[df_filtered['Day_Type'] == 'Weekday']; weekdays_in_data = df_weekday['주문일'].nunique(); weekday_pred_df = pd.DataFrame()
        if weekdays_in_data > 0:
            hist_wd = df_weekday.groupby(group_by_col).agg(agg_dict)
            if not hist_wd.empty:
                cols_div = [c for c in cols_to_divide if c in hist_wd.columns]
                hist_wd = adjust_for_vat(hist_wd, cols_div)
                avg_wd = hist_wd[numeric_cols].div(weekdays_in_data); pred_wd = avg_wd.multiply(remaining_weekdays); total_wd = hist_wd[numeric_cols].add(pred_wd)
                cols_rnd = [c for c in cols_to_round if c in total_wd.columns]
                total_wd[cols_rnd] = total_wd[cols_rnd].round(-3)
                total_wd.loc['합계'] = total_wd.sum(); weekday_pred_df = total_wd

        df_weekend = df_filtered[df_filtered['Day_Type'] == 'Weekend']; weekends_in_data = df_weekend['주문일'].nunique(); weekend_pred_df = pd.DataFrame()
        if weekends_in_data > 0:
            hist_we = df_weekend.groupby(group_by_col).agg(agg_dict)
            if not hist_we.empty:
                cols_div = [c for c in cols_to_divide if c in hist_we.columns]
                hist_we = adjust_for_vat(hist_we, cols_div)
                avg_we = hist_we[numeric_cols].div(weekends_in_data); pred_we = avg_we.multiply(remaining_weekends); total_we = hist_we[numeric_cols].add(pred_we)
                cols_rnd = [c for c in cols_to_round if c in total_we.columns]
                total_we[cols_rnd] = total_we[cols_rnd].round(-3)
                total_we.loc['합계'] = total_we.sum(); weekend_pred_df = total_we
        prediction_blocks.append({'title': title, 'weekday_df': weekday_pred_df, 'weekend_df': weekend_pred_df, 'wd_count': weekdays_in_data, 'we_count': weekends_in_data})

    def generate_scalar_prediction(df_filtered, agg_dict, title, multipliers={}, cols_to_divide=[], cols_to_round=[]):
        df_weekday = df_filtered[df_filtered['Day_Type'] == 'Weekday']; weekdays_in_data = df_weekday['주문일'].nunique(); weekday_pred_df = pd.DataFrame()
        if weekdays_in_data > 0:
            hist = df_weekday.agg(agg_dict); hist = pd.Series(hist)
            cols_div = [c for c in cols_to_divide if c in hist.index]
            for col in cols_div: hist[col] /= 1.1
            avg = hist / weekdays_in_data; pred = avg * remaining_weekdays; total = hist + pred
            for col, mult in multipliers.items(): total[col] *= mult
            cols_rnd = [c for c in cols_to_round if c in total.index]
            total[cols_rnd] = total[cols_rnd].round(-3)
            weekday_pred_df = pd.DataFrame(total).T

        df_weekend = df_filtered[df_filtered['Day_Type'] == 'Weekend']; weekends_in_data = df_weekend['주문일'].nunique(); weekend_pred_df = pd.DataFrame()
        if weekends_in_data > 0:
            hist = df_weekend.agg(agg_dict); hist = pd.Series(hist)
            cols_div = [c for c in cols_to_divide if c in hist.index]
            for col in cols_div: hist[col] /= 1.1
            avg = hist / weekends_in_data; pred = avg * remaining_weekends; total = hist + pred
            for col, mult in multipliers.items(): total[col] *= mult
            cols_rnd = [c for c in cols_to_round if c in total.index]
            total[cols_rnd] = total[cols_rnd].round(-3)
            weekend_pred_df = pd.DataFrame(total).T
        prediction_blocks.append({'title': title, 'weekday_df': weekday_pred_df, 'weekend_df': weekend_pred_df, 'wd_count': weekdays_in_data, 'we_count': weekends_in_data})
    
    def generate_service_value_prediction(df_tire, df_etc, df_oil, remaining_weekdays, remaining_weekends):
        weekday_pred_df, weekend_pred_df = pd.DataFrame(), pd.DataFrame()
        df_tire_wd = df_tire[df_tire['Day_Type'] == 'Weekday']; weekdays_in_data = df_tire_wd['주문일'].nunique()
        if weekdays_in_data > 0:
            hist_v1 = (df_tire_wd['실결제금액'].sum() - df_tire_wd['상품주문금액'].sum()) / 1.1
            hist_v2 = (df_etc[df_etc['Day_Type'] == 'Weekday']['실결제금액'].sum() - df_etc[df_etc['Day_Type'] == 'Weekday']['상품주문금액'].sum() - df_etc[df_etc['Day_Type'] == 'Weekday']['장착비'].sum()) / 1.1
            hist_v3 = df_oil[df_oil['Day_Type'] == 'Weekday']['주문수량'].sum() * 25000
            total_v1 = hist_v1 + (hist_v1 / weekdays_in_data * remaining_weekdays); total_v2 = hist_v2 + (hist_v2 / weekdays_in_data * remaining_weekdays); total_v3 = hist_v3 + (hist_v3 / weekdays_in_data * remaining_weekdays)
            weekday_pred_df = pd.DataFrame({'금액': [total_v1, total_v2, total_v3, total_v1 + total_v2 + total_v3]}, index=['타이어 용역가치 (1)', '기타상품 용역가치 (2)', '엔진오일 용역가치 (3)', '총 용역가치'])
            weekday_pred_df['금액'] = weekday_pred_df['금액'].round(-3)

        df_tire_we = df_tire[df_tire['Day_Type'] == 'Weekend']; weekends_in_data = df_tire_we['주문일'].nunique()
        if weekends_in_data > 0:
            hist_v1 = (df_tire_we['실결제금액'].sum() - df_tire_we['상품주문금액'].sum()) / 1.1
            hist_v2 = (df_etc[df_etc['Day_Type'] == 'Weekend']['실결제금액'].sum() - df_etc[df_etc['Day_Type'] == 'Weekend']['상품주문금액'].sum() - df_etc[df_etc['Day_Type'] == 'Weekend']['장착비'].sum()) / 1.1
            hist_v3 = df_oil[df_oil['Day_Type'] == 'Weekend']['주문수량'].sum() * 25000
            total_v1 = hist_v1 + (hist_v1 / weekends_in_data * remaining_weekends); total_v2 = hist_v2 + (hist_v2 / weekends_in_data * remaining_weekends); total_v3 = hist_v3 + (hist_v3 / weekends_in_data * remaining_weekends)
            weekend_pred_df = pd.DataFrame({'금액': [total_v1, total_v2, total_v3, total_v1 + total_v2 + total_v3]}, index=['타이어 용역가치 (1)', '기타상품 용역가치 (2)', '엔진오일 용역가치 (3)', '총 용역가치'])
            weekend_pred_df['금액'] = weekend_pred_df['금액'].round(-3)

        prediction_blocks.append({'title': '4. 용역 가치 분석 - 예측', 'weekday_df': weekday_pred_df, 'weekend_df': weekend_pred_df, 'wd_count': weekdays_in_data, 'we_count': weekends_in_data})

    # --- Generate all prediction blocks ---
    generate_grouped_prediction(df_tire, '주문채널', {'주문수량': 'sum', '상품주문금액': 'sum', '실결제금액': 'sum'}, "1. 타이어 판매 현황 (by 주문채널) - 예측", ['주문수량', '상품주문금액', '실결제금액'], cols_to_divide=r_cols_financial, cols_to_round=r_cols_financial)
    generate_grouped_prediction(df_etc, '상품타입', {'주문수량': 'sum', '상품주문금액': 'sum', '장착비': 'sum', '실결제금액': 'sum'}, "2. 기타 상품 판매 현황 - 예측", ['주문수량', '상품주문금액', '장착비', '실결제금액'], cols_to_divide=r_cols_financial, cols_to_round=r_cols_financial)
    generate_scalar_prediction(df_oil_filtered, {'주문수량': 'sum', '상품주문금액':'sum', '실결제금액':'sum'}, '3. 엔진오일(오일필터) 주문 내역 (집계) - 예측', cols_to_divide=r_cols_financial, cols_to_round=r_cols_financial)
    generate_service_value_prediction(df_tire, df_etc, df_oil_filtered, remaining_weekdays, remaining_weekends)
    generate_grouped_prediction(df_tire, '주문채널', {'고객id': 'nunique'}, "5. 타이어 구매 고객 분석 - 예측", ['고객id'])
    generate_grouped_prediction(df_tire, 'Analysis_Brand', {'주문수량': 'sum', '상품주문금액': 'sum', '실결제금액': 'sum'}, "6. 타이어 판매 현황 (by 브랜드) - 예측", ['주문수량', '상품주문금액', '실결제금액'], cols_to_divide=r_cols_financial, cols_to_round=r_cols_financial)
    generate_scalar_prediction(df_alignment, {'주문수량': 'sum'}, "7. 휠얼라이먼트 분석 - 예측", multipliers={'주문수량': 3000}, cols_to_round=['주문수량'])

    # --- 4. Save All Results to Excel file in memory ---
    output_buffer = io.BytesIO()
    with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
        # Write Historical Analysis Sheet
        current_row = 0
        for df_to_write, title in historical_results:
            if not df_to_write.empty:
                pd.DataFrame([title]).to_excel(writer, sheet_name=output_sheet_name, startrow=current_row, header=False, index=False)
                df_to_write.to_excel(writer, sheet_name=output_sheet_name, startrow=current_row + 2, index=False)
                current_row += len(df_to_write) + 5

        # Write Prediction Analysis Sheet
        pred_row = 0
        for block in prediction_blocks:
            pd.DataFrame([block['title']]).to_excel(writer, sheet_name=prediction_sheet_name, startrow=pred_row, header=False, index=False)
            pred_row += 2

            if not block['weekday_df'].empty:
                pd.DataFrame([f"평일 최종 예측 (데이터 {block['wd_count']}일, 남은 평일 {remaining_weekdays}일)"]).to_excel(writer, sheet_name=prediction_sheet_name, startrow=pred_row, header=False, index=False)
                block['weekday_df'].to_excel(writer, sheet_name=prediction_sheet_name, startrow=pred_row + 1, index=True)
                pred_row += len(block['weekday_df']) + 3

            if not block['weekend_df'].empty:
                pd.DataFrame([f"주말 최종 예측 (데이터 {block['we_count']}일, 남은 주말 {remaining_weekends}일)"]).to_excel(writer, sheet_name=prediction_sheet_name, startrow=pred_row, header=False, index=False)
                block['weekend_df'].to_excel(writer, sheet_name=prediction_sheet_name, startrow=pred_row + 1, index=True)
                pred_row += len(block['weekend_df']) + 3

            # Calculate and Write the Combined Total
            wd_df, we_df = block['weekday_df'], block['weekend_df']
            if wd_df.empty and we_df.empty:
                pred_row += 2
                continue

            if not wd_df.empty and not we_df.empty: combined_df = wd_df.add(we_df, fill_value=0)
            elif not wd_df.empty: combined_df = wd_df.copy()
            else: combined_df = we_df.copy()

            if '총 용역가치' in combined_df.index:
                total_to_write = combined_df.loc[['총 용역가치']]
            elif '합계' in combined_df.index:
                total_to_write = combined_df.loc[['합계']]
            else:
                total_to_write = combined_df

            if not total_to_write.empty:
                total_to_write.index = ['평일+주말 총합계']
                
                if block['title'] == '4. 용역 가치 분석 - 예측':
                    total_to_write['금액'] = total_to_write['금액'].apply(lambda x: f"{x:,.0f}")
                if block['title'] == '7. 휠얼라이먼트 분석 - 예측':
                    total_to_write.rename(columns={'주문수량': '계산결과 (수량*3000)'}, inplace=True)
                    total_to_write['계산결과 (수량*3000)'] = total_to_write['계산결과 (수량*3000)'].apply(lambda x: f"{x:,.0f}")

                pd.DataFrame(["▶ 평일+주말 통합 예측 결과"]).to_excel(writer, sheet_name=prediction_sheet_name, startrow=pred_row, header=False, index=False)
                total_to_write.to_excel(writer, sheet_name=prediction_sheet_name, startrow=pred_row + 1, index=True)
                pred_row += len(total_to_write) + 4

    output_buffer.seek(0)
    return output_buffer