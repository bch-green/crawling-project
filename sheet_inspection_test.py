#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
sheet_inspection_test.py
현재 시트 상태를 정확히 파악하는 테스트 코드
"""

import yaml
import gspread
from google.oauth2.service_account import Credentials

def main():
    print("📊 Google Sheets 상태 확인")
    print("=" * 60)
    
    # 설정 로드
    cfg = yaml.safe_load(open('config/settings.yaml'))
    
    # 시트 연결
    creds = Credentials.from_service_account_file(
        cfg['service_account_json'], 
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    gc = gspread.authorize(creds)
    ws = gc.open_by_key(cfg['sheet_id']).worksheet(cfg['worksheet'])
    
    # 1. 헤더 확인 (1행)
    header = ws.row_values(1)
    print(f"📋 1행 (헤더): {len(header)}개 컬럼")
    for i, col in enumerate(header, 1):
        print(f"  {i:2d}. {col}")
    
    # 2. 최근 추가된 데이터 확인 (2행)
    print(f"\n📄 2행 (최근 데이터):")
    row2 = ws.row_values(2)
    if row2:
        for i, (col_name, value) in enumerate(zip(header, row2), 1):
            display_value = value[:50] + "..." if len(value) > 50 else value
            print(f"  {i:2d}. {col_name}: {display_value}")
    else:
        print("  (빈 행)")
    
    # 3. 전체 행 수 확인
    all_values = ws.get_all_values()
    total_rows = len([row for row in all_values if any(cell.strip() for cell in row)])
    print(f"\n📊 전체 데이터 행 수: {total_rows}행 (헤더 포함)")
    
    # 4. clncTestSn 컬럼의 최근 값들 확인
    if "clncTestSn" in header:
        clnc_col_idx = header.index("clncTestSn")
        clnc_values = ws.col_values(clnc_col_idx + 1)[1:6]  # 상위 5개
        print(f"\n🔢 clncTestSn 최근 5개 값:")
        for i, sn in enumerate(clnc_values, 1):
            print(f"  {i}. {sn}")
    
    # 5. 문제가 될 수 있는 부분 체크
    print(f"\n⚠️ 잠재적 문제 체크:")
    
    # 빈 셀이 많은 행 체크
    if row2:
        empty_count = sum(1 for cell in row2 if not cell.strip())
        print(f"  - 2행의 빈 셀 개수: {empty_count}/{len(row2)}")
        
        # clncTestSn 값 확인
        if "clncTestSn" in header and len(row2) > header.index("clncTestSn"):
            sn_value = row2[header.index("clncTestSn")]
            print(f"  - 2행의 clncTestSn 값: '{sn_value}'")
            
        # 임상시험명 확인
        if "임상시험명" in header and len(row2) > header.index("임상시험명"):
            title_value = row2[header.index("임상시험명")]
            print(f"  - 2행의 임상시험명: '{title_value[:30]}...'")
    
    print("\n" + "=" * 60)
    print("✅ 시트 상태 확인 완료")

if __name__ == "__main__":
    main()