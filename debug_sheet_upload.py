#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
debug_sheet_upload.py
시트 업로드 문제 진단 스크립트
"""

import csv
import yaml
import gspread
from google.oauth2.service_account import Credentials

def load_config(cfg_path="config/settings.yaml"):
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def connect_sheet(cfg):
    creds = Credentials.from_service_account_file(
        cfg["service_account_json"], 
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(cfg["sheet_id"])
    ws = sh.worksheet(cfg["worksheet"])
    return ws

def check_existing_sns(ws):
    """기존 clncTestSn 목록 확인"""
    header = ws.row_values(1)
    print(f"📋 시트 헤더: {header}")
    
    if "clncTestSn" not in header:
        print("❌ clncTestSn 컬럼이 시트에 없습니다!")
        return set()
    
    col_index = header.index("clncTestSn") + 1
    values = ws.col_values(col_index)[1:]
    existing_sns = set(v.strip() for v in values if v and str(v).strip())
    
    print(f"📊 기존 SN 개수: {len(existing_sns)}")
    
    # 최근 10개 SN 표시
    recent_sns = sorted([int(sn) for sn in existing_sns if sn.isdigit()])[-10:]
    print(f"🔢 최근 10개 SN: {recent_sns}")
    
    return existing_sns

def check_clean_csv(csv_path):
    """정제된 CSV 파일 확인"""
    print(f"\n📄 정제 CSV 확인: {csv_path}")
    
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print(f"📊 정제 CSV 행 수: {len(rows)}")
    print(f"📋 정제 CSV 컬럼: {list(rows[0].keys()) if rows else '없음'}")
    
    if rows:
        for i, row in enumerate(rows):
            sn = row.get("clncTestSn", "").strip()
            title = row.get("임상시험명", "")[:50]
            print(f"  {i+1}. SN={sn} | {title}...")
    
    return rows

def test_mapping(csv_rows):
    """매핑 테스트"""
    print(f"\n🔄 매핑 테스트:")
    
    def map_csv_to_sheet_format(csv_row):
        clnc_sn = (csv_row.get("clncTestSn") or "").strip()
        return {
            "clncTestSn": clnc_sn,
            "title": csv_row.get("임상시험명") or "",
            "sponsor": csv_row.get("임상시험 의뢰자") or "",
            "phase": csv_row.get("임상시험 단계") or "",
            "status": csv_row.get("진행상태") or "",
            # 필수 필드만 테스트
        }
    
    for row in csv_rows:
        mapped = map_csv_to_sheet_format(row)
        sn = mapped.get("clncTestSn")
        print(f"  매핑 결과 SN={sn}:")
        for k, v in mapped.items():
            print(f"    {k}: {v[:50] if v else 'Empty'}...")

def main():
    print("🔍 시트 업로드 문제 진단 시작")
    print("=" * 50)
    
    # 1. 설정 로드
    cfg = load_config()
    print(f"✅ 설정 로드 완료")
    
    # 2. 시트 연결 및 기존 데이터 확인
    try:
        ws = connect_sheet(cfg)
        print(f"✅ 시트 연결 성공")
        
        existing_sns = check_existing_sns(ws)
        
        # 647이 이미 있는지 확인
        target_sn = "202500647"
        if target_sn in existing_sns:
            print(f"⚠️ {target_sn}이 이미 시트에 존재합니다!")
        else:
            print(f"✅ {target_sn}이 시트에 없음 - 추가 가능")
            
    except Exception as e:
        print(f"❌ 시트 연결 실패: {e}")
        return
    
    # 3. 정제 CSV 확인
    clean_csv_path = "test_clean.csv"  # 또는 실제 경로
    try:
        csv_rows = check_clean_csv(clean_csv_path)
        if csv_rows:
            test_mapping(csv_rows)
    except Exception as e:
        print(f"❌ CSV 확인 실패: {e}")
    
    print("\n" + "=" * 50)
    print("🔍 진단 완료")

if __name__ == "__main__":
    main()