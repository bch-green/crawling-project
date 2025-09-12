#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
수정된 init_load_from_csv.py
한글 헤더 Google Sheets에 호환되도록 수정
"""

import csv
from datetime import datetime, timezone, timedelta
import os
import yaml

def ensure_header(ws, header):
    """헤더 확인 및 설정"""
    cur = ws.row_values(1)
    if not cur:
        ws.append_row(header)
        return
    # 헤더가 일부 없으면 오류(명시적 운영)
    missing = [h for h in header if h not in cur]
    if missing:
        print(f"⚠️ 누락된 헤더 컬럼: {missing}")
        print("시트의 기존 헤더를 사용합니다.")

def list_existing_keys(ws, key_col: str) -> set[str]:
    """기존 clncTestSn 목록 가져오기"""
    header = ws.row_values(1)
    if key_col not in header:
        return set()
    idx = header.index(key_col) + 1
    vals = ws.col_values(idx)[1:]  # exclude header
    return set(v.strip() for v in vals if v and str(v).strip())

def append_rows(ws, rows: list[dict], header: list[str]) -> int:
    """새 행들 추가"""
    if not rows:
        return 0
    
    # 시트의 실제 헤더 사용
    actual_header = ws.row_values(1)
    
    values = []
    for row in rows:
        row_values = []
        for col in actual_header:
            value = row.get(col, "")
            if value is None:
                value = ""
            row_values.append(str(value))
        values.append(row_values)
    
    ws.append_rows(values, value_input_option="RAW")
    return len(values)

def open_ws(cfg):
    """Google Sheets 워크시트 열기"""
    import gspread
    from google.oauth2.service_account import Credentials

    creds = Credentials.from_service_account_file(
        cfg["service_account_json"], 
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(cfg["sheet_id"])
    try:
        ws = sh.worksheet(cfg["worksheet"])
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(cfg["worksheet"], rows=2000, cols=50)
    return ws

# 한글 헤더 (현재 Google Sheets와 일치)
KOREAN_HEADER = [
    "clncTestSn", "진행상태", "임상시험명", "임상시험 의뢰자", "소재지", 
    "대상질환", "대상질환명", "임상시험 단계", "임상시험 기간", 
    "임상시험 시작월", "임상시험 종료월", "성별", "나이", 
    "목표 대상자 수(국내)", "임상시험 승인일자", "최근 변경일자", "이용문의",
    "실시기관1", "실시기관2", "실시기관3", "실시기관4", "실시기관5",
    "실시기관6", "실시기관7", "실시기관8", "실시기관9", "실시기관10",
    "실시기관11", "실시기관12", "실시기관13", "실시기관14", "실시기관15",
    "실시기관16", "실시기관17", "실시기관18", "실시기관19", "실시기관20",
    "실시기관21", "실시기관22", "실시기관23", "실시기관24", "실시기관25",
    "실시기관26", "실시기관27", "실시기관28", "실시기관29", "실시기관30",
    "조회수", "등록일자"
]

def map_csv_row(row: dict) -> dict:
    """정제 CSV(한글 헤더)를 시트 형식으로 매핑"""
    clncsn = (row.get("clncTestSn") or "").strip()
    
    mapped_row = {
        "clncTestSn": clncsn,
        "진행상태": row.get("진행상태") or "",
        "임상시험명": row.get("임상시험명") or "",
        "임상시험 의뢰자": row.get("임상시험 의뢰자") or "",
        "소재지": row.get("소재지") or "",
        "대상질환": row.get("대상질환") or "",
        "대상질환명": row.get("대상질환명") or "",
        "임상시험 단계": row.get("임상시험 단계") or "",
        "임상시험 기간": row.get("임상시험 기간") or "",
        "임상시험 시작월": row.get("임상시험 시작월") or "",
        "임상시험 종료월": row.get("임상시험 종료월") or "",
        "성별": row.get("성별") or "",
        "나이": row.get("나이") or "",
        "목표 대상자 수(국내)": row.get("목표 대상자 수(국내)") or "",
        "임상시험 승인일자": row.get("임상시험 승인일자") or "",
        "최근 변경일자": row.get("최근 변경일자") or "",
        "이용문의": row.get("이용문의") or "",
        "조회수": row.get("조회수") or "",
        "등록일자": row.get("등록일자") or "",
    }
    
    # 실시기관 1-30 매핑
    for i in range(1, 31):
        institution_key = f"실시기관{i}"
        contact_key = f"실시기관{i}_담당자"
        etc_key = f"실시기관{i}_기타"
        
        mapped_row[institution_key] = row.get(institution_key) or ""
        # 담당자, 기타 정보는 현재 시트에 없으므로 생략
    
    return mapped_row

def main(csv_path: str, cfg_path="config/settings.yaml"):
    """메인 실행 함수"""
    print(f"CSV 파일 로드 시작: {csv_path}")
    
    # 설정 로드
    cfg = yaml.safe_load(open(cfg_path, "r", encoding="utf-8"))
    
    # 시트 열기
    ws = open_ws(cfg)
    
    # 기존 clncTestSn 집합
    existing_sn = list_existing_keys(ws, key_col="clncTestSn")
    print(f"기존 데이터: {len(existing_sn)}개")
    
    # CSV 읽어서 신규만 필터링
    rows_to_add = []
    total_processed = 0
    
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_processed += 1
            mapped_row = map_csv_row(row)
            sn = mapped_row.get("clncTestSn", "").strip()
            
            if not sn:
                print(f"⚠️ clncTestSn이 없는 행 스킵")
                continue
                
            if sn in existing_sn:
                print(f"⚠️ 중복 SN 스킵: {sn}")
                continue
            
            rows_to_add.append(mapped_row)
    
    print(f"처리된 총 행 수: {total_processed}")
    print(f"추가할 새 데이터: {len(rows_to_add)}개")
    
    if rows_to_add:
        # 헤더 확인 (시트의 기존 헤더 사용)
        ensure_header(ws, KOREAN_HEADER)
        
        # 새 데이터 추가
        added_count = append_rows(ws, rows_to_add, KOREAN_HEADER)
        print(f"✅ Google Sheets에 {added_count}개 행 추가됨")
        
        # 샘플 데이터 표시
        if rows_to_add:
            sample = rows_to_add[0]
            title = sample.get('임상시험명', '')[:50]
            print(f"📄 샘플: SN={sample.get('clncTestSn')} | {title}...")
    else:
        print("ℹ️ 추가할 새 데이터가 없습니다.")
    
    print("✅ 업로드 완료!")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python jobs/init_load_from_csv.py <clean_csv_path> [config/settings.yaml]")
    
    csv_path = sys.argv[1]
    cfg_path = sys.argv[2] if len(sys.argv) > 2 else "config/settings.yaml"
    
    if not os.path.exists(csv_path):
        raise SystemExit(f"❌ CSV 파일을 찾을 수 없습니다: {csv_path}")
    
    main(csv_path, cfg_path)