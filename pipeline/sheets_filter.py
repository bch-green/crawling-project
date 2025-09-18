#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
구글 시트 기반 임상시험 데이터 필터링 및 분류 시트 생성 모듈

기존 구글 시트의 데이터를 읽어와서 다양한 기준으로 필터링한 후
별도의 워크시트로 분류하여 저장하는 모듈입니다.

주요 기능:
1️⃣ 구글 시트에서 전체 임상시험 데이터 읽기
2️⃣ 진행상태 기본 필터링 (승인완료, 모집중)
3️⃣ 추가 필터링 조건 적용 (건강인 제외, 2상 이상 등)
4️⃣ 필터링된 데이터를 새로운 워크시트로 저장

생성되는 시트:
- filtered_trials_premium: 모든 필터링 조건을 만족하는 프리미엄 임상시험
- filtered_trials_recruiting: 모집중인 임상시험만
- filtered_trials_approved: 승인완료된 임상시험만

사용법:
  python pipeline/sheets_filter.py config/settings.yaml

작성자: 시트 필터링 파이프라인  
최종 수정: 2025-09-15
"""

import re
import yaml
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple
from pathlib import Path

# 기존 sheets_io 모듈 import
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))
from sheets_io import client_from_sa, open_ws


# =============================================================================
# 필터링 기준 설정
# =============================================================================

# 건강인 대상 시험 키워드
HEALTHY_VOLUNTEER_KEYWORDS = [
    "건강한", "건강한 성인", "건강한 자원자", "생동성", "생물학적 동등성",
    "약동학적 특성", "BE 시험", "PK 시험", "건강한 피험자", "약동학", "생체이용률"
]

# 연구자 임상시험 패턴
INVESTIGATOR_INITIATED_PATTERNS = [
    "연구자 임상시험", "연구자주도", "연구자 주도", "IIT", "의사주도"
]

# 2상 이상 패턴
PHASE_2_PLUS_PATTERN = re.compile(
    r'(?:2상|2a상|2b상|2/3상|2-3상|3상|3a상|3b상|4상|II상|IIa상|IIb상|II/III상|III상|IIIa상|IIIb상|IV상)', 
    re.IGNORECASE
)


# =============================================================================
# 필터링 함수들
# =============================================================================

def is_healthy_volunteer_study(row: pd.Series) -> bool:
    """건강인 대상 시험인지 판별"""
    title = str(row.get("임상시험명", "")).strip()
    disease = str(row.get("대상질환명", "")).strip()
    phase = str(row.get("임상시험 단계", "")).strip()
    
    text_to_check = f"{title} {disease}".lower()
    
    for keyword in HEALTHY_VOLUNTEER_KEYWORDS:
        if keyword.lower() in text_to_check:
            return True
    
    if "생동" in phase or "BE" in phase.upper() or "PK" in phase.upper():
        return True
        
    return False


def is_investigator_initiated(row: pd.Series) -> bool:
    """연구자 임상시험인지 판별"""
    sponsor = str(row.get("임상시험 의뢰자", "")).strip()
    phase = str(row.get("임상시험 단계", "")).strip()
    title = str(row.get("임상시험명", "")).strip()
    
    for pattern in INVESTIGATOR_INITIATED_PATTERNS:
        if pattern in phase or pattern in title:
            return True
    
    hospital_patterns = ["병원", "의료원", "센터", "의과대학", "대학교"]
    if any(pattern in sponsor for pattern in hospital_patterns):
        return True
        
    return False


def is_phase_2_or_higher(row: pd.Series) -> bool:
    """2상 이상인지 판별"""
    phase = str(row.get("임상시험 단계", "")).strip()
    if not phase:
        return False
    return bool(PHASE_2_PLUS_PATTERN.search(phase))


def extract_domestic_participants(participants_str: str) -> int:
    """국내 모집인원 추출 (괄호 안 숫자)"""
    if not isinstance(participants_str, str):
        return 0
    
    match = re.search(r'\((\d+)\)', participants_str.strip())
    if match:
        return int(match.group(1))
    return 0


def calculate_study_duration_months(start_month: str, end_month: str) -> int:
    """연구 기간을 월 단위로 계산"""
    if not start_month or not end_month:
        return 0
    
    try:
        start_parts = str(start_month).split('-')
        end_parts = str(end_month).split('-')
        
        if len(start_parts) != 2 or len(end_parts) != 2:
            return 0
            
        start_year, start_mon = int(start_parts[0]), int(start_parts[1])
        end_year, end_mon = int(end_parts[0]), int(end_parts[1])
        
        duration = (end_year - start_year) * 12 + (end_mon - start_mon)
        return max(0, duration)
        
    except (ValueError, IndexError):
        return 0


# =============================================================================
# 구글 시트 데이터 처리
# =============================================================================

def read_sheet_data(cfg: Dict) -> pd.DataFrame:
    """구글 시트에서 전체 데이터 읽어오기"""
    print("📡 구글 시트에서 데이터 읽는 중...")
    
    gc = client_from_sa(cfg["service_account_json"])
    ws = open_ws(gc, cfg["sheet_id"], cfg["worksheet"])
    
    # 모든 데이터 가져오기
    all_values = ws.get_all_values()
    
    if not all_values:
        raise ValueError("시트에서 데이터를 읽을 수 없습니다")
    
    # 첫 번째 행을 헤더로, 나머지를 데이터로 변환
    headers = all_values[0]
    data_rows = all_values[1:]
    
    df = pd.DataFrame(data_rows, columns=headers)
    print(f"✅ {len(df):,}개 행 읽기 완료")
    
    return df


def apply_base_filters(df: pd.DataFrame) -> pd.DataFrame:
    """기본 필터링: 진행상태가 '승인완료' 또는 '모집중'인 것만"""
    print("🔍 기본 필터링 적용 중 (진행상태: 승인완료, 모집중)")
    
    original_count = len(df)
    
    # 진행상태 컬럼이 있는지 확인
    if "진행상태" not in df.columns:
        print("⚠️ '진행상태' 컬럼을 찾을 수 없습니다")
        return df
    
    # 승인완료 또는 모집중인 것만 필터링
    mask = df["진행상태"].isin(["승인완료", "모집중"])
    filtered_df = df[mask].copy()
    
    excluded_count = original_count - len(filtered_df)
    print(f"📊 기본 필터링 결과: {len(filtered_df):,}개 (제외: {excluded_count:,}개)")
    
    return filtered_df


def apply_premium_filters(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """프리미엄 필터링: 모든 고급 조건 적용"""
    print("⭐ 프리미엄 필터링 적용 중...")
    
    original_count = len(df)
    stats = {"original": original_count, "stages": {}}
    
    current_df = df.copy()
    
    # 1. 건강인 대상 시험 제외
    mask_healthy = ~current_df.apply(is_healthy_volunteer_study, axis=1)
    current_df = current_df[mask_healthy]
    stats["stages"]["exclude_healthy"] = len(current_df)
    
    # 2. 연구자 임상시험 제외
    mask_ii = ~current_df.apply(is_investigator_initiated, axis=1)
    current_df = current_df[mask_ii]
    stats["stages"]["exclude_investigator"] = len(current_df)
    
    # 3. 2상 이상만 포함
    mask_phase = current_df.apply(is_phase_2_or_higher, axis=1)
    current_df = current_df[mask_phase]
    stats["stages"]["phase_2_plus"] = len(current_df)
    
    # 4. 국내 모집인원 10명 이상
    current_df["국내_모집인원"] = current_df["목표 대상자 수(국내)"].apply(extract_domestic_participants)
    mask_participants = current_df["국내_모집인원"] >= 10
    current_df = current_df[mask_participants]
    stats["stages"]["min_10_participants"] = len(current_df)
    
    # 5. 모집기간 12개월 이상
    current_df["연구기간_월"] = current_df.apply(
        lambda row: calculate_study_duration_months(
            row["임상시험 시작월"], 
            row["임상시험 종료월"]
        ), axis=1
    )
    mask_duration = current_df["연구기간_월"] >= 12
    current_df = current_df[mask_duration]
    stats["stages"]["min_12_months"] = len(current_df)
    
    stats["final"] = len(current_df)
    
    print(f"✨ 프리미엄 필터링 완료: {len(current_df):,}개")
    return current_df, stats


def setup_contact_status_dropdown(ws, header: list[str]):
    """컨택상태 컬럼에 드롭다운 목록 설정"""
    try:
        if "컨택상태" not in header:
            return
            
        # 컨택상태 컬럼 인덱스 찾기
        contact_col_idx = header.index("컨택상태") + 1
        
        # 드롭다운 옵션 정의
        contact_options = [
            "데이터없음",
            "컨택필요", 
            "컨택중",
            "컨택종료",
            "계약진행중",
            "계약완료"
        ]
        
        # 데이터 검증 규칙 설정
        from gspread.utils import rowcol_to_a1
        
        # 워크시트 ID 가져오기
        worksheet_id = ws.id
        
        # 배치 업데이트 요청
        requests = [{
            "setDataValidation": {
                "range": {
                    "sheetId": worksheet_id,
                    "startRowIndex": 1,  # 헤더 제외
                    "endRowIndex": 1000,
                    "startColumnIndex": contact_col_idx - 1,
                    "endColumnIndex": contact_col_idx
                },
                "rule": {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": option} for option in contact_options]
                    },
                    "showCustomUi": True,
                    "strict": True
                }
            }
        }]
        
        ws.spreadsheet.batch_update({"requests": requests})
        print("📋 컨택상태 드롭다운 설정 완료")
        
    except Exception as e:
        print(f"⚠️ 드롭다운 설정 실패: {e}")

def create_filtered_worksheets(cfg: Dict, base_df: pd.DataFrame, premium_df: pd.DataFrame) -> None:
    """필터링된 데이터를 별도 워크시트에 저장"""
    print("📝 필터링된 워크시트 생성 중...")
    
    gc = client_from_sa(cfg["service_account_json"])
    
    # 워크시트 정의
    worksheets_to_create = [
        {
            "name": "filtered_premium",
            "data": premium_df,
            "description": "프리미엄 필터링 (모든 조건 만족)"
        },
        {
            "name": "filtered_recruiting", 
            "data": base_df[base_df["진행상태"] == "모집중"],
            "description": "모집중인 임상시험"
        },
        {
            "name": "filtered_approved",
            "data": base_df[base_df["진행상태"] == "승인완료"], 
            "description": "승인완료된 임상시험"
        }
    ]
    
    for ws_info in worksheets_to_create:
        try:
            ws_name = ws_info["name"]
            ws_data = ws_info["data"]
            ws_desc = ws_info["description"]
            
            if len(ws_data) == 0:
                print(f"⚠️ {ws_desc}: 데이터가 없어 시트를 생성하지 않습니다")
                continue
            
            print(f"📋 {ws_desc} 시트 생성 중... ({len(ws_data):,}개 행)")
            
            # 워크시트 열기 또는 생성
            ws = open_ws(gc, cfg["sheet_id"], ws_name)
            
            # 기존 컨택상태 보존을 위해 기존 데이터 먼저 읽기
            existing_contact_status = {}
            try:
                existing_records = ws.get_all_records()
                existing_contact_status = {
                    str(record.get("clncTestSn", "")): record.get("컨택상태", "데이터없음")
                    for record in existing_records
                    if record.get("clncTestSn")
                }
                print(f"📋 기존 컨택상태 {len(existing_contact_status)}개 보존됨")
            except Exception as e:
                print(f"⚠️ 기존 컨택상태 읽기 실패 (빈 시트일 수 있음): {e}")

            # 기존 데이터 모두 삭제
            ws.clear()

            # 컨택상태 컬럼 추가 (기존 값 보존 또는 기본값 설정)
            ws_data = ws_data.copy()
            ws_data["컨택상태"] = ws_data["clncTestSn"].astype(str).map(
                lambda sn: existing_contact_status.get(sn, "데이터없음")
            )
            
            # 컨택상태를 clncTestSn과 진행상태 사이에 위치시키기
            cols = ws_data.columns.tolist()
            if "컨택상태" in cols and "clncTestSn" in cols and "진행상태" in cols:
                cols.remove("컨택상태")
                clnc_idx = cols.index("clncTestSn")
                cols.insert(clnc_idx + 1, "컨택상태")
                ws_data = ws_data[cols]
            
            # 헤더와 데이터 준비
            headers = ws_data.columns.tolist()
            
            # 헤더 추가
            ws.append_row(headers)
            
            # 데이터를 배치로 추가 (성능 최적화)
            if len(ws_data) > 0:
                # DataFrame을 리스트로 변환
                data_values = ws_data.fillna("").astype(str).values.tolist()
                
                # 한 번에 모든 데이터 추가
                ws.append_rows(data_values, value_input_option="RAW")
            
            # 컨택상태 드롭다운 설정
            setup_contact_status_dropdown(ws, headers)
            
            print(f"✅ {ws_desc} 완료: {len(ws_data):,}개 행 저장")
            
        except Exception as e:
            print(f"❌ {ws_info['description']} 시트 생성 실패: {e}")


def print_summary_stats(base_df: pd.DataFrame, premium_stats: Dict) -> None:
    """최종 통계 요약 출력"""
    print(f"\n{'='*60}")
    print(f"📊 필터링 결과 요약")
    print(f"{'='*60}")
    
    print(f"🔸 기본 필터링 (승인완료 + 모집중): {len(base_df):,}개")
    
    recruiting_count = len(base_df[base_df["진행상태"] == "모집중"])
    approved_count = len(base_df[base_df["진행상태"] == "승인완료"])
    
    print(f"  ├─ 모집중: {recruiting_count:,}개")
    print(f"  └─ 승인완료: {approved_count:,}개")
    
    print(f"\n⭐ 프리미엄 필터링: {premium_stats['final']:,}개")
    print(f"  ├─ 건강인 제외 후: {premium_stats['stages']['exclude_healthy']:,}개")
    print(f"  ├─ 연구자시험 제외 후: {premium_stats['stages']['exclude_investigator']:,}개") 
    print(f"  ├─ 2상 이상만: {premium_stats['stages']['phase_2_plus']:,}개")
    print(f"  ├─ 국내 10명 이상: {premium_stats['stages']['min_10_participants']:,}개")
    print(f"  └─ 12개월 이상: {premium_stats['stages']['min_12_months']:,}개")
    
    filter_ratio = premium_stats['final'] / premium_stats['original'] if premium_stats['original'] > 0 else 0
    print(f"\n🎯 전체 대비 프리미엄 비율: {filter_ratio:.1%}")


# =============================================================================
# 메인 실행 함수
# =============================================================================

def main(cfg_path: str = "config/settings.yaml"):
    """메인 실행 함수"""
    print("🚀 구글 시트 기반 필터링 시작")
    print(f"⏰ 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 설정 로드
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        
        # 1단계: 구글 시트에서 데이터 읽기
        full_df = read_sheet_data(cfg)
        
        # 2단계: 기본 필터링 (진행상태)
        base_df = apply_base_filters(full_df)
        
        # 3단계: 프리미엄 필터링
        premium_df, premium_stats = apply_premium_filters(base_df)
        
        # 4단계: 필터링된 워크시트 생성
        create_filtered_worksheets(cfg, base_df, premium_df)
        
        # 5단계: 통계 요약
        print_summary_stats(base_df, premium_stats)
        
        print(f"\n🎉 필터링 완료!")
        print(f"📝 생성된 시트: filtered_premium, filtered_recruiting, filtered_approved")
        
        return 0
        
    except Exception as e:
        print(f"❌ 필터링 처리 중 오류: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config/settings.yaml"
    sys.exit(main(cfg_path))