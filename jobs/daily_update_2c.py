#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
매일 자동 실행되는 임상시험 데이터 증분 업데이트 스크립트

이 스크립트는 cron을 통해 매일 10:00 AM에 자동 실행되며, 
다음과 같은 4단계 파이프라인을 수행합니다:

1단계: 증분 크롤링 (crawler/2c.py)
  - Google Sheets에서 마지막 수집된 clncTestSn 확인
  - 새로운 임상시험 정보만 증분 수집
  - 출력: outputs/increment_YYYYMMDD_HHMMSS.csv

2단계: 데이터 정제 (pipeline/clean_trials.py)
  - 임상시험 기간을 시작월/종료월로 분리
  - 제목에서 진행상태 추출 및 정규화
  - 불필요한 컬럼 제거 및 데이터 클렌징
  - 출력: outputs/increment_YYYYMMDD_HHMMSS_clean.csv

3단계: Google Sheets 업데이트
  - 기존 데이터와 중복 확인 (clncTestSn 기준)
  - 새로운 데이터만 Google Sheets에 추가
  - 실행 결과 로깅 및 통계 리포트

4단계: 필터링된 시트 업데이트 (pipeline/sheets_filter.py)
  - 전체 구글 시트 데이터 기반으로 필터링
  - 진행상태별, 조건별 분류 시트 자동 생성
  - filtered_premium, filtered_recruiting, filtered_approved 시트 업데이트

사용법:
  python jobs/daily_update_2c.py [config/settings.yaml]

작성자: 자동화 시스템
최종 수정: 2025-09-12
"""

import os
import subprocess
import csv
from datetime import datetime, timezone, timedelta
import yaml

def run_command(cmd: list[str]) -> str:
    """
    서브프로세스로 명령어 실행 및 결과 반환
    
    Args:
        cmd (list[str]): 실행할 명령어와 인자들의 리스트
    
    Returns:
        str: 명령어 실행 결과 (stdout)
    
    Raises:
        subprocess.CalledProcessError: 명령어 실행 실패시
    """
    print(f"▶️ 실행: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"❌ 명령어 실행 실패: {e}")
        print(f"📋 오류 출력: {e.stderr}")
        raise

def open_worksheet(cfg):
    """
    Google Sheets 워크시트 열기 또는 생성
    
    config/sa.json의 서비스 계정 인증을 사용하여
    설정된 Google Sheets에 접근합니다.
    워크시트가 존재하지 않으면 자동으로 생성합니다.
    
    Args:
        cfg (dict): settings.yaml에서 로드된 설정 딕셔너리
                   - sheet_id: Google Sheets 문서 ID
                   - worksheet: 워크시트 탭 이름
                   - service_account_json: 인증 파일 경로
    
    Returns:
        gspread.Worksheet: Google Sheets 워크시트 객체
    """
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

def ensure_header(ws, header):
    """
    Google Sheets의 첫 번째 행에 헤더가 있는지 확인하고 설정
    
    헤더가 없으면 자동으로 추가하고, 
    기존 헤더와 다른 컬럼이 있으면 경고를 출력합니다.
    
    Args:
        ws (gspread.Worksheet): Google Sheets 워크시트 객체
        header (list[str]): 설정할 헤더 컬럼명 리스트
    """
    current_header = ws.row_values(1)
    if not current_header:
        ws.append_row(header)
        print(f"✅ 헤더 설정: {len(header)}개 컬럼")
        return
    
    missing = [h for h in header if h not in current_header]
    if missing:
        print(f"⚠️ 누락된 헤더 컬럼: {missing}")

def get_existing_sns(ws, key_col: str = "clncTestSn") -> set[str]:
    """
    Google Sheets에서 기존 임상시험 일련번호(clncTestSn) 목록 조회
    
    중복 데이터 방지를 위해 이미 시트에 존재하는 
    clncTestSn 값들을 set으로 반환합니다.
    
    Args:
        ws (gspread.Worksheet): Google Sheets 워크시트 객체
        key_col (str): 확인할 컬럼명 (기본값: 'clncTestSn')
    
    Returns:
        set[str]: 기존에 존재하는 clncTestSn 값들의 집합
                 오류 발생시 빈 set 반환
    """
    try:
        header = ws.row_values(1)
        if key_col not in header:
            return set()
        
        col_index = header.index(key_col) + 1
        values = ws.col_values(col_index)[1:]
        return set(v.strip() for v in values if v and str(v).strip())
    except Exception as e:
        print(f"⚠️ 기존 SN 목록 가져오기 실패: {e}")
        return set()

def sort_worksheet_by_clnc_sn(ws):
    """워크시트를 clncTestSn 컬럼 기준으로 오름차순 정렬"""
    try:
        # 헤더 행 제외하고 데이터 영역만 정렬
        ws.sort((1, 1), num_rows=ws.row_count)
        print("📊 clncTestSn 기준 정렬 적용됨")
    except Exception as e:
        print(f"⚠️ 정렬 실패: {e}")

def remove_status_dropdown(ws, header: list[str]):
    """진행상태 컬럼의 잘못된 드롭다운 속성 제거 (매일 업데이트되는 전체 임상시험 시트 전용)"""
    try:
        if "진행상태" not in header:
            return

        # 진행상태 컬럼 인덱스 찾기 (0-based)
        status_col_idx = header.index("진행상태")

        # 데이터 검증 규칙 제거 요청
        body = {
            "requests": [
                {
                    "setDataValidation": {
                        "range": {
                            "sheetId": ws.id,
                            "startRowIndex": 1,  # 헤더 제외
                            "endRowIndex": 1000,  # 충분히 큰 범위
                            "startColumnIndex": status_col_idx,
                            "endColumnIndex": status_col_idx + 1
                        },
                        "rule": None  # 검증 규칙 제거
                    }
                }
            ]
        }

        ws.spreadsheet.batch_update(body)
        print("✅ 진행상태 컬럼 드롭다운 속성 제거 완료")

    except Exception as e:
        print(f"⚠️ 진행상태 드롭다운 제거 실패: {e}")


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
        
        # 전체 컬럼에 대해 드롭다운 설정 (최대 1000행)
        range_name = f"{rowcol_to_a1(2, contact_col_idx)}:{rowcol_to_a1(1000, contact_col_idx)}"
        
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

def append_new_rows(ws, rows: list[dict], header: list[str]) -> int:
    """
    새로운 데이터 행들을 Google Sheets에 일괄 추가
    
    딕셔너리 형태의 데이터를 헤더 순서에 맞게 변환하여
    Google Sheets에 append_rows로 일괄 추가합니다.
    
    Args:
        ws (gspread.Worksheet): Google Sheets 워크시트 객체
        rows (list[dict]): 추가할 데이터 행들 (딕셔너리 리스트)
        header (list[str]): 컬럼 순서를 결정하는 헤더 리스트
    
    Returns:
        int: 실제로 추가된 행의 개수
    """
    if not rows:
        return 0
    
    values = []
    for row in rows:
        row_values = []
        for col in header:
            value = row.get(col, "")
            if value is None:
                value = ""
            row_values.append(str(value))
        values.append(row_values)
    
    ws.append_rows(values, value_input_option="RAW")
    return len(values)

def map_csv_to_sheet_format(csv_row: dict) -> dict:
    """
    정제된 CSV 데이터를 Google Sheets 형식으로 매핑
    
    clean_trials.py에서 정제된 CSV 데이터의 컬럼명을
    Google Sheets의 한글 헤더 형식에 맞게 변환합니다.
    누락된 값은 빈 문자열로 처리합니다.
    
    Args:
        csv_row (dict): CSV에서 읽어온 한 행의 데이터 (DictReader 결과)
    
    Returns:
        dict: Google Sheets 헤더에 맞게 매핑된 데이터 딕셔너리
    """
    clnc_sn = (csv_row.get("clncTestSn") or "").strip()
    
    return {
        # 시트의 실제 한글 헤더에 맞춤
        "clncTestSn": clnc_sn,
        "진행상태": csv_row.get("진행상태") or "",
        "임상시험명": csv_row.get("임상시험명") or "",
        "임상시험 의뢰자": csv_row.get("임상시험 의뢰자") or "",
        "소재지": csv_row.get("소재지") or "",
        "대상질환": csv_row.get("대상질환") or "",
        "대상질환명": csv_row.get("대상질환명") or "",
        "임상시험 단계": csv_row.get("임상시험 단계") or "",
        "임상시험 기간": csv_row.get("임상시험 기간") or "",
        "임상시험 시작월": csv_row.get("임상시험 시작월") or "",
        "임상시험 종료월": csv_row.get("임상시험 종료월") or "",
        "성별": csv_row.get("성별") or "",
        "나이": csv_row.get("나이") or "",
        "목표 대상자 수(국내)": csv_row.get("목표 대상자 수(국내)") or "",
        "임상시험 승인일자": csv_row.get("임상시험 승인일자") or "",
        "최근 변경일자": csv_row.get("최근 변경일자") or "",
        "이용문의": csv_row.get("이용문의") or "",
        "실시기관1": csv_row.get("실시기관1") or "",
        "실시기관2": csv_row.get("실시기관2") or "",
        "실시기관3": csv_row.get("실시기관3") or "",
        "실시기관4": csv_row.get("실시기관4") or "",
        "실시기관5": csv_row.get("실시기관5") or "",
        "조회수": csv_row.get("조회수") or "",
        "등록일자": csv_row.get("등록일자") or "",
    }

# 매일 업데이트되는 전체 임상시험 시트 헤더 (컨택상태 없음)
MAIN_SHEET_HEADER = [
    "clncTestSn", "진행상태", "임상시험명", "임상시험 의뢰자", "소재지",
    "대상질환", "대상질환명", "임상시험 단계", "임상시험 기간",
    "임상시험 시작월", "임상시험 종료월", "성별", "나이",
    "목표 대상자 수(국내)", "임상시험 승인일자", "최근 변경일자", "이용문의",
    "실시기관1", "실시기관2", "실시기관3", "실시기관4", "실시기관5",
    "조회수", "등록일자"
]

# filtered 시트들용 헤더 (컨택상태 포함)
FILTERED_SHEET_HEADER = [
    "clncTestSn", "컨택상태", "진행상태", "임상시험명", "임상시험 의뢰자", "소재지",
    "대상질환", "대상질환명", "임상시험 단계", "임상시험 기간",
    "임상시험 시작월", "임상시험 종료월", "성별", "나이",
    "목표 대상자 수(국내)", "임상시험 승인일자", "최근 변경일자", "이용문의",
    "실시기관1", "실시기관2", "실시기관3", "실시기관4", "실시기관5",
    "조회수", "등록일자"
]

def main(cfg_path="config/settings.yaml"):
    """
    매일 자동 업데이트의 메인 실행 함수
    
    3단계 파이프라인을 순차적으로 실행하고 결과를 로깅합니다:
    1. 증분 크롤링 (2c.py)
    2. 데이터 정제 (clean_trials.py)
    3. Google Sheets 업데이트
    
    Args:
        cfg_path (str): YAML 설정 파일 경로 (기본값: config/settings.yaml)
    
    Returns:
        int: 종료 코드 (0: 성공, 1: 실패)
    """
    print("🚀 자동화된 매일 업데이트 시작")
    print(f"⏰ 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 설정 로드
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        print(f"❌ 설정 파일 로드 실패: {e}")
        return 1
    
    try:
        # 1단계: 증분 크롤링
        # Google Sheets에서 마지막 clncTestSn을 확인하고
        # 그 이후의 새로운 임상시험 데이터만 수집
        print("📡 1단계: 2c.py 증분 크롤링")
        crawler_cmd = ["/Users/park/project/.venv/bin/python", "crawler/2c.py", "--cfg", cfg_path]
        raw_csv_output = run_command(crawler_cmd)
        
        # 2c.py 출력에서 CSV 파일 경로 추출 (마지막 줄)
        raw_csv_path = raw_csv_output.strip().split('\n')[-1]
        
        if not os.path.exists(raw_csv_path):
            print(f"❌ 2c.py 출력 파일을 찾을 수 없습니다: {raw_csv_path}")
            return 1
        
        print(f"✅ 원시 데이터 수집 완료: {raw_csv_path}")
        
        # 2단계: 데이터 정제 및 변환
        # 원시 크롤링 데이터를 분석 가능한 형태로 정제
        # - 임상시험 기간 → 시작월/종료월 분리
        # - 제목에서 진행상태 추출
        # - 불필요한 컬럼 제거 및 정규화
        print("\n🔧 2단계: 데이터 정제")
        clean_csv_path = raw_csv_path.replace(".csv", "_clean.csv")
        clean_cmd = [
            "/Users/park/project/.venv/bin/python", "pipeline/clean_trials.py", 
            "-i", raw_csv_path, 
            "-o", clean_csv_path
        ]
        run_command(clean_cmd)
        
        if not os.path.exists(clean_csv_path):
            print(f"❌ 정제된 파일을 찾을 수 없습니다: {clean_csv_path}")
            return 1
        
        print(f"✅ 데이터 정제 완료: {clean_csv_path}")
        
        # 3단계: Google Sheets 업데이트
        # 중복 체크 후 새로운 데이터만 시트에 추가
        # clncTestSn을 기준으로 중복 여부 판단
        print("\n📊 3단계: Google Sheets 업데이트")
        ws = open_worksheet(cfg)
        existing_sns = get_existing_sns(ws, "clncTestSn")
        print(f"📋 기존 데이터: {len(existing_sns)}개 항목")
        
        # 정제된 CSV에서 새 데이터만 필터링
        # 기존 Google Sheets에 없는 clncTestSn만 선별
        new_rows = []
        total_processed = 0
        
        with open(clean_csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_processed += 1
                mapped_row = map_csv_to_sheet_format(row)
                sn = mapped_row.get("clncTestSn", "").strip()
                
                if sn and sn not in existing_sns:
                    new_rows.append(mapped_row)
        
        print(f"📈 처리된 총 행 수: {total_processed}")
        print(f"🆕 새로운 데이터: {len(new_rows)}개")
        
        if new_rows:
            # 매일 업데이트되는 전체 임상시험 시트에는 컨택상태 컬럼 없음
            # 헤더 설정 (컨택상태 제외)
            ensure_header(ws, MAIN_SHEET_HEADER)
            added_count = append_new_rows(ws, new_rows, MAIN_SHEET_HEADER)
            print(f"✅ Google Sheets에 {added_count}개 행 추가됨")

            # 진행상태 컬럼의 잘못된 드롭다운 속성 제거
            print("🔧 진행상태 컬럼 드롭다운 속성 제거 중...")
            remove_status_dropdown(ws, MAIN_SHEET_HEADER)
            
            # 데이터 추가 후 clncTestSn 기준으로 정렬
            print("🔄 clncTestSn 기준으로 데이터 정렬 중...")
            sort_worksheet_by_clnc_sn(ws)
            print("✅ 데이터 정렬 완료")
            
            if new_rows:
                sample = new_rows[0]
                print(f"📄 샘플: {sample.get('title', '')[:50]}...")
        else:
            print("ℹ️ 추가할 새 데이터가 없습니다.")
        
        # 4단계: 필터링된 시트 업데이트
        # 새로운 데이터가 추가되었거나 매일 정기적으로 필터링 시트를 업데이트
        print("\n📂 4단계: 필터링된 시트 업데이트")
        try:
            filter_cmd = ["/Users/park/project/.venv/bin/python", "pipeline/sheets_filter.py", cfg_path]
            run_command(filter_cmd)
            print("✅ 필터링된 시트 업데이트 완료")
        except Exception as filter_error:
            print(f"⚠️ 필터링 시트 업데이트 실패: {filter_error}")
            # 필터링 실패해도 전체 파이프라인은 성공으로 처리
        
        print("\n" + "=" * 60)
        print("🎉 자동화 업데이트 완료!")
        print(f"📊 최종 결과: {len(new_rows)}개 새 항목 추가")
        print("📝 필터링된 시트도 업데이트됨")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ 업데이트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    import sys
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config/settings.yaml"
    exit_code = main(cfg_path)
    sys.exit(exit_code)