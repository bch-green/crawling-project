#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
임상시험 데이터 정제 및 변환 도구 (clean_trials.py)

이 스크립트는 원시 크롤링 데이터를 분석 가능한 형태로 정제하고 변환합니다.

주요 정제 기능:
1️⃣ 날짜 분리: '임상시험 기간' → '시작월', '종료월' (월/연 형식)
2️⃣ 상태 추출: 제목에서 '승인완료/모집중/모집완료/종료' 추출 → '진행상태' 컬럼
3️⃣ 컬럼 정리: '크롤링일시' 등 불필요 컬럼 완전 삭제
4️⃣ 기관명 정규화: 실시기관1~N에서 '병원/의원' 등만 유지, 담당자 정보 제거
5️⃣ 데이터 품질: 가비지 데이터 및 중복/비정상 행 제거
6️⃣ 인코딩 지원: UTF-8, CP949 등 자동 감지 및 처리

출력 형식:
- 정제된 CSV 파일 (UTF-8 인코딩, BOM 없이)
- 비두어 없는 일관된 컬럼명
- Google Sheets 업로드 준비 완료

사용법:
  python pipeline/clean_trials.py -i input.csv -o output_clean.csv [--backup]
  
옵션:
  -i, --input       입력 CSV 파일 경로
  -o, --output      출력 CSV 파일 경로  
  --backup          원본 파일 백업 생성

작성자: 데이터 정제 파이프라인
최종 수정: 2025-09-12
"""

import re
import argparse
import os

from datetime import datetime
from typing import Tuple
from pathlib import Path

import numpy as np
import pandas as pd


# =============================================================================
# 데이터 정제 설정
# =============================================================================

# 가비지 데이터로 간주되는 제목 패턴 (이 문자열이 포함된 행은 삭제)
GARBAGE_KEYS = [
    "임상시험 정보",      # 메뉴 제목
    "식약처 승인 목록",    # 링크 텍스트
    "목록으로",            # 내비게이션 링크
    "의약품 정보",         # 사이드바 메뉴
    "실시기관 정보",       # 서브 카테고리
    "대상자 선정기준",     # 상세 정보 제목
    "대상자 제외기준",     # 상세 정보 제목
    "연구설계 및 수행방법",  # 상세 정보 제목
]

# 핵심 필드 (이 값들이 모두 비어있으면 유효하지 않은 데이터로 간주)
CORE_FIELDS = [
    "임상시험 의뢰자",    # 연구 주체
    "임상시험 단계",      # 1상, 2상, 3상 등
    "임상시험 승인일자"    # 공식 승인 날짜
]

# 제목 맨 앞에서 진행상태를 추출하는 정규식 패턴
# 예시: [승인완료] 제목, (모집중) 제목, 종료 - 제목 등
STATUS_PATTERN = re.compile(
    r'^\s*'                    # 앞쪽 공백 무시
    r'[\[\(【]?'             # 선택적 여는 괄호: [, (, 【
    r'\s*'                     # 괄호 내부 공백
    r'(승인완료|모집중|모집완료|종료)'  # 상태 텍스트 (캡처 그룹)
    r'\s*'                     # 상태 뒤 공백
    r'[\]\)】]?'             # 선택적 닫는 괄호: ], ), 】
    r'\s*'                     # 괄호 뒤 공백
    r'[-–:·•]?'          # 선택적 구분자: -, –, :, ·, •
    r'\s*'                     # 구분자 뒤 공백
)

# 날짜 파싱을 위한 포맷 패턴
# 월/연 형식 (기본 사용)
YM_FMTS = [
    "%Y-%m",     # 2024-03 형식
    "%Y.%m",     # 2024.03 형식  
    "%Y/%m"      # 2024/03 형식
]

# 전체 날짜 형식 (월/연만 추출하고 일자는 무시)
DATE_FMTS = [
    "%Y-%m-%d",   # 2024-03-15 형식
    "%Y.%m.%d",   # 2024.03.15 형식
    "%Y/%m/%d"    # 2024/03/15 형식
]


# ---------------------------
# 유틸
# ---------------------------
def read_csv_any(path: str) -> pd.DataFrame:
    """utf-8-sig ↔ cp949 ↔ euc-kr 등 자동 시도"""
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path, dtype=str)


def write_csv(df: pd.DataFrame, path: str) -> None:
    # BOM 없이 저장하도록 수정
    df.to_csv(path, index=False, encoding="utf-8")  # utf-8-sig → utf-8로 변경


def looks_garbage_title(text: str) -> bool:
    """가비지(레이아웃/목록) 타이틀 감지"""
    if not isinstance(text, str) or not text.strip():
        return True
    t = text.strip()
    if sum(1 for k in GARBAGE_KEYS if k in t) >= 2:
        return True
    if len(t) > 300:
        return True
    return False


def is_dummy_row(row: pd.Series) -> bool:
    """더미(가짜) 행 판정"""
    # 1) 제목 가비지/없음
    if looks_garbage_title(row.get("임상시험명", np.nan)):
        return True
    # 2) 핵심 3필드 전부 공란
    empty_core = sum(
        1 for c in CORE_FIELDS
        if (not isinstance(row.get(c), str)) or (str(row.get(c)).strip() == "")
    )
    if empty_core >= len(CORE_FIELDS):
        return True
    # 3) 유효 데이터 개수 너무 적음(전체에서 비어있지 않은 값 3개 이하)
    non_null = (
        row.astype(str)
           .replace({"nan": ""})
           .str.strip()
           .replace("", np.nan)
           .notna()
           .sum()
    )
    return non_null <= 3


def trim_hospital_name(s: str) -> str:
    """값에서 병원명만 남기기: '...병원' 또는 보조적으로 '...의원'까지"""
    if not isinstance(s, str):
        return s
    t = s.strip().strip('"\''" ")
    if not t:
        return t
    m = re.search(r'(.+?병원)', t)
    if m:
        return m.group(1).strip()
    m2 = re.search(r'(.+?의원)', t)  # '의원'을 제외하려면 이 2줄 삭제
    if m2:
        return m2.group(1).strip()
    # 콤마 뒤 주소/직함 제거
    return t.split(",")[0].strip()


def extract_status_and_clean_title(title: str) -> Tuple[str, str]:
    """제목에서 진행상태를 분리하고, 제목에서는 상태 접두사를 제거"""
    if not isinstance(title, str):
        return "", title
    base = title.strip()
    m = STATUS_PATTERN.match(base)
    if m:
        status = m.group(1)
        clean = STATUS_PATTERN.sub("", base, count=1).strip()
        return status, clean
    return "", base


def _try_parse(fmt_list, s: str):
    for fmt in fmt_list:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None


def parse_month_or_year(raw: str):
    """
    단일 문자열을 'YYYY-MM' 또는 'YYYY'로 정규화.
    - 지원: YYYY-MM-DD / YYYY-MM / YYYY (+ '.', '/', 한글 'YYYY년 MM월', 'YYYY년')
    - 일(day)은 버리고 월까지만. 월이 없으면 연도만.
    """
    if not isinstance(raw, str):
        return pd.NA
    s = raw.strip()
    if not s:
        return pd.NA

    # 1) full date → YYYY-MM
    dt = _try_parse(DATE_FMTS, s)
    if dt:
        return dt.strftime("%Y-%m")

    # 2) year-month → YYYY-MM
    dt_ym = _try_parse(YM_FMTS, s)
    if dt_ym:
        return dt_ym.strftime("%Y-%m")

    # 3) 한글 포맷
    m = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월", s)
    if m:
        y, mo = map(int, m.groups())
        return f"{y:04d}-{mo:02d}"

    m = re.search(r"(\d{4})\s*년", s)
    if m:
        y = int(m.group(1))
        return f"{y:04d}"

    # 4) 숫자만 (YYYYMMDD/ YYYYMM/ YYYY)
    digits = re.sub(r"\D", "", s)
    if len(digits) >= 6:
        y = int(digits[:4]); mo = int(digits[4:6])
        return f"{y:04d}-{mo:02d}"
    elif len(digits) == 4:
        y = int(digits)
        return f"{y:04d}"

    return pd.NA


def parse_period_to_ym(val: str):
    """
    '임상시험 기간' → (시작월, 종료월)
    - 구분자: ~, -, – (양쪽 공백 허용)
    - 단일 값이면 종료월은 NA
    - 반환: 'YYYY-MM' 또는 'YYYY'
    """
    if not isinstance(val, str) or not val.strip():
        return (pd.NA, pd.NA)
    s = val.strip()
    parts = re.split(r"\s*[~\-–]\s*", s)
    if len(parts) >= 2:
        start_raw, end_raw = parts[0], parts[1]
        start = parse_month_or_year(start_raw)
        end   = parse_month_or_year(end_raw)
        return (start, end)
    start = parse_month_or_year(s)
    return (start, pd.NA)


# ---------------------------
# 메인 처리
# ---------------------------
def process(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    rep = {"rows_in": len(df), "cols_in": len(df.columns)}

    # 0) '임상시험 기간' → '임상시험 시작월', '임상시험 종료월' (원본 컬럼 바로 뒤에 삽입)
    if "임상시험 기간" in df.columns:
        start_end = df["임상시험 기간"].apply(parse_period_to_ym)
        start_col = "임상시험 시작월"
        end_col   = "임상시험 종료월"
        start_vals = start_end.apply(lambda x: x[0])
        end_vals   = start_end.apply(lambda x: x[1])

        # 임시로 추가 후 위치 재배치(원본 바로 뒤)
        df[start_col] = start_vals
        df[end_col]   = end_vals

        insert_at = df.columns.get_loc("임상시험 기간") + 1
        cols = df.columns.tolist()
        cols.remove(start_col)
        cols.remove(end_col)
        cols[insert_at:insert_at] = [start_col, end_col]
        df = df[cols]

    # 1) 진행상태 분리 + 제목 정리
    if "임상시험명" in df.columns:
        extracted = df["임상시험명"].fillna("").apply(extract_status_and_clean_title)
        df["진행상태"] = extracted.apply(lambda x: x[0])  # 새로 채움(기존 값 무시)
        df["임상시험명"] = extracted.apply(lambda x: x[1])

    # 2) '크롤링일시' 컬럼 완전 삭제
    if "크롤링일시" in df.columns:
        df.drop(columns=["크롤링일시"], inplace=True)

    # 3) 실시기관 정제
    site_cols = [c for c in df.columns if re.fullmatch(r"실시기관\d+", c)]
    for c in site_cols:
        df[c] = df[c].apply(trim_hospital_name)

    #   담당자/기타 컬럼 제거
    drop_cols = [c for c in df.columns if re.fullmatch(r"실시기관\d+_(담당자|기타)", c)]
    rep["drop_site_subcols"] = len(drop_cols)
    if drop_cols:
        df.drop(columns=drop_cols, inplace=True, errors="ignore")

    # 4) 더미 행 삭제
    mask_dummy = df.apply(is_dummy_row, axis=1)
    rep["dummy_rows_removed"] = int(mask_dummy.sum())
    df = df[~mask_dummy].reset_index(drop=True)

    rep["rows_out"] = len(df)
    rep["cols_out"] = len(df.columns)
    return df, rep


def main():
    ap = argparse.ArgumentParser(description="clinical_trials_full.csv 가공 스크립트 (최신 통합본)")
    ap.add_argument("-i", "--input", required=True, help="입력 CSV 경로 (예: clinical_trials_full.csv)")
    # 출력은 선택으로 변경: 미지정 시 자동 'outputs/clean/<입력이름>_clean.csv'
    ap.add_argument("-o", "--output", required=False, help="출력 CSV 경로 (미지정 시 자동 저장)")
    ap.add_argument("--backup", action="store_true", help="입력 파일 백업본도 함께 생성")
    args = ap.parse_args()

    src_path = Path(args.input)

    # 출력 경로 결정 로직
    if args.output:
        out_arg = Path(args.output)
        if out_arg.suffix.lower() == ".csv":
            out_path = out_arg
        else:
            # 디렉토리를 준 경우: 해당 디렉토리 아래에 <stem>_clean.csv로 저장
            out_dir = out_arg
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{src_path.stem}_clean.csv"
    else:
        # 기본 규칙: outputs/clean/<입력이름>_clean.csv
        out_dir = Path("outputs/clean")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{src_path.stem}_clean.csv"

    # 백업
    if args.backup:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        # 입력 파일과 같은 폴더에 백업본 생성
        backup_path = src_path.with_name(f"{src_path.stem}_backup_{ts}.csv")
        try:
            df_src = read_csv_any(str(src_path))
            write_csv(df_src, str(backup_path))
            print(f"[백업] {backup_path}")
        except Exception as e:
            print(f"[경고] 백업 실패: {e}")

    # 가공
    df = read_csv_any(str(src_path))
    df_out, rep = process(df)
    write_csv(df_out, str(out_path))

    print("[완료] 저장:", out_path)
    for k, v in rep.items():
        print(f" - {k}: {v}")


if __name__ == "__main__":
    pd.options.mode.copy_on_write = True  # pandas 2.x에서 성능/경고 완화
    main()
