#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
두 개 이상의 clinical_trials CSV 파일을 합병하는 스크립트

기능:
- 여러 CSV를 하나로 합침
- clncTestSn 기준 중복 제거
- 오름차순 정렬
"""

import pandas as pd

# ✅ 합칠 파일 경로 지정
files = [
    "clinical_trials_full_clean.csv",   # 첫 번째 CSV
    "crawling1.csv",   # 두 번째 CSV
]
output = "clinical_trials_merged.csv"

# CSV 로드 및 병합
dfs = [pd.read_csv(f, dtype=str) for f in files]
merged = pd.concat(dfs, ignore_index=True)

# 중복 제거 (clncTestSn 기준)
merged.drop_duplicates(subset="clncTestSn", keep="first", inplace=True)

# 정렬 (숫자형으로 변환 후 오름차순)
merged["clncTestSn_int"] = pd.to_numeric(merged["clncTestSn"], errors="coerce")
merged.sort_values(by="clncTestSn_int", inplace=True, ascending=True)
merged.drop(columns=["clncTestSn_int"], inplace=True)

# 저장
merged.to_csv(output, index=False, encoding="utf-8-sig")
print(f"✅ 합병 완료: {output}")
print(f"총 {len(merged)}개 항목")
