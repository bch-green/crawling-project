#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
collect_missing.py
year_analysis.py에서 발견된 빠진 SN들을 자동으로 수집하는 스크립트
"""

import os
import sys
import time
import random
from typing import List
import pandas as pd

# 프로젝트 루트를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 2c.py에서 크롤러 클래스 import
try:
    from crawler.IncrementalClinicalTrialCrawler import IncrementalClinicalTrialCrawler
except ImportError:
    print("크롤러 클래스를 직접 import 할 수 없어서 subprocess로 실행합니다.")
    import subprocess

def get_missing_sns_from_analysis():
    """year_analysis.py 결과를 바탕으로 빠진 SN 목록 생성"""
    missing_sns = [
        # 2019년 (17개)
        201900065, 201900069, 201900071, 201900075, 201900077,
        201900086, 201900094, 201900131, 201900153, 201900177,
        201900181, 201900195, 201900225, 201900293, 201900317,
        201900436, 201900441,
        
        # 2020년 (16개)
        202000148, 202000188, 202000218, 202000255, 202000427,
        202000539, 202000540, 202000541, 202000542, 202000543,
        202000544, 202000545, 202000546, 202000717, 202000865,
        202001020,
        
        # 2021년 (9개)
        202100019, 202100099, 202100358, 202100368, 202100621,
        202100726, 202100768, 202100835, 202101041,
        
        # 2022년 (7개)
        202200044, 202200157, 202200420, 202200564, 202200567,
        202200598, 202200659,
        
        # 2023년 (5개)
        202300237, 202300525, 202300563, 202300844, 202300997,
        
        # 2024년 (12개)
        202400112, 202400283, 202400362, 202400363, 202400365,
        202400371, 202400498, 202400541, 202400637, 202400721,
        202400803, 202400932,
        
        # 2025년 (10개)
        202500089, 202500170, 202500189, 202500237, 202500267,
        202500301, 202500358, 202500421, 202500487, 202500589
    ]
    
    return sorted(missing_sns)

def collect_with_subprocess(sns_to_collect: List[int], output_dir: str = "outputs"):
    """subprocess로 2c.py를 호출해서 개별 SN 수집"""
    
    collected_data = []
    success_count = 0
    fail_count = 0
    
    print(f"수집할 SN 개수: {len(sns_to_collect)}")
    print("=" * 50)
    
    for i, sn in enumerate(sns_to_collect, 1):
        print(f"[{i}/{len(sns_to_collect)}] SN {sn} 수집 시도...")
        
        try:
            # 2c.py를 개별 SN으로 호출
            cmd = [
                "python", "crawler/2c.py",
                "--since-sn", str(sn - 1),
                "--limit", "1",
                "--cfg", "config/settings.yaml"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                # 성공한 경우 출력에서 CSV 파일 경로 찾기
                output_lines = result.stdout.strip().split('\n')
                csv_path = output_lines[-1] if output_lines else None
                
                if csv_path and os.path.exists(csv_path):
                    # CSV 파일 읽어서 데이터 확인
                    try:
                        df = pd.read_csv(csv_path)
                        if len(df) > 0 and str(df.iloc[0]['clncTestSn']) == str(sn):
                            print(f"  ✅ 성공: {df.iloc[0]['임상시험명'][:50]}...")
                            collected_data.append(csv_path)
                            success_count += 1
                        else:
                            print(f"  ❌ 다른 SN이 수집됨")
                            fail_count += 1
                    except Exception as e:
                        print(f"  ❌ CSV 읽기 실패: {e}")
                        fail_count += 1
                else:
                    print(f"  ❌ 출력 파일 없음")
                    fail_count += 1
            else:
                print(f"  ❌ 크롤링 실패: {result.stderr}")
                fail_count += 1
                
        except subprocess.TimeoutExpired:
            print(f"  ⏰ 타임아웃")
            fail_count += 1
        except Exception as e:
            print(f"  ❌ 오류: {e}")
            fail_count += 1
        
        # 요청 간 대기 (서버 부하 방지)
        if i < len(sns_to_collect):
            wait_time = random.uniform(2, 5)
            time.sleep(wait_time)
    
    print("=" * 50)
    print(f"수집 완료: 성공 {success_count}개, 실패 {fail_count}개")
    
    return collected_data

def merge_collected_data(csv_files: List[str], output_path: str = "outputs/missing_data_collected.csv"):
    """수집된 여러 CSV 파일을 하나로 합치기"""
    
    if not csv_files:
        print("합칠 CSV 파일이 없습니다.")
        return None
    
    all_data = []
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            all_data.append(df)
            print(f"병합: {csv_file} ({len(df)}행)")
        except Exception as e:
            print(f"파일 읽기 실패: {csv_file} - {e}")
    
    if all_data:
        merged_df = pd.concat(all_data, ignore_index=True)
        merged_df = merged_df.drop_duplicates(subset='clncTestSn', keep='first')
        
        # 정렬
        merged_df['clncTestSn_int'] = pd.to_numeric(merged_df['clncTestSn'], errors='coerce')
        merged_df = merged_df.sort_values('clncTestSn_int').drop('clncTestSn_int', axis=1)
        
        # 저장
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        print(f"✅ 병합 완료: {output_path} ({len(merged_df)}행)")
        return output_path
    
    return None

def update_main_csv(missing_data_csv: str, main_csv: str = "clinical_trials_full.csv"):
    """메인 CSV 파일에 누락 데이터 추가"""
    
    if not os.path.exists(missing_data_csv):
        print(f"누락 데이터 파일이 없습니다: {missing_data_csv}")
        return False
    
    if not os.path.exists(main_csv):
        print(f"메인 CSV 파일이 없습니다: {main_csv}")
        return False
    
    try:
        # 기존 데이터와 누락 데이터 로드
        main_df = pd.read_csv(main_csv, dtype={'clncTestSn': str})
        missing_df = pd.read_csv(missing_data_csv, dtype={'clncTestSn': str})
        
        print(f"기존 데이터: {len(main_df)}행")
        print(f"누락 데이터: {len(missing_df)}행")
        
        # 병합
        combined_df = pd.concat([main_df, missing_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset='clncTestSn', keep='first')
        
        # 정렬
        combined_df['clncTestSn_int'] = pd.to_numeric(combined_df['clncTestSn'], errors='coerce')
        combined_df = combined_df.sort_values('clncTestSn_int').drop('clncTestSn_int', axis=1)
        
        # 백업 생성
        backup_path = main_csv.replace('.csv', f'_backup_{int(time.time())}.csv')
        main_df.to_csv(backup_path, index=False, encoding='utf-8-sig')
        print(f"백업 생성: {backup_path}")
        
        # 메인 파일 업데이트
        combined_df.to_csv(main_csv, index=False, encoding='utf-8-sig')
        
        added_count = len(combined_df) - len(main_df)
        print(f"✅ 메인 CSV 업데이트 완료: {added_count}개 행 추가")
        print(f"총 데이터: {len(combined_df)}행")
        
        return True
        
    except Exception as e:
        print(f"❌ CSV 업데이트 실패: {e}")
        return False

def main():
    print("누락된 SN 자동 수집 시작")
    print("=" * 50)
    
    # 빠진 SN 목록 가져오기
    missing_sns = get_missing_sns_from_analysis()
    
    if not missing_sns:
        print("수집할 빠진 SN이 없습니다.")
        return
    
    print(f"총 {len(missing_sns)}개 SN 수집 예정")
    print(f"예상 소요 시간: {len(missing_sns) * 4 / 60:.1f}분")
    
    # 사용자 확인
    response = input("계속 진행하시겠습니까? (y/N): ")
    if response.lower() != 'y':
        print("취소되었습니다.")
        return
    
    # 수집 실행
    collected_files = collect_with_subprocess(missing_sns)
    
    if collected_files:
        # 수집된 데이터 병합
        merged_file = merge_collected_data(collected_files)
        
        if merged_file:
            # 메인 CSV에 추가 (선택사항)
            response = input("메인 CSV 파일에 추가하시겠습니까? (y/N): ")
            if response.lower() == 'y':
                update_main_csv(merged_file)
            
            print(f"\n✅ 수집 완료!")
            print(f"병합된 파일: {merged_file}")
    else:
        print("수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main()