#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
개선된 시급성 임상시험 필터링 시스템
- 숫자 확대: 52건 → 100건
- 질환 다양성 확보: 강제 다양화 옵션
- 품질 필터링: 경고 수준별 선별
- 모집 압박 가중치 조정: 실제 시급성 반영
"""

import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python path에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
from datetime import datetime
import json

# 로컬 스코어링 시스템 임포트
from scoring.urgency_scoring import calculate_total_urgency_score, score_dataframe, WEIGHTS

class ImprovedUrgencyFilter:
    def __init__(self, csv_path: str = None):
        """개선된 시급성 필터 초기화"""
        if csv_path is None:
            csv_path = project_root / "ctfc.csv"
        
        self.csv_path = Path(csv_path)
        self.output_dir = project_root / "outputs" / "scoring_results_v2"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.df = None
        self.scored_df = None
        
        # 개선된 가중치 시나리오 추가
        self.custom_weights = {
            "balanced_urgent": {  # 모집 압박 강화
                "A": 0.25, "B": 0.35, "C": 0.25, "D": 0.15
            },
            "diversified": {      # 질환 다양성 위한 균형
                "A": 0.30, "B": 0.25, "C": 0.30, "D": 0.15  
            },
            "quality_first": {    # 품질 우선 (낮은 경고 가중)
                "A": 0.35, "B": 0.20, "C": 0.25, "D": 0.20
            }
        }
        
        print(f"🎯 개선된 필터 시작")
        print(f"📁 분석 대상: {self.csv_path}")
        print(f"📂 결과 저장: {self.output_dir}")
        
    def load_and_prepare_data(self):
        """데이터 로드 및 전처리"""
        print(f"\n📊 데이터 로딩...")
        
        if not self.csv_path.exists():
            raise FileNotFoundError(f"파일을 찾을 수 없습니다: {self.csv_path}")
        
        self.df = pd.read_csv(self.csv_path, encoding='utf-8-sig')
        
        print(f"✅ 로드 완료: {len(self.df):,}건")
        
        # 기본 통계
        status_counts = self.df['진행상태'].value_counts()
        print(f"\n📈 진행상태별 분포:")
        for status, count in status_counts.items():
            print(f"   {status}: {count:,}건")
            
        return self.df
    
    def apply_custom_scoring(self, weight_mode: str = "balanced_urgent"):
        """개선된 가중치로 스코어링"""
        print(f"\n🔬 개선된 스코어링 적용 (mode: {weight_mode})")
        
        # 기본 스코어링 먼저 실행
        self.scored_df = score_dataframe(
            self.df, 
            mode="auto", 
            current_date=datetime.now(),
            with_adjustment=True
        )
        
        # 커스텀 가중치가 있으면 재계산
        if weight_mode in self.custom_weights:
            print(f"🎛️ 커스텀 가중치 적용: {weight_mode}")
            weights = self.custom_weights[weight_mode]
            
            # 새로운 total_score_custom 계산
            self.scored_df['total_score_custom'] = (
                (self.scored_df['A'] / 30) * weights['A'] * 100 +
                (self.scored_df['B'] / 25) * weights['B'] * 100 +
                (self.scored_df['C'] / 25) * weights['C'] * 100 +
                (self.scored_df['D'] / 20) * weights['D'] * 100
            ).round(1)
            
            score_col = 'total_score_custom'
        else:
            score_col = 'total_score_adjusted'
        
        total_scores = self.scored_df[score_col]
        
        print(f"📊 {weight_mode} 스코어 분포:")
        print(f"   평균: {total_scores.mean():.1f}")
        print(f"   중간값: {total_scores.median():.1f}")
        print(f"   최고점: {total_scores.max():.1f}")
        print(f"   최저점: {total_scores.min():.1f}")
        
        return self.scored_df
    
    def extract_diversified_top_n(self, n: int = 100, quality_threshold: int = 15) -> pd.DataFrame:
        """질환 다양성을 고려한 상위 N건 추출"""
        
        print(f"\n🎯 다양성 고려 상위 {n}건 추출")
        print(f"📊 품질 임계값: 경고 {quality_threshold}개 이하")
        
        # 품질 필터링
        if 'warnings' in self.scored_df.columns:
            # 경고 개수 계산 (세미콜론으로 구분된 경고들)
            warning_counts = self.scored_df['warnings'].str.split(';').str.len()
            warning_counts = warning_counts.fillna(0)
            
            quality_filter = warning_counts <= quality_threshold
            filtered_df = self.scored_df[quality_filter].copy()
            
            print(f"📋 품질 필터링: {len(self.scored_df):,}건 → {len(filtered_df):,}건")
            print(f"   제외된 시험: {len(self.scored_df) - len(filtered_df):,}건")
        else:
            filtered_df = self.scored_df.copy()
        
        # 활성 상태만
        active_df = filtered_df[
            filtered_df['진행상태'].isin(['모집중', '승인완료'])
        ].copy()
        
        print(f"🟢 활성 상태: {len(active_df):,}건")
        
        # 점수 컬럼 결정
        score_col = 'total_score_custom' if 'total_score_custom' in active_df.columns else 'total_score_adjusted'
        
        # 질환 카테고리 분류
        active_df = self._categorize_diseases(active_df)
        
        # 카테고리별 분포 확인
        category_counts = active_df['disease_category'].value_counts()
        print(f"\n🏥 질환 카테고리별 분포:")
        for cat, count in category_counts.items():
            print(f"   {cat}: {count:,}건")
        
        # 다양성 할당 전략
        target_distribution = self._calculate_target_distribution(n, category_counts)
        
        print(f"\n🎯 목표 분배:")
        for cat, target in target_distribution.items():
            print(f"   {cat}: {target}건")
        
        # 카테고리별 선별
        selected_trials = []
        remaining_slots = n
        
        for category, target_count in target_distribution.items():
            category_df = active_df[active_df['disease_category'] == category]
            
            if len(category_df) == 0:
                continue
                
            # 해당 카테고리에서 상위 N건 선택
            actual_count = min(target_count, len(category_df), remaining_slots)
            
            if actual_count > 0:
                top_category = category_df.nlargest(actual_count, score_col)
                selected_trials.append(top_category)
                remaining_slots -= actual_count
                
                print(f"   ✅ {category}: {actual_count}건 선택 (목표: {target_count})")
        
        # 합치기
        if selected_trials:
            result_df = pd.concat(selected_trials, ignore_index=False)
        else:
            result_df = pd.DataFrame()
        
        # 부족하면 전체에서 추가 선별
        if len(result_df) < n and remaining_slots > 0:
            print(f"\n🔄 부족분 보충: {remaining_slots}건")
            excluded_indices = result_df.index
            remaining_df = active_df[~active_df.index.isin(excluded_indices)]
            
            additional = remaining_df.nlargest(remaining_slots, score_col)
            if len(additional) > 0:
                result_df = pd.concat([result_df, additional])
        
        # 최종 정렬
        result_df = result_df.sort_values(score_col, ascending=False).head(n)
        
        print(f"\n✅ 최종 선별: {len(result_df)}건")
        print(f"   점수 범위: {result_df[score_col].min():.1f} ~ {result_df[score_col].max():.1f}")
        
        return result_df
    
    def _categorize_diseases(self, df: pd.DataFrame) -> pd.DataFrame:
        """질환 카테고리 분류"""
        df = df.copy()
        df['disease_category'] = 'Others'
        
        # 질환명 기준 분류
        disease_name = df['대상질환명'].fillna('').str.lower()
        
        # 카테고리 정의 (우선순위 순서)
        categories = {
            'Cancer': ['암', '종양', 'cancer', 'tumor', '악성', 'malign', 'oncol', 'carcinoma', 'lymphoma', 'leukemia'],
            'Cardiovascular': ['심', '혈관', '고혈압', 'cardio', 'vascular', 'hypertension', '심근', '심장'],
            'Neurological': ['신경', '뇌', '치매', 'neuro', 'brain', 'dementia', 'parkinson', '파킨슨', 'alzheimer'],
            'Immunology': ['면역', '류마티스', 'immun', 'rheumat', '자가면역', 'autoimmune'],
            'Endocrine': ['당뇨', '내분비', 'diabetes', 'endocrin', '갑상선', 'thyroid', '호르몬'],
            'Respiratory': ['호흡', '폐', 'lung', 'respiratory', '천식', 'asthma', 'copd'],
            'Rare_Disease': ['희귀', 'rare', 'orphan'],
            'Infectious': ['감염', 'infection', '바이러스', 'virus', '세균', 'bacteria']
        }
        
        # 카테고리별 분류
        for category, keywords in categories.items():
            mask = disease_name.str.contains('|'.join(keywords), na=False, case=False)
            df.loc[mask, 'disease_category'] = category
        
        return df
    
    def _calculate_target_distribution(self, total_n: int, category_counts: pd.Series) -> dict:
        """목표 분배 계산 (다양성 확보)"""
        
        # 기본 할당: 실제 분포 기반이지만 최소/최대 제한
        min_per_category = max(1, total_n // 20)  # 최소 5% (100건 기준 5건)
        max_per_category = total_n // 2  # 최대 50%
        
        total_available = category_counts.sum()
        
        target_distribution = {}
        allocated = 0
        
        # 1차: 비례 할당 (제한 적용)
        for category, count in category_counts.items():
            if count == 0:
                continue
                
            # 비례 계산
            proportion = count / total_available
            raw_allocation = int(total_n * proportion)
            
            # 제한 적용
            if category == 'Cancer':
                # 암은 최대 70%까지 허용 (원래 92%에서 감소)
                allocation = min(raw_allocation, int(total_n * 0.7))
            elif count < min_per_category:
                allocation = min(count, min_per_category)
            else:
                allocation = min(max(raw_allocation, min_per_category), max_per_category)
            
            allocation = min(allocation, count)  # 실제 데이터 수를 초과할 수 없음
            
            target_distribution[category] = allocation
            allocated += allocation
        
        # 2차: 부족분/초과분 조정
        remaining = total_n - allocated
        
        if remaining > 0:
            # 부족분을 가장 큰 카테고리들에 분배
            sorted_categories = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
            
            for category, available in sorted_categories:
                if remaining <= 0:
                    break
                    
                current = target_distribution.get(category, 0)
                can_add = min(remaining, available - current)
                
                if can_add > 0:
                    target_distribution[category] = current + can_add
                    remaining -= can_add
        
        elif remaining < 0:
            # 초과분을 가장 큰 카테고리에서 감소
            over_allocated = -remaining
            largest_category = max(target_distribution.items(), key=lambda x: x[1])
            target_distribution[largest_category[0]] -= over_allocated
        
        return target_distribution
    
    def generate_improved_report(self, top_n_df: pd.DataFrame, n: int = 100) -> dict:
        """개선된 리포트 생성"""
        
        score_col = 'total_score_custom' if 'total_score_custom' in top_n_df.columns else 'total_score_adjusted'
        
        report = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_trials_analyzed": len(self.df),
                "active_trials": len(self.scored_df[
                    self.scored_df['진행상태'].isin(['모집중', '승인완료'])
                ]),
                "selection_count": n,
                "improvement_version": "v2.0"
            },
            "summary": {
                "total_count": len(top_n_df),
                "avg_score": round(top_n_df[score_col].mean(), 1),
                "median_score": round(top_n_df[score_col].median(), 1),
                "score_range": [
                    round(top_n_df[score_col].min(), 1),
                    round(top_n_df[score_col].max(), 1)
                ]
            },
            "diversity": {
                "disease_categories": top_n_df['disease_category'].value_counts().to_dict(),
                "category_percentages": (top_n_df['disease_category'].value_counts() / len(top_n_df) * 100).round(1).to_dict()
            },
            "breakdown": {
                "status": top_n_df['진행상태'].value_counts().to_dict(),
                "phases": top_n_df['임상시험 단계'].value_counts().head(10).to_dict(),
                "score_components": {
                    "A_상태_중요도": round(top_n_df['A'].mean(), 1),
                    "B_모집_압박": round(top_n_df['B'].mean(), 1), 
                    "C_모집_난이도": round(top_n_df['C'].mean(), 1),
                    "D_시간_민감도": round(top_n_df['D'].mean(), 1)
                }
            }
        }
        
        # 품질 분석
        if 'warnings' in top_n_df.columns:
            warning_counts = top_n_df['warnings'].str.split(';').str.len().fillna(0)
            report["quality"] = {
                "trials_with_warnings": int((warning_counts > 0).sum()),
                "avg_warnings_per_trial": round(warning_counts.mean(), 2),
                "max_warnings": int(warning_counts.max()),
                "quality_improvement": "경고 15개 이하 필터링 적용"
            }
        
        return report
    
    def save_improved_results(self, top_n_df: pd.DataFrame, report: dict, 
                            strategy: str = "diversified", n: int = 100):
        """개선된 결과 저장"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1) CSV 저장
        csv_file = self.output_dir / f"urgent_trials_top{n}_{strategy}_{timestamp}.csv"
        top_n_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        
        # 2) 리포트 JSON 저장
        report_file = self.output_dir / f"urgency_report_top{n}_{strategy}_{timestamp}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        # 3) 개선된 요약 리포트
        summary_file = self.output_dir / f"urgency_summary_top{n}_{strategy}_{timestamp}.txt"
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write(f"🎯 개선된 시급성 임상시험 {n}건 선별 결과\n")
            f.write("=" * 70 + "\n\n")
            
            f.write(f"📊 메타데이터:\n")
            f.write(f"  - 분석 시간: {report['metadata']['generated_at']}\n")
            f.write(f"  - 개선 버전: {report['metadata']['improvement_version']}\n")
            f.write(f"  - 전체 분석: {report['metadata']['total_trials_analyzed']:,}건\n")
            f.write(f"  - 활성 상태: {report['metadata']['active_trials']:,}건\n")
            f.write(f"  - 최종 선별: {report['summary']['total_count']}건\n\n")
            
            f.write(f"📈 스코어 요약:\n")
            f.write(f"  - 평균 점수: {report['summary']['avg_score']}점\n")
            f.write(f"  - 점수 범위: {report['summary']['score_range'][0]} ~ {report['summary']['score_range'][1]}점\n\n")
            
            f.write("🎯 질환 다양성 (개선됨):\n")
            for category, count in report['diversity']['disease_categories'].items():
                percentage = report['diversity']['category_percentages'][category]
                f.write(f"  - {category}: {count}건 ({percentage}%)\n")
            
            f.write(f"\n📋 진행상태별:\n")
            for status, count in report['breakdown']['status'].items():
                f.write(f"  - {status}: {count}건\n")
            
            f.write(f"\n🏥 임상시험 단계별:\n")
            for phase, count in list(report['breakdown']['phases'].items())[:5]:
                f.write(f"  - {phase}: {count}건\n")
            
            if 'quality' in report:
                f.write(f"\n⚡ 품질 개선:\n")
                f.write(f"  - {report['quality']['quality_improvement']}\n")
                f.write(f"  - 평균 경고: {report['quality']['avg_warnings_per_trial']}개/건\n")
                f.write(f"  - 최대 경고: {report['quality']['max_warnings']}개\n")
        
        print(f"\n💾 개선된 결과 저장:")
        print(f"  📄 {n}건 CSV: {csv_file.name}")
        print(f"  📋 리포트: {report_file.name}")
        print(f"  📝 요약: {summary_file.name}")
        
        return csv_file, report_file, summary_file

def main():
    """개선된 메인 실행"""
    import argparse
    
    parser = argparse.ArgumentParser(description="개선된 시급성 임상시험 필터링")
    parser.add_argument("--csv", default="ctfc.csv", help="분석할 CSV 파일")
    parser.add_argument("--count", type=int, default=100, help="선별할 시험 수 (기본: 100)")
    parser.add_argument("--weight-mode", default="balanced_urgent", 
                       choices=["balanced_urgent", "diversified", "quality_first"],
                       help="가중치 모드")
    parser.add_argument("--quality-threshold", type=int, default=15, 
                       help="품질 임계값 (경고 개수, 기본: 15)")
    
    args = parser.parse_args()
    
    print("🚀 개선된 시급성 임상시험 필터링 시작")
    print("=" * 70)
    print(f"🎯 선별 목표: {args.count}건")
    print(f"⚖️ 가중치 모드: {args.weight_mode}")
    print(f"📊 품질 임계값: 경고 {args.quality_threshold}개 이하")
    
    try:
        # 1) 필터 시스템 초기화
        filter_system = ImprovedUrgencyFilter(args.csv)
        
        # 2) 데이터 로드
        filter_system.load_and_prepare_data()
        
        # 3) 개선된 스코어링
        filter_system.apply_custom_scoring(weight_mode=args.weight_mode)
        
        # 4) 다양성 고려 선별
        top_n = filter_system.extract_diversified_top_n(
            n=args.count, 
            quality_threshold=args.quality_threshold
        )
        
        # 5) 리포트 생성
        report = filter_system.generate_improved_report(top_n, n=args.count)
        
        # 6) 결과 저장
        csv_file, report_file, summary_file = filter_system.save_improved_results(
            top_n, report, strategy=args.weight_mode, n=args.count
        )
        
        # 7) 요약 출력
        print(f"\n🎉 개선된 처리 완료!")
        print(f"📊 전체 분석: {len(filter_system.df):,}건")
        print(f"🎯 최종 선별: {len(top_n)}건")
        print(f"📈 평균 점수: {report['summary']['avg_score']}점")
        
        print(f"\n🌈 질환 다양성:")
        for cat, count in report['diversity']['disease_categories'].items():
            pct = report['diversity']['category_percentages'][cat]
            print(f"   {cat}: {count}건 ({pct}%)")
        
        print(f"\n📋 진행상태:")
        for status, count in report['breakdown']['status'].items():
            print(f"   {status}: {count}건")
        
        return True
        
    except Exception as e:
        print(f"❌ 실행 중 오류: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)