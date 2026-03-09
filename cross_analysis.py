"""
통합 트렌드 분석 모듈 v2 — 키워드 레벨 교차 검증

[핵심 변경사항 vs v1]
- v1: 카테고리/원료 그룹 단위 분석 → 인사이트가 추상적
- v2: 개별 키워드 단위 분석 → "진짜 뜨고 있는 키워드"를 발굴

[분석 흐름]
1. 홈쇼핑모아 인기 상품에서 트렌딩 키워드를 추출
2. 각 키워드를 네이버 검색 트렌드로 검증 (개별 검색량 추이)
3. 네이버 연관 검색어로 확장 → 추가 트렌드 키워드 발굴
4. 교차 검증된 키워드들을 우리 SKU와 대조 → 구체적 추가 제안

[핵심 철학]
"카테고리가 아닌 키워드 레벨에서 트렌드를 보고,
 연관 검색어까지 분석해서 진짜 뜨고 있는 제품/원료를 발견한다"
"""

import pandas as pd
from typing import Dict, List, Optional, Set
from datetime import datetime

from sku_enrichment import (
    enriched_keyword_match,
    enriched_keyword_check,
    expand_keyword_synonyms,
    normalize_keyword,
    KEYWORD_SYNONYMS,
)

from naver_keyword_api import (
    is_api_configured,
    fetch_multiple_keyword_trends,
    analyze_keyword_trend,
    batch_fetch_related_keywords,
    filter_health_related_keywords,
    generate_demo_keyword_trends,
    generate_demo_related_keywords,
)


# ──────────────────────────────────────
# 1. 홈쇼핑모아에서 트렌딩 키워드 추출
# ──────────────────────────────────────

def extract_hsmoa_keywords(products_df: pd.DataFrame) -> List[dict]:
    """
    홈쇼핑모아 상품에서 트렌딩 키워드를 추출한다.

    [추출 소스]
    1. 검색 키워드: 각 상품이 어떤 검색어로 발견되었는지
    2. 상품명 성분 스캔: 알려진 원료/성분 키워드 탐색
    3. 인기 브랜드 (보조)

    Returns:
        [
            {
                "keyword": "유산균",
                "canonical": "프로바이오틱스",
                "source_type": "search_keyword" | "product_ingredient",
                "hsmoa_product_count": 15,
                "hsmoa_brands": ["종근당건강", "덴프스", ...],
                "hsmoa_products": [{"name": ..., "brand": ..., "price": ...}, ...],
                "hsmoa_avg_price": 50000,
                "hsmoa_avg_review": 3.5,
            },
            ...
        ]
    """
    if products_df.empty:
        return []

    # 키워드별 상품 데이터 수집
    keyword_products = {}  # canonical → {products, brands, pdids, ...}

    for _, product in products_df.iterrows():
        name_lower = str(product.get("name", "")).lower()
        search_kw = str(product.get("keyword", "")).lower().strip()
        pdid = product.get("pdid", "")

        found_keywords = set()

        # 방법 1: 검색 키워드에서 추출
        if search_kw and len(search_kw) >= 2:
            canonical = normalize_keyword(search_kw)
            if canonical and len(canonical) >= 2:
                found_keywords.add((canonical, "search_keyword"))

        # 방법 2: 상품명에서 알려진 성분 스캔
        for canonical_key, variants in KEYWORD_SYNONYMS.items():
            for variant in variants:
                v_lower = variant.lower()
                if len(v_lower) >= 2 and v_lower in name_lower:
                    found_keywords.add((canonical_key, "product_ingredient"))
                    break

        # 너무 일반적인 키워드 제외
        skip_set = {
            "건강", "식품", "추천", "영양", "영양제", "건강식품",
            "헬스", "보조", "기능", "비타민", "미네랄",
        }

        for (kw, source_type) in found_keywords:
            if kw in skip_set:
                continue

            if kw not in keyword_products:
                keyword_products[kw] = {
                    "source_types": set(),
                    "products": [],
                    "brands": set(),
                    "prices": [],
                    "reviews": [],
                    "pdids": set(),
                }

            data = keyword_products[kw]
            data["source_types"].add(source_type)

            if pdid in data["pdids"]:
                continue
            data["pdids"].add(pdid)

            data["products"].append({
                "name": product.get("name", ""),
                "brand": product.get("brand", ""),
                "price": product.get("sale_price", 0),
                "review_rating": product.get("review_rating", 0),
                "review_count": product.get("review_count", 0),
            })

            brand = product.get("brand", "")
            if brand:
                data["brands"].add(brand)

            price = product.get("sale_price", 0)
            if price > 0:
                data["prices"].append(price)

            review = product.get("review_rating", 0)
            if review > 0:
                data["reviews"].append(review)

    # 결과 정리 (상품 2개 이상인 키워드만)
    result = []
    for kw, data in keyword_products.items():
        count = len(data["pdids"])
        if count < 1:
            continue

        # 상품을 리뷰수 기준으로 정렬
        sorted_products = sorted(
            data["products"],
            key=lambda x: x.get("review_count", 0),
            reverse=True,
        )

        # 대표 source_type 결정
        if "search_keyword" in data["source_types"]:
            source_type = "search_keyword"
        else:
            source_type = "product_ingredient"

        result.append({
            "keyword": kw,
            "canonical": kw,
            "source_type": source_type,
            "hsmoa_product_count": count,
            "hsmoa_brands": sorted(data["brands"]),
            "hsmoa_products": sorted_products[:10],
            "hsmoa_avg_price": int(sum(data["prices"]) / len(data["prices"])) if data["prices"] else 0,
            "hsmoa_avg_review": round(sum(data["reviews"]) / len(data["reviews"]), 1) if data["reviews"] else 0,
        })

    # 상품 수 기준 내림차순 정렬
    result.sort(key=lambda x: x["hsmoa_product_count"], reverse=True)
    return result


# ──────────────────────────────────────
# 2. 네이버 트렌드 검증 + 연관 검색어 확장
# ──────────────────────────────────────

def validate_and_expand_keywords(
    hsmoa_keywords: List[dict],
    use_api: bool = True,
) -> dict:
    """
    홈쇼핑모아 키워드를 네이버 검색 트렌드로 검증하고,
    연관 검색어를 통해 추가 트렌드 키워드를 발굴한다.

    Args:
        hsmoa_keywords: extract_hsmoa_keywords()의 결과
        use_api: True=실제 API, False=데모 데이터

    Returns:
        {
            "keyword_trends": {
                "키워드": { trend_direction, growth_rate, momentum_score, ... },
                ...
            },
            "related_keywords": {
                "키워드": ["연관1", "연관2", ...],
                ...
            },
            "related_trends": {
                "연관키워드": { trend_direction, growth_rate, ... },
                ...
            },
            "is_demo": bool,
        }
    """
    keyword_list = [kw["keyword"] for kw in hsmoa_keywords]

    # 동의어 맵 생성 (네이버 검색량 합산용)
    keywords_with_synonyms = {}
    for kw_info in hsmoa_keywords:
        kw = kw_info["keyword"]
        syns = list(expand_keyword_synonyms(kw))
        keywords_with_synonyms[kw] = syns

    if use_api and is_api_configured():
        is_demo = False

        # (1) 개별 키워드 트렌드 조회
        raw_trends = fetch_multiple_keyword_trends(keywords_with_synonyms, months_back=6)
        keyword_trends = {}
        for kw, df in raw_trends.items():
            keyword_trends[kw] = analyze_keyword_trend(df)

        # (2) 연관 검색어 수집
        related_keywords = batch_fetch_related_keywords(keyword_list, max_per_keyword=8)

        # (3) 건강 관련 연관 검색어만 필터 + 이미 분석한 키워드 제외
        all_related_flat = set()
        for kw, related_list in related_keywords.items():
            health_related = filter_health_related_keywords(related_list)
            related_keywords[kw] = health_related
            for r in health_related:
                r_canonical = normalize_keyword(r)
                if r_canonical not in keyword_trends:
                    all_related_flat.add(r)

        # (4) 주요 연관 키워드의 트렌드도 조회 (상위 15개만)
        related_to_check = list(all_related_flat)[:15]
        related_with_synonyms = {}
        for r_kw in related_to_check:
            syns = list(expand_keyword_synonyms(r_kw))
            related_with_synonyms[r_kw] = syns

        related_raw_trends = fetch_multiple_keyword_trends(
            related_with_synonyms, months_back=6,
        )
        related_trends = {}
        for kw, df in related_raw_trends.items():
            related_trends[kw] = analyze_keyword_trend(df)

    else:
        is_demo = True
        # 데모 데이터
        demo_trends = generate_demo_keyword_trends(keyword_list)
        keyword_trends = {}
        for kw, df in demo_trends.items():
            keyword_trends[kw] = analyze_keyword_trend(df)

        related_keywords = generate_demo_related_keywords(keyword_list)
        # 건강 관련만 필터
        for kw in related_keywords:
            related_keywords[kw] = filter_health_related_keywords(
                related_keywords[kw]
            )

        # 연관 키워드 데모 트렌드
        all_related = set()
        for related_list in related_keywords.values():
            all_related.update(related_list)
        related_demo = generate_demo_keyword_trends(list(all_related)[:15])
        related_trends = {}
        for kw, df in related_demo.items():
            related_trends[kw] = analyze_keyword_trend(df)

    return {
        "keyword_trends": keyword_trends,
        "related_keywords": related_keywords,
        "related_trends": related_trends,
        "is_demo": is_demo,
    }


# ──────────────────────────────────────
# 3. 종합 분석 → 트렌드 키워드 리포트
# ──────────────────────────────────────

def generate_keyword_trend_report(
    hsmoa_keywords: List[dict],
    naver_results: dict,
    sku_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    키워드 레벨의 종합 트렌드 리포트 생성

    [각 키워드에 대해]
    1. 홈쇼핑 인기도 (상품수, 브랜드, 가격)
    2. 네이버 검색 트렌드 (방향, 성장률, 모멘텀)
    3. 연관 검색어에서 발견된 추가 시그널
    4. 우리 SKU 보유 현황
    5. 최종 추천

    Returns:
        DataFrame — 키워드별 종합 분석 결과
    """
    if not hsmoa_keywords or sku_df.empty:
        return pd.DataFrame()

    keyword_trends = naver_results.get("keyword_trends", {})
    related_keywords = naver_results.get("related_keywords", {})
    related_trends = naver_results.get("related_trends", {})

    rows = []

    for kw_info in hsmoa_keywords:
        kw = kw_info["keyword"]
        hsmoa_count = kw_info["hsmoa_product_count"]
        hsmoa_brands = kw_info["hsmoa_brands"]
        hsmoa_avg_price = kw_info["hsmoa_avg_price"]

        # 네이버 트렌드
        naver = keyword_trends.get(kw, {})
        naver_direction = naver.get("trend_direction", "데이터없음")
        naver_growth = naver.get("growth_rate", 0)
        naver_momentum = naver.get("momentum_score", 0)
        naver_latest = naver.get("latest_ratio", 0)
        naver_data = naver.get("trend_data", [])

        # 트렌드 아이콘
        direction_icons = {
            "급상승": "🔺🔺", "상승": "🔺", "유지": "➡️",
            "하락": "🔻", "급하락": "🔻🔻", "데이터없음": "—",
        }
        direction_display = "{} {}".format(
            direction_icons.get(naver_direction, ""),
            naver_direction,
        )

        # 연관 검색어 정보
        related = related_keywords.get(kw, [])
        # 연관 검색어 중 상승 중인 것만 뽑기
        rising_related = []
        for r_kw in related:
            r_trend = related_trends.get(r_kw, {})
            r_dir = r_trend.get("trend_direction", "")
            if r_dir in ("급상승", "상승"):
                rising_related.append(r_kw)

        related_text = ", ".join(related[:5]) if related else "-"
        rising_related_text = ", ".join(rising_related[:3]) if rising_related else "-"

        # SKU 보유 현황
        sku_info = enriched_keyword_check(kw, sku_df)
        our_count = sku_info["matched_count"]
        our_products = sku_info["matched_products"]

        # ── 종합 점수 산출 ──
        # 홈쇼핑 인기도 (0~40)
        hsmoa_score = min(hsmoa_count * 3, 40)
        # 네이버 모멘텀 (0~40)
        naver_score = min(naver_momentum * 0.4, 40)
        # 연관 검색어 상승 보너스 (0~20)
        related_bonus = min(len(rising_related) * 5, 20)
        combined_score = round(hsmoa_score + naver_score + related_bonus, 1)

        # ── 교차 검증 신뢰도 ──
        has_hsmoa = hsmoa_count >= 2
        has_naver = naver_direction in ("급상승", "상승")
        has_related_signal = len(rising_related) >= 1

        if has_hsmoa and has_naver and has_related_signal:
            confidence = "🔴 확실"
            confidence_sort = 0
        elif has_hsmoa and has_naver:
            confidence = "🟠 높음"
            confidence_sort = 1
        elif has_hsmoa and naver_direction not in ("하락", "급하락", "데이터없음"):
            confidence = "🟡 중간"
            confidence_sort = 2
        elif has_hsmoa or has_naver:
            confidence = "🔵 참고"
            confidence_sort = 3
        else:
            confidence = "⚪ 약함"
            confidence_sort = 4

        # ── SKU 추천 행동 ──
        if our_count == 0:
            action = "🔴 신규 도입"
            action_sort = 0
        elif our_count <= 2:
            action = "🟠 라인업 강화"
            action_sort = 1
        elif our_count <= 5:
            action = "🟡 추가 검토"
            action_sort = 2
        else:
            action = "✅ 충분"
            action_sort = 3

        # ── 추천 브랜드 (우리가 안 갖고 있는 홈쇼핑 인기 브랜드) ──
        suggested_brands = []
        for brand in hsmoa_brands[:8]:
            if len(brand) >= 2:
                brand_match = enriched_keyword_match(brand, sku_df)
                if len(brand_match) == 0:
                    suggested_brands.append(brand)

        rows.append({
            "키워드": kw,
            "신뢰도": confidence,
            "종합점수": combined_score,
            "추천행동": action,
            # 홈쇼핑
            "홈쇼핑_상품수": hsmoa_count,
            "홈쇼핑_브랜드": ", ".join(hsmoa_brands[:3]),
            "홈쇼핑_평균가격": hsmoa_avg_price,
            # 네이버
            "네이버_트렌드": direction_display,
            "네이버_성장률": naver_growth,
            "네이버_성장률_표시": "{:+.1f}%".format(naver_growth) if naver_growth != 0 else "-",
            "네이버_모멘텀": naver_momentum,
            "네이버_검색량": naver_latest,
            # 연관 검색어
            "연관검색어": related_text,
            "상승_연관검색어": rising_related_text,
            "연관_상승_수": len(rising_related),
            # SKU
            "보유SKU수": our_count,
            "보유현황": "{}개".format(our_count) if our_count > 0 else "미보유",
            "보유_상품": " / ".join(our_products[:3]) if our_products else "-",
            "추천_브랜드": ", ".join(suggested_brands[:3]) if suggested_brands else "-",
            # 정렬용
            "_confidence_sort": confidence_sort,
            "_action_sort": action_sort,
            # 차트용
            "_trend_data": naver_data,
        })

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    result = result.sort_values(
        ["_action_sort", "_confidence_sort", "종합점수"],
        ascending=[True, True, False],
    ).reset_index(drop=True)

    return result


# ──────────────────────────────────────
# 4. 연관 검색어 확장 리포트
# ──────────────────────────────────────

def generate_related_keyword_report(
    naver_results: dict,
    sku_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    연관 검색어에서 발견된 추가 트렌드 키워드 리포트

    홈쇼핑모아에는 없지만, 네이버 연관 검색어 분석에서 발견된
    상승 트렌드 키워드를 보여준다.

    Returns:
        DataFrame — 연관 키워드별 분석 결과
    """
    related_keywords = naver_results.get("related_keywords", {})
    related_trends = naver_results.get("related_trends", {})
    keyword_trends = naver_results.get("keyword_trends", {})

    if not related_trends:
        return pd.DataFrame()

    rows = []
    seen = set()

    for parent_kw, related_list in related_keywords.items():
        for r_kw in related_list:
            if r_kw in seen:
                continue
            seen.add(r_kw)

            # 이미 메인 키워드로 분석된 것은 제외
            r_canonical = normalize_keyword(r_kw)
            if r_canonical in keyword_trends:
                continue

            r_trend = related_trends.get(r_kw, {})
            if not r_trend:
                continue

            r_direction = r_trend.get("trend_direction", "데이터없음")
            r_growth = r_trend.get("growth_rate", 0)
            r_momentum = r_trend.get("momentum_score", 0)

            # 상승/급상승만
            if r_direction not in ("급상승", "상승"):
                continue

            # SKU 보유 확인
            sku_info = enriched_keyword_check(r_kw, sku_df)
            our_count = sku_info["matched_count"]

            direction_icons = {
                "급상승": "🔺🔺", "상승": "🔺", "유지": "➡️",
                "하락": "🔻", "급하락": "🔻🔻",
            }

            rows.append({
                "연관키워드": r_kw,
                "원래키워드": parent_kw,
                "네이버_트렌드": "{} {}".format(
                    direction_icons.get(r_direction, ""), r_direction
                ),
                "성장률": "{:+.1f}%".format(r_growth),
                "모멘텀": r_momentum,
                "보유SKU수": our_count,
                "보유현황": "{}개".format(our_count) if our_count > 0 else "❌ 미보유",
                "_growth": r_growth,
            })

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    result = result.sort_values("_growth", ascending=False).reset_index(drop=True)
    result = result.drop(columns=["_growth"])

    return result


# ──────────────────────────────────────
# 5. 최종 SKU 추가 제안 요약
# ──────────────────────────────────────

def generate_sku_action_summary(
    keyword_report: pd.DataFrame,
    related_report: pd.DataFrame,
    hsmoa_keywords: List[dict],
    sku_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    키워드 리포트 + 연관 키워드 리포트를 종합하여
    최종 SKU 추가 제안 목록을 생성한다.

    우선순위: 신규도입 > 라인업강화
    각 제안에: 키워드, 근거, 추천 브랜드/상품, 보유현황

    Returns:
        DataFrame — 최종 제안 목록
    """
    suggestions = []

    # 1) 메인 키워드 중 신규 도입 / 라인업 강화
    if not keyword_report.empty:
        action_targets = keyword_report[
            keyword_report["추천행동"].isin(["🔴 신규 도입", "🟠 라인업 강화"])
        ]

        for _, row in action_targets.iterrows():
            # 추천 상품 (홈쇼핑에서 가장 인기 있는 것)
            kw = row["키워드"]
            hsmoa_info = next((h for h in hsmoa_keywords if h["keyword"] == kw), None)

            suggested_products = []
            if hsmoa_info:
                for p in hsmoa_info.get("hsmoa_products", [])[:3]:
                    name = p.get("name", "")[:40]
                    brand = p.get("brand", "")
                    price = p.get("price", 0)
                    if brand:
                        suggested_products.append(
                            "{} ({}{})".format(
                                name, brand,
                                " {:,}원".format(price) if price > 0 else ""
                            )
                        )

            # 근거 정리
            reasons = []
            if row["홈쇼핑_상품수"] >= 2:
                reasons.append("홈쇼핑 {}개 상품".format(row["홈쇼핑_상품수"]))
            naver_dir = row["네이버_트렌드"].strip()
            if "상승" in naver_dir or "급상승" in naver_dir:
                reasons.append("네이버 검색 {}".format(row["네이버_성장률_표시"]))
            if row["연관_상승_수"] > 0:
                reasons.append("연관어 {}개 상승".format(row["연관_상승_수"]))

            suggestions.append({
                "우선순위": row["추천행동"],
                "키워드": kw,
                "신뢰도": row["신뢰도"],
                "종합점수": row["종합점수"],
                "근거": " | ".join(reasons) if reasons else "-",
                "추천브랜드": row["추천_브랜드"],
                "추천상품": " / ".join(suggested_products[:2]) if suggested_products else "-",
                "보유현황": row["보유현황"],
                "_sort": 0 if "신규" in row["추천행동"] else 1,
            })

    # 2) 연관 키워드 중 미보유 + 상승 중인 것
    if not related_report.empty:
        related_targets = related_report[related_report["보유SKU수"] == 0]

        for _, row in related_targets.head(5).iterrows():
            suggestions.append({
                "우선순위": "🟣 연관키워드 발굴",
                "키워드": row["연관키워드"],
                "신뢰도": "🔵 참고",
                "종합점수": row["모멘텀"],
                "근거": "{}의 연관어 | 네이버 {}".format(
                    row["원래키워드"], row["성장률"]
                ),
                "추천브랜드": "-",
                "추천상품": "-",
                "보유현황": "미보유",
                "_sort": 2,
            })

    if not suggestions:
        return pd.DataFrame()

    result = pd.DataFrame(suggestions)
    result = result.sort_values(["_sort", "종합점수"], ascending=[True, False])
    result = result.drop(columns=["_sort"]).reset_index(drop=True)

    return result


# ──────────────────────────────────────
# 6. 드릴다운: 특정 키워드 상세 데이터
# ──────────────────────────────────────

def get_keyword_hsmoa_products(
    keyword: str,
    hsmoa_keywords: List[dict],
) -> List[dict]:
    """특정 키워드에 해당하는 홈쇼핑모아 인기 상품 리스트"""
    for kw_info in hsmoa_keywords:
        if kw_info["keyword"] == keyword:
            return kw_info.get("hsmoa_products", [])
    return []


def get_keyword_trend_data(
    keyword: str,
    keyword_report: pd.DataFrame,
) -> List[dict]:
    """특정 키워드의 네이버 검색량 시계열 데이터"""
    if keyword_report.empty:
        return []

    row = keyword_report[keyword_report["키워드"] == keyword]
    if row.empty:
        return []

    return row.iloc[0].get("_trend_data", [])


# ──────────────────────────────────────
# 통합 실행 함수
# ──────────────────────────────────────

def run_full_cross_analysis(
    hsmoa_products_df: pd.DataFrame,
    sku_df: pd.DataFrame,
    use_api: bool = True,
    baro_products_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    전체 교차 검증 분석을 한 번에 실행

    Args:
        hsmoa_products_df: 홈쇼핑모아 상품 DataFrame
        sku_df: 전체 SKU DataFrame
        use_api: True=실제 API, False=데모 데이터
        baro_products_df: 바로팜 상품 DataFrame (선택적)

    Returns:
        {
            "hsmoa_keywords": [...],
            "keyword_report": DataFrame,
            "related_report": DataFrame,
            "action_summary": DataFrame,
            "naver_results": {...},
            "is_demo": bool,
        }
    """
    # 바로팜 데이터가 있으면 홈쇼핑모아 호환 형태로 변환 후 통합
    combined_products = hsmoa_products_df.copy() if not hsmoa_products_df.empty else pd.DataFrame()

    if baro_products_df is not None and not baro_products_df.empty:
        # 바로팜 → 홈쇼핑모아 호환 형식으로 매핑
        baro_compat = pd.DataFrame()
        baro_rows = []
        for _, item in baro_products_df.iterrows():
            product_type = "의약품" if item.get("type") == "DRUG" else "건강식품"
            baro_rows.append({
                "pdid": "baro_{}".format(item.get("id", "")),
                "name": item.get("name", ""),
                "brand": item.get("manufacturer", ""),
                "category3": product_type,
                "price": item.get("normal_price", 0),
                "sale_price": item.get("lowest_price", 0),
                "review_count": item.get("total_qty", 0),
                "review_rating": 0,
                "site": "바로팜",
                "keyword": item.get("keyword", ""),
                "section": product_type,
            })
        if baro_rows:
            baro_compat = pd.DataFrame(baro_rows)
            if combined_products.empty:
                combined_products = baro_compat
            else:
                combined_products = pd.concat(
                    [combined_products, baro_compat], ignore_index=True,
                )

    # Step 1: 키워드 추출 (홈쇼핑 + 바로팜 통합)
    hsmoa_keywords = extract_hsmoa_keywords(combined_products)

    if not hsmoa_keywords:
        return {
            "hsmoa_keywords": [],
            "keyword_report": pd.DataFrame(),
            "related_report": pd.DataFrame(),
            "action_summary": pd.DataFrame(),
            "naver_results": {},
            "is_demo": True,
        }

    # Step 2: 네이버 트렌드 검증 + 연관 검색어
    naver_results = validate_and_expand_keywords(
        hsmoa_keywords, use_api=use_api,
    )

    # Step 3: 키워드 트렌드 리포트
    keyword_report = generate_keyword_trend_report(
        hsmoa_keywords, naver_results, sku_df,
    )

    # Step 4: 연관 검색어 리포트
    related_report = generate_related_keyword_report(
        naver_results, sku_df,
    )

    # Step 5: 최종 SKU 제안
    action_summary = generate_sku_action_summary(
        keyword_report, related_report, hsmoa_keywords, sku_df,
    )

    return {
        "hsmoa_keywords": hsmoa_keywords,
        "keyword_report": keyword_report,
        "related_report": related_report,
        "action_summary": action_summary,
        "naver_results": naver_results,
        "is_demo": naver_results.get("is_demo", True),
    }


# ──────────────────────────────────────
# 테스트
# ──────────────────────────────────────

if __name__ == "__main__":
    from pathlib import Path
    from hsmoa_api import fetch_health_products

    print("=" * 60)
    print("통합 트렌드 분석 v2 — 키워드 레벨 교차 검증 테스트")
    print("=" * 60)

    # 데이터 로드
    sku_df = pd.read_excel(Path(__file__).parent / "신규_sku분류_정제.xlsx")
    print("SKU: {}개".format(len(sku_df)))

    products = fetch_health_products(force_refresh=False)
    print("홈쇼핑모아: {}개 상품".format(len(products)))

    # Step 1: 키워드 추출
    print("\n[1] 홈쇼핑모아 키워드 추출")
    hsmoa_kws = extract_hsmoa_keywords(products)
    print("  추출된 키워드: {}개".format(len(hsmoa_kws)))
    for kw_info in hsmoa_kws[:10]:
        print("  {:12s} | 상품 {:2d}개 | 브랜드: {}".format(
            kw_info["keyword"],
            kw_info["hsmoa_product_count"],
            ", ".join(kw_info["hsmoa_brands"][:3]),
        ))

    # Step 2: 전체 분석
    print("\n[2] 전체 교차 검증 분석")
    result = run_full_cross_analysis(products, sku_df, use_api=is_api_configured())

    print("  데모 모드: {}".format(result["is_demo"]))

    report = result["keyword_report"]
    if not report.empty:
        print("\n  [키워드 트렌드 리포트] — {}개 키워드".format(len(report)))
        for _, r in report.head(10).iterrows():
            print("  {} | {:10s} | {} | 성장:{} | 점수:{:.0f} | SKU:{}".format(
                r["신뢰도"], r["키워드"],
                r["네이버_트렌드"][:8], r["네이버_성장률_표시"],
                r["종합점수"], r["보유현황"],
            ))

    related = result["related_report"]
    if not related.empty:
        print("\n  [연관 검색어 트렌드] — {}개 발견".format(len(related)))
        for _, r in related.head(5).iterrows():
            print("  {:15s} ← {} | {} | SKU:{}".format(
                r["연관키워드"], r["원래키워드"],
                r["네이버_트렌드"][:8], r["보유현황"],
            ))

    summary = result["action_summary"]
    if not summary.empty:
        print("\n  [최종 SKU 제안] — {}개".format(len(summary)))
        for _, r in summary.iterrows():
            print("  {} {:12s} | {} | {}".format(
                r["우선순위"][:5], r["키워드"],
                r["근거"][:40], r["보유현황"],
            ))
