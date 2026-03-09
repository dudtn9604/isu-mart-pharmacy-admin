"""
역방향 분석 모듈 — SKU 부족 세부분류 → 트렌드 역분석 → 구체적 제품 추천

[분석 흐름]
1. 현황 분석에서 "SKU 부족" / "SKU 보충 검토"로 판정된 세부분류를 시작점으로
2. 세부분류명 → 검색 키워드 변환 (동의어 포함)
3. 홈쇼핑모아 + 바로팜 검색 → 해당 키워드의 인기 상품/브랜드 수집
4. 검색 결과에서 성분/원료 키워드 추출
5. 네이버 트렌드 검증 + 연관 검색어 확장
6. 최종 추천: 추가해야 할 성분별 제품군 + 구체적 상품

[데이터 소스]
- 홈쇼핑모아 (hsmoa.com): 건강기능식품 위주
- 바로팜 (baropharm.com): 의약품 + 건강기능식품 도매
- 네이버 검색 트렌드: 검증 + 연관 검색어 확장

[핵심 철학]
"SKU가 부족한 곳에서 시작하여, 시장 트렌드를 역추적하고,
 구체적으로 어떤 제품을 추가해야 하는지 제시한다"
"""

import time
import pandas as pd
from typing import Dict, List, Set

from sku_enrichment import (
    KEYWORD_SYNONYMS,
    enriched_keyword_match,
    enriched_keyword_check,
    expand_keyword_synonyms,
    normalize_keyword,
)

from naver_keyword_api import (
    is_api_configured,
    fetch_keyword_trend,
    fetch_related_keywords,
    analyze_keyword_trend,
    filter_health_related_keywords,
)

from hsmoa_api import search_hsmoa
from baropharm_api import search_baropharm, is_baropharm_configured


# ──────────────────────────────────────
# 1. 세부분류명 → 검색 키워드 변환
# ──────────────────────────────────────

# 세부분류명 → 홈쇼핑 검색용 키워드 매핑
# 세부분류명이 직접 검색에 적합하지 않을 때 사용
SUBCATEGORY_SEARCH_MAP: Dict[str, List[str]] = {
    "경옥고": ["경옥고"],
    "여드름치료제": ["여드름", "여드름 치료"],
    "종합비타민": ["종합비타민", "멀티비타민"],
    "탈모약": ["탈모", "탈모 샴푸", "미녹시딜"],
    "오메가3": ["오메가3", "크릴오일"],
    "무좀약": ["무좀", "무좀약"],
    "파스": ["파스", "붙이는 파스"],
    "비염스프레이": ["비염", "비염 스프레이"],
    "소화제": ["소화제", "소화효소"],
    "변비약": ["변비", "변비약"],
    "수면유도제": ["수면", "멜라토닌", "수면 보조"],
    "비타민C": ["비타민C", "비타민씨"],
    "비타민D": ["비타민D", "비타민디"],
    "유산균": ["유산균", "프로바이오틱스"],
    "콜라겐": ["콜라겐"],
    "루테인": ["루테인", "눈건강"],
    "관절영양제": ["관절", "글루코사민", "보스웰리아"],
    "프로폴리스": ["프로폴리스"],
    "밀크씨슬": ["밀크씨슬", "실리마린"],
    "마그네슘": ["마그네슘"],
    "철분제": ["철분", "헴철"],
    "아연": ["아연"],
    "비오틴": ["비오틴"],
    "홍삼": ["홍삼", "정관장"],
    "칼슘": ["칼슘", "칼슘 비타민D"],
    "코큐텐": ["코큐텐", "코엔자임"],
    "글루타치온": ["글루타치온"],
}


def prepare_search_keywords(subcategory: str) -> List[str]:
    """
    세부분류명을 홈쇼핑모아 검색 키워드 세트로 변환

    1. SUBCATEGORY_SEARCH_MAP에서 사전 정의된 키워드 확인
    2. KEYWORD_SYNONYMS에서 관련 동의어 추가
    3. 세부분류명 자체를 기본 키워드로 포함

    Returns:
        중복 제거된 검색 키워드 리스트 (최대 5개)
    """
    keywords = set()

    # 1차: 사전 정의 매핑
    if subcategory in SUBCATEGORY_SEARCH_MAP:
        keywords.update(SUBCATEGORY_SEARCH_MAP[subcategory])
    else:
        # 세부분류명 자체를 키워드로
        keywords.add(subcategory)

    # 2차: 동의어 사전에서 관련 키워드 추가
    sub_lower = subcategory.lower()
    for canonical, variants in KEYWORD_SYNONYMS.items():
        # 세부분류명이 canonical이나 variant에 포함되면 관련 키워드
        if sub_lower in canonical.lower() or canonical.lower() in sub_lower:
            keywords.add(canonical)
            break
        for v in variants:
            if sub_lower in v.lower() or v.lower() in sub_lower:
                keywords.add(canonical)
                break

    return list(keywords)[:5]


# ──────────────────────────────────────
# 2. 홈쇼핑모아 통합 검색
# ──────────────────────────────────────

def search_hsmoa_for_subcategory(keywords: List[str]) -> pd.DataFrame:
    """
    여러 키워드로 홈쇼핑모아 검색 → 통합 DataFrame 반환

    Returns:
        상품 DataFrame (name, brand, category3, sale_price, review_count, ...)
    """
    all_products = []
    seen_pdids = set()

    for kw in keywords:
        data = search_hsmoa(kw)
        if data is None:
            time.sleep(0.5)
            continue

        for section in ["best", "past", "future", "ep0", "ep1", "ep2", "ep3"]:
            items = data.get(section, [])
            if not items:
                continue

            for item in items:
                pdid = item.get("pdid", "")
                if pdid in seen_pdids:
                    continue
                seen_pdids.add(pdid)

                product = {
                    "pdid": pdid,
                    "name": item.get("name", ""),
                    "brand": item.get("brand", ""),
                    "category3": item.get("category3", item.get("ep_cate3", "")),
                    "price": item.get("price", 0),
                    "sale_price": item.get("sale_price", 0),
                    "review_count": item.get("review_count", 0),
                    "review_rating": item.get("review_rating", 0),
                    "site": item.get("site", ""),
                    "keyword": kw,
                    "section": section,
                }
                all_products.append(product)

        time.sleep(0.5)

    return pd.DataFrame(all_products) if all_products else pd.DataFrame()


# ──────────────────────────────────────
# 2-2. 바로팜 통합 검색
# ──────────────────────────────────────

def search_baropharm_for_subcategory(keywords: List[str]) -> pd.DataFrame:
    """
    여러 키워드로 바로팜 검색 → 홈쇼핑모아 호환 형태의 통합 DataFrame 반환

    바로팜 필드를 홈쇼핑모아 호환 필드명으로 매핑:
    - manufacturer → brand
    - lowest_price → sale_price
    - total_qty → review_count (인기도 대용)
    - type(DRUG/PRODUCT) → product_type

    Returns:
        상품 DataFrame (name, brand, sale_price, product_type, source, ...)
    """
    if not is_baropharm_configured():
        return pd.DataFrame()

    all_products = []
    seen_ids = set()

    for kw in keywords:
        results = search_baropharm(kw, per_page=30, max_pages=2)

        for item in results:
            pid = item.get("id", "")
            if pid in seen_ids:
                continue
            seen_ids.add(pid)

            product_type = "의약품" if item.get("type") == "DRUG" else "건강식품"
            cats = item.get("categories", [])
            category3 = cats[-1] if cats else ""

            product = {
                "pdid": "baro_{}".format(pid),
                "name": item.get("name", ""),
                "brand": item.get("manufacturer", ""),
                "category3": category3,
                "price": item.get("normal_price", 0),
                "sale_price": item.get("lowest_price", 0),
                "review_count": item.get("total_qty", 0),
                "review_rating": 0,
                "site": "바로팜",
                "keyword": kw,
                "section": product_type,
                "source": "바로팜",
                "product_type": product_type,
            }
            all_products.append(product)

        time.sleep(0.3)

    return pd.DataFrame(all_products) if all_products else pd.DataFrame()


# ──────────────────────────────────────
# 2-3. 통합 검색 (홈쇼핑모아 + 바로팜)
# ──────────────────────────────────────

def search_all_sources_for_subcategory(keywords: List[str]) -> pd.DataFrame:
    """
    홈쇼핑모아 + 바로팜 양쪽에서 검색하여 통합 DataFrame 반환

    Returns:
        통합 상품 DataFrame (source 컬럼으로 출처 구분)
    """
    # 홈쇼핑모아 검색
    hsmoa_df = search_hsmoa_for_subcategory(keywords)
    if not hsmoa_df.empty:
        hsmoa_df["source"] = "홈쇼핑모아"
        hsmoa_df["product_type"] = "건강식품"

    # 바로팜 검색
    baro_df = search_baropharm_for_subcategory(keywords)

    # 통합
    dfs = [df for df in [hsmoa_df, baro_df] if not df.empty]
    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)
    return combined


# ──────────────────────────────────────
# 3. 성분/원료 분석
# ──────────────────────────────────────

def analyze_ingredients(
    all_products: pd.DataFrame,
    sku_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    홈쇼핑모아 + 바로팜 검색 결과에서 성분/원료 키워드를 추출하고 분석

    - 상품명에서 KEYWORD_SYNONYMS의 성분 키워드 스캔
    - 각 성분별: 상품수, 대표 브랜드, 평균 가격, 우리 약국 보유 여부
    - 데이터 소스별 상품수 (홈쇼핑/바로팜)

    Returns:
        성분별 분석 DataFrame
    """
    if all_products.empty:
        return pd.DataFrame()

    ingredient_data = {}  # canonical → {products, brands, prices, ...}

    for _, product in all_products.iterrows():
        name_lower = str(product.get("name", "")).lower()
        source = product.get("source", "홈쇼핑모아")

        for canonical, variants in KEYWORD_SYNONYMS.items():
            for variant in variants:
                if len(variant) >= 2 and variant.lower() in name_lower:
                    if canonical not in ingredient_data:
                        ingredient_data[canonical] = {
                            "products": [],
                            "brands": set(),
                            "prices": [],
                            "pdids": set(),
                            "source_counts": {"홈쇼핑모아": 0, "바로팜": 0},
                        }

                    pdid = product.get("pdid", "")
                    if pdid in ingredient_data[canonical]["pdids"]:
                        break
                    ingredient_data[canonical]["pdids"].add(pdid)
                    ingredient_data[canonical]["source_counts"][source] = (
                        ingredient_data[canonical]["source_counts"].get(source, 0) + 1
                    )

                    ingredient_data[canonical]["products"].append({
                        "name": product.get("name", ""),
                        "brand": product.get("brand", ""),
                        "price": product.get("sale_price", 0),
                        "source": source,
                    })
                    brand = product.get("brand", "")
                    if brand:
                        ingredient_data[canonical]["brands"].add(brand)
                    price = product.get("sale_price", 0)
                    if price > 0:
                        ingredient_data[canonical]["prices"].append(price)
                    break  # 한 상품에서 한 성분만 매칭

    if not ingredient_data:
        return pd.DataFrame()

    rows = []
    for ingredient, data in ingredient_data.items():
        count = len(data["pdids"])
        if count < 1:
            continue

        # 우리 약국 보유 확인
        sku_info = enriched_keyword_check(ingredient, sku_df)
        our_count = sku_info["matched_count"]

        avg_price = (
            int(sum(data["prices"]) / len(data["prices"]))
            if data["prices"]
            else 0
        )

        # 소스별 상품수
        hsmoa_cnt = data["source_counts"].get("홈쇼핑모아", 0)
        baro_cnt = data["source_counts"].get("바로팜", 0)

        rows.append({
            "성분/원료": ingredient,
            "시장_상품수": count,
            "홈쇼핑": hsmoa_cnt,
            "바로팜": baro_cnt,
            "주요_브랜드": ", ".join(sorted(data["brands"])[:4]),
            "평균_가격": avg_price,
            "우리_보유수": our_count,
            "보유상태": "{}개 보유".format(our_count) if our_count > 0 else "미보유",
        })

    result = pd.DataFrame(rows)
    result = result.sort_values("시장_상품수", ascending=False).reset_index(drop=True)
    return result


# ──────────────────────────────────────
# 4. 네이버 트렌드 역검증
# ──────────────────────────────────────

def reverse_naver_analysis(
    subcategory: str,
    ingredient_keywords: List[str],
    use_api: bool = True,
) -> dict:
    """
    세부분류 키워드 + 성분 키워드를 네이버 트렌드로 검증

    Returns:
        {
            "main_trend": { trend_direction, growth_rate, ... },
            "ingredient_trends": { "성분": { trend_direction, ... }, ... },
            "related_keywords": ["연관1", "연관2", ...],
        }
    """
    result = {
        "main_trend": {},
        "ingredient_trends": {},
        "related_keywords": [],
    }

    if not use_api or not is_api_configured():
        # API 없으면 빈 결과 반환 (데모 모드에서도 역분석 표시 가능)
        return result

    # 메인 키워드 트렌드
    synonyms = list(expand_keyword_synonyms(subcategory))
    main_df = fetch_keyword_trend(subcategory, synonyms=synonyms, months_back=6)
    if main_df is not None and not main_df.empty:
        result["main_trend"] = analyze_keyword_trend(main_df)

    # 연관 검색어
    related = fetch_related_keywords(subcategory, max_results=10)
    health_related = filter_health_related_keywords(related)
    result["related_keywords"] = health_related

    # 성분별 트렌드
    for ingredient in ingredient_keywords[:8]:
        syns = list(expand_keyword_synonyms(ingredient))
        trend_df = fetch_keyword_trend(ingredient, synonyms=syns, months_back=6)
        if trend_df is not None and not trend_df.empty:
            result["ingredient_trends"][ingredient] = analyze_keyword_trend(trend_df)

    return result


# ──────────────────────────────────────
# 5. 최종 추천 생성
# ──────────────────────────────────────

def generate_reverse_recommendations(
    subcategory: str,
    ingredient_analysis: pd.DataFrame,
    naver_result: dict,
    all_products: pd.DataFrame,
    sku_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    성분별 분석 + 네이버 트렌드를 종합하여 최종 제품 추천 생성

    Returns:
        추천 DataFrame (우선순위, 추천키워드, 트렌드, 추천브랜드, 추천상품, 가격대, 근거)
    """
    suggestions = []
    ingredient_trends = naver_result.get("ingredient_trends", {})

    if not ingredient_analysis.empty:
        for _, row in ingredient_analysis.iterrows():
            ingredient = row["성분/원료"]
            our_count = row["우리_보유수"]
            market_count = row["시장_상품수"]
            hsmoa_cnt = row.get("홈쇼핑", 0)
            baro_cnt = row.get("바로팜", 0)
            avg_price = row["평균_가격"]
            brands = row["주요_브랜드"]

            # 네이버 트렌드 정보
            trend_info = ingredient_trends.get(ingredient, {})
            trend_dir = trend_info.get("trend_direction", "데이터없음")
            growth = trend_info.get("growth_rate", 0)
            momentum = trend_info.get("momentum_score", 0)

            direction_icons = {
                "급상승": "🔺🔺", "상승": "🔺", "유지": "➡️",
                "하락": "🔻", "급하락": "🔻🔻", "데이터없음": "—",
            }
            trend_display = "{} {}".format(
                direction_icons.get(trend_dir, "—"), trend_dir,
            )

            # 우선순위 결정
            if our_count == 0 and trend_dir in ("급상승", "상승"):
                priority = "🔴 즉시 도입"
                priority_sort = 0
            elif our_count == 0:
                priority = "🟠 신규 도입"
                priority_sort = 1
            elif our_count <= 2 and trend_dir in ("급상승", "상승"):
                priority = "🟡 라인업 확대"
                priority_sort = 2
            elif our_count <= 2:
                priority = "🔵 보충 검토"
                priority_sort = 3
            else:
                priority = "✅ 충분"
                priority_sort = 4

            # 추천 상품 (홈쇼핑+바로팜에서 해당 성분 인기 상품)
            recommended_products = []
            if not all_products.empty:
                ingredient_lower = ingredient.lower()
                # 동의어 포함 검색
                search_terms = [ingredient_lower]
                if ingredient in KEYWORD_SYNONYMS:
                    search_terms.extend(
                        v.lower() for v in KEYWORD_SYNONYMS[ingredient]
                    )

                for _, p in all_products.iterrows():
                    p_name = str(p.get("name", "")).lower()
                    if any(term in p_name for term in search_terms):
                        src_tag = "💊" if p.get("source") == "바로팜" else "🛒"
                        recommended_products.append({
                            "name": p.get("name", "")[:35],
                            "brand": p.get("brand", ""),
                            "price": p.get("sale_price", 0),
                            "source": p.get("source", ""),
                            "tag": src_tag,
                        })
                        if len(recommended_products) >= 4:
                            break

            product_text = " / ".join(
                "{}{} ({})".format(p["tag"], p["name"], p["brand"])
                for p in recommended_products
            ) if recommended_products else "-"

            # 추천 브랜드 (우리가 안 갖고 있는 브랜드)
            suggested_brands = []
            for brand_name in brands.split(", "):
                brand_name = brand_name.strip()
                if len(brand_name) >= 2:
                    brand_skus = enriched_keyword_match(brand_name, sku_df)
                    if len(brand_skus) == 0:
                        suggested_brands.append(brand_name)

            # 근거
            reasons = []
            source_parts = []
            if hsmoa_cnt >= 1:
                source_parts.append("홈쇼핑{}개".format(hsmoa_cnt))
            if baro_cnt >= 1:
                source_parts.append("바로팜{}개".format(baro_cnt))
            if source_parts:
                reasons.append(" + ".join(source_parts))
            if trend_dir in ("급상승", "상승"):
                reasons.append("네이버 {:+.1f}%".format(growth))
            if our_count == 0:
                reasons.append("현재 미보유")

            price_text = "{:,}원대".format(
                round(avg_price / 10000) * 10000
            ) if avg_price > 0 else "-"

            suggestions.append({
                "우선순위": priority,
                "추천_성분": ingredient,
                "트렌드": trend_display,
                "추천_브랜드": ", ".join(suggested_brands[:3]) if suggested_brands else "-",
                "추천_상품": product_text,
                "가격대": price_text,
                "현재_보유": "{}개".format(our_count) if our_count > 0 else "미보유",
                "근거": " | ".join(reasons) if reasons else "-",
                "_sort": priority_sort,
            })

    # 연관 검색어에서 추가 발굴 (홈쇼핑 결과에 없는 키워드)
    related_kws = naver_result.get("related_keywords", [])
    existing_ingredients = set(
        ingredient_analysis["성분/원료"].tolist()
    ) if not ingredient_analysis.empty else set()

    for r_kw in related_kws[:5]:
        r_canonical = normalize_keyword(r_kw)
        if r_canonical in existing_ingredients:
            continue

        # 우리 약국 보유 확인
        sku_info = enriched_keyword_check(r_kw, sku_df)
        if sku_info["matched_count"] > 2:
            continue

        suggestions.append({
            "우선순위": "🟣 연관 검색어 발굴",
            "추천_성분": r_kw,
            "트렌드": "— (연관검색어)",
            "추천_브랜드": "-",
            "추천_상품": "-",
            "가격대": "-",
            "현재_보유": "{}개".format(sku_info["matched_count"]) if sku_info["matched_count"] > 0 else "미보유",
            "근거": "'{}' 연관검색어".format(subcategory),
            "_sort": 5,
        })

    if not suggestions:
        return pd.DataFrame()

    result = pd.DataFrame(suggestions)
    result = result.sort_values("_sort").reset_index(drop=True)
    result = result.drop(columns=["_sort"])
    return result


# ──────────────────────────────────────
# 통합 실행 함수
# ──────────────────────────────────────

def run_reverse_analysis(
    subcategory: str,
    sku_df: pd.DataFrame,
    current_sku_count: int = 0,
    current_avg_sales: float = 0,
    use_api: bool = True,
) -> dict:
    """
    SKU 부족 세부분류 → 역방향 트렌드 분석 통합 실행

    Args:
        subcategory: 세부분류명 (예: "탈모약")
        sku_df: 전체 SKU DataFrame
        current_sku_count: 현재 해당 세부분류 SKU 수
        current_avg_sales: 현재 해당 세부분류 SKU당 평균매출
        use_api: True=실제 API, False=API 미사용

    Returns:
        {
            "subcategory": str,
            "current_sku_count": int,
            "current_avg_sales": float,
            "search_keywords": list,
            "hsmoa_products": DataFrame,
            "ingredient_analysis": DataFrame,
            "naver_result": dict,
            "recommendations": DataFrame,
        }
    """
    # Step 1: 검색 키워드 준비
    search_keywords = prepare_search_keywords(subcategory)

    # Step 2: 홈쇼핑모아 + 바로팜 통합 검색
    all_products = search_all_sources_for_subcategory(search_keywords)

    # 소스별 분리 (UI 표시용)
    hsmoa_products = (
        all_products[all_products["source"] == "홈쇼핑모아"].copy()
        if not all_products.empty and "source" in all_products.columns
        else pd.DataFrame()
    )
    baro_products = (
        all_products[all_products["source"] == "바로팜"].copy()
        if not all_products.empty and "source" in all_products.columns
        else pd.DataFrame()
    )

    # Step 3: 성분 분석 (통합 데이터 기반)
    ingredient_analysis = analyze_ingredients(all_products, sku_df)

    # Step 4: 네이버 트렌드 역검증
    ingredient_keywords = (
        ingredient_analysis["성분/원료"].tolist()
        if not ingredient_analysis.empty
        else []
    )
    naver_result = reverse_naver_analysis(
        subcategory, ingredient_keywords, use_api=use_api,
    )

    # Step 5: 최종 추천
    recommendations = generate_reverse_recommendations(
        subcategory, ingredient_analysis, naver_result,
        all_products, sku_df,
    )

    return {
        "subcategory": subcategory,
        "current_sku_count": current_sku_count,
        "current_avg_sales": current_avg_sales,
        "search_keywords": search_keywords,
        "all_products": all_products,
        "hsmoa_products": hsmoa_products,
        "baro_products": baro_products,
        "ingredient_analysis": ingredient_analysis,
        "naver_result": naver_result,
        "recommendations": recommendations,
    }
