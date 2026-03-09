"""
바로팜 API 연동 모듈

[데이터 소스]
바로팜 bestmall API (api-v2.baropharm.com)
- 약국 도매 플랫폼: 의약품 + 건강기능식품 43,000+ 상품
- 로그인 필요 (쿠키 기반 인증)
- 검색, 카테고리 필터, 페이지네이션 지원

[핵심 목적]
홈쇼핑모아(건강식품 위주)로는 커버되지 않는 의약품까지 포함하여
마트약국 SKU에 추가할 수 있는 전체 상품 풀을 탐색한다.

[API 구조]
1. 로그인: POST api.baropharm.com/api/rest-auth/login/ → sessionid 쿠키
2. 상품 검색: GET api-v2.baropharm.com/best-products?per_page=&page=&q=
3. 인증: 쿠키 기반 (sessionid + csrftoken)
"""

import json
import time
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

from trend_config import (
    BAROPHARM_USERNAME,
    BAROPHARM_PASSWORD,
    BAROPHARM_LOGIN_URL,
    BAROPHARM_API_URL,
    BAROPHARM_WEB_VERSION,
    BAROPHARM_HEALTH_KEYWORDS,
    TREND_DATA_DIR,
    TREND_CACHE_HOURS,
)

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / TREND_DATA_DIR

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


# ──────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────

def ensure_data_dir():
    DATA_DIR.mkdir(exist_ok=True)


def get_cache_path(cache_key: str) -> Path:
    return DATA_DIR / "baro_{}.json".format(cache_key)


def is_cache_valid(cache_key: str) -> bool:
    cache_path = get_cache_path(cache_key)
    if not cache_path.exists():
        return False
    mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
    return (datetime.now() - mtime) < timedelta(hours=TREND_CACHE_HOURS)


def load_cache(cache_key: str) -> Optional[list]:
    cache_path = get_cache_path(cache_key)
    if not cache_path.exists():
        return None
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_cache(cache_key: str, data: list):
    ensure_data_dir()
    cache_path = get_cache_path(cache_key)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ──────────────────────────────────────
# 1. 인증 (쿠키 기반)
# ──────────────────────────────────────

_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    """인증된 세션 반환 (싱글턴, 필요시 재로그인)"""
    global _session

    if _session is not None:
        return _session

    _session = requests.Session()
    _session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Baropharm-Web-Version": BAROPHARM_WEB_VERSION,
        "Origin": "https://www.baropharm.com",
        "Referer": "https://www.baropharm.com/bestmall",
    })

    # 로그인
    try:
        resp = _session.post(
            BAROPHARM_LOGIN_URL,
            json={
                "username": BAROPHARM_USERNAME,
                "password": BAROPHARM_PASSWORD,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if "key" not in data:
            print("[바로팜] 로그인 실패: {}".format(data))
            _session = None
            return requests.Session()
    except Exception as e:
        print("[바로팜] 로그인 오류: {}".format(e))
        _session = None
        return requests.Session()

    return _session


def is_baropharm_configured() -> bool:
    """바로팜 계정이 설정되어 있는지 확인"""
    return bool(BAROPHARM_USERNAME) and bool(BAROPHARM_PASSWORD)


# ──────────────────────────────────────
# 2. 상품 검색 API
# ──────────────────────────────────────

def search_baropharm(
    keyword: str,
    per_page: int = 30,
    max_pages: int = 3,
) -> List[dict]:
    """
    바로팜 bestmall에서 키워드 검색

    Args:
        keyword: 검색어
        per_page: 페이지당 상품 수 (최대 100)
        max_pages: 최대 페이지 수

    Returns:
        상품 리스트 [{name, type, manufacturer, lowest_price, ...}, ...]
    """
    session = _get_session()
    all_products = []

    encoded_q = urllib.parse.quote(keyword)

    for page in range(1, max_pages + 1):
        url = "{}/best-products?per_page={}&page={}&category_id=&whole_saler_id=&q={}".format(
            BAROPHARM_API_URL, per_page, page, encoded_q,
        )

        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 401:
                # 세션 만료 → 재로그인
                global _session
                _session = None
                session = _get_session()
                resp = session.get(url, timeout=15)

            resp.raise_for_status()
            data = resp.json()

            items = data.get("items", [])
            if not items:
                break

            for item in items:
                product = {
                    "id": item.get("id", ""),
                    "type": item.get("type", ""),  # DRUG or PRODUCT
                    "name": item.get("name", ""),
                    "manufacturer": item.get("manufacturer", ""),
                    "standard": item.get("standard", ""),
                    "kd_code": item.get("kd_code", ""),
                    "lowest_price": item.get("lowest_price", 0),
                    "highest_price": item.get("highest_price", 0),
                    "normal_price": item.get("normal_price", 0),
                    "discount_rate": item.get("discount_rate", 0),
                    "total_qty": item.get("total_qty", 0),
                    "sales": item.get("sales", 0),
                    "insured": item.get("insured", False),
                    "over_the_counter": item.get("over_the_counter", False),
                    "categories": item.get("categories", []),
                    "images": item.get("images", []),
                    "keyword": keyword,
                    "source": "바로팜",
                }
                all_products.append(product)

            # 마지막 페이지 도달 시 중단
            if page >= data.get("last_page", 1):
                break

        except Exception as e:
            print("[바로팜] 검색 오류 ({}): {}".format(keyword, e))
            break

        time.sleep(0.3)

    return all_products


# ──────────────────────────────────────
# 3. 건강/의약품 상품 대량 수집
# ──────────────────────────────────────

def fetch_health_products(
    keywords: Optional[List[str]] = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """
    건강기능식품 + 의약품 키워드로 바로팜 검색 → 통합 DataFrame

    Returns:
        DataFrame with: name, type, manufacturer, lowest_price, categories, ...
    """
    cache_key = "health_products"
    if not force_refresh and is_cache_valid(cache_key):
        cached = load_cache(cache_key)
        if cached:
            return pd.DataFrame(cached)

    if not is_baropharm_configured():
        return pd.DataFrame()

    if keywords is None:
        keywords = BAROPHARM_HEALTH_KEYWORDS

    all_products = []
    seen_ids = set()

    for kw in keywords:
        products = search_baropharm(kw, per_page=30, max_pages=2)

        for p in products:
            pid = p.get("id", "")
            if pid in seen_ids:
                continue
            seen_ids.add(pid)
            all_products.append(p)

        time.sleep(0.3)

    df = pd.DataFrame(all_products)
    if not df.empty:
        save_cache(cache_key, all_products)

    return df


# ──────────────────────────────────────
# 4. 분석 함수
# ──────────────────────────────────────

def analyze_baropharm_products(products_df: pd.DataFrame) -> dict:
    """
    바로팜 상품 데이터 분석

    Returns:
        {
            "total_products": int,
            "drug_count": int,       # 의약품 수
            "product_count": int,    # 건강식품 수
            "top_manufacturers": DataFrame,  # 인기 제조사 TOP 15
            "category_stats": DataFrame,     # 카테고리별 통계
        }
    """
    if products_df.empty:
        return {
            "total_products": 0,
            "drug_count": 0,
            "product_count": 0,
            "top_manufacturers": pd.DataFrame(),
            "category_stats": pd.DataFrame(),
        }

    total = len(products_df)
    drug_count = len(products_df[products_df["type"] == "DRUG"])
    product_count = len(products_df[products_df["type"] == "PRODUCT"])

    # 제조사별 통계
    mfr_stats = products_df.groupby("manufacturer").agg(
        상품수=("name", "count"),
        평균가격=("lowest_price", "mean"),
        총판매량=("total_qty", "sum"),
    ).reset_index()
    mfr_stats["평균가격"] = mfr_stats["평균가격"].round(0).astype(int)
    mfr_stats = mfr_stats.sort_values("상품수", ascending=False).head(15)
    mfr_stats.columns = ["제조사", "상품수", "평균가격", "총판매량"]

    # 카테고리별 통계 (categories 리스트의 마지막 항목을 세부 카테고리로)
    cat_rows = []
    for _, row in products_df.iterrows():
        cats = row.get("categories", [])
        if cats:
            cat1 = cats[0] if len(cats) > 0 else ""
            cat_detail = cats[-1] if len(cats) > 1 else cats[0]
            cat_rows.append({
                "대분류": cat1,
                "세부분류": cat_detail,
                "상품명": row["name"],
                "가격": row["lowest_price"],
                "type": row["type"],
            })

    if cat_rows:
        cat_df = pd.DataFrame(cat_rows)
        cat_stats = cat_df.groupby(["대분류", "세부분류"]).agg(
            상품수=("상품명", "count"),
            평균가격=("가격", "mean"),
        ).reset_index()
        cat_stats["평균가격"] = cat_stats["평균가격"].round(0).astype(int)
        cat_stats = cat_stats.sort_values("상품수", ascending=False)
    else:
        cat_stats = pd.DataFrame()

    return {
        "total_products": total,
        "drug_count": drug_count,
        "product_count": product_count,
        "top_manufacturers": mfr_stats,
        "category_stats": cat_stats,
    }


def _extract_search_name(name: str) -> str:
    """
    바로팜 상품명에서 SKU 매칭에 사용할 핵심 키워드를 추출

    괄호, 특수문자 등을 제거하고 핵심 상품명만 추출.
    예: "[KPAI](20%)이젠나 탈모에센스+탈모샴푸 세트" → "이젠나 탈모에센스"
    """
    import re
    # 괄호 안 내용 제거 ([], (), {})
    cleaned = re.sub(r'[\[\(（\{][^\]\)）\}]*[\]\)）\}]', '', name)
    # 특수문자 제거 (한글, 영문, 숫자, 공백만 유지)
    cleaned = re.sub(r'[^\w\sㄱ-ㅎㅏ-ㅣ가-힣a-zA-Z0-9]', ' ', cleaned)
    # 연속 공백 정리
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    # 앞 3단어만 사용 (너무 긴 상품명은 매칭 정확도 떨어짐)
    words = cleaned.split()
    if len(words) > 4:
        cleaned = ' '.join(words[:4])
    return cleaned


def generate_baropharm_sku_suggestions(
    baro_products: pd.DataFrame,
    sku_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    바로팜 인기 상품 중 현재 SKU에 없는 항목 제안

    Returns:
        제안 DataFrame
    """
    from sku_enrichment import enriched_keyword_match

    if baro_products.empty or sku_df.empty:
        return pd.DataFrame()

    suggestions = []
    seen_names = set()

    # 판매량 TOP 상품 중 SKU에 없는 것
    sorted_products = baro_products.sort_values("total_qty", ascending=False)

    for _, product in sorted_products.head(200).iterrows():
        name = product["name"]
        if name in seen_names:
            continue
        seen_names.add(name)

        # 상품명에서 핵심 키워드 추출 (정규식 특수문자 제거)
        search_name = _extract_search_name(name)
        if len(search_name) < 2:
            continue

        # 추출된 키워드로 SKU 매칭 시도
        try:
            matched = enriched_keyword_match(search_name, sku_df)
            if len(matched) > 0:
                continue
        except Exception:
            # regex 오류 시 skip
            continue

        mfr = product.get("manufacturer", "")

        cats = product.get("categories", [])
        cat_text = " > ".join(cats) if isinstance(cats, list) and cats else "-"

        product_type_raw = product.get("type", "PRODUCT")
        if product_type_raw == "DRUG":
            product_type = "의약품"
        elif product_type_raw == "ANIMALDRUG":
            product_type = "동물의약품"
        else:
            product_type = "건강식품"

        suggestions.append({
            "유형": product_type,
            "상품명": name[:50],
            "제조사": mfr,
            "카테고리": cat_text,
            "최저가": product.get("lowest_price", 0),
            "판매량": product.get("total_qty", 0),
            "검색키워드": product.get("keyword", ""),
        })

        if len(suggestions) >= 30:
            break

    if not suggestions:
        return pd.DataFrame()

    return pd.DataFrame(suggestions)


# ──────────────────────────────────────
# 통합 수집 함수
# ──────────────────────────────────────

def fetch_all_baropharm_data(force_refresh: bool = False) -> dict:
    """
    바로팜 데이터 통합 수집

    Returns:
        {
            "products": DataFrame,    # 전체 상품
            "analysis": dict,         # 분석 결과
        }
    """
    products = fetch_health_products(force_refresh=force_refresh)

    analysis = analyze_baropharm_products(products)

    return {
        "products": products,
        "analysis": analysis,
    }


# ──────────────────────────────────────
# 테스트
# ──────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("바로팜 API 연동 테스트")
    print("=" * 60)

    if not is_baropharm_configured():
        print("바로팜 계정이 설정되지 않았습니다.")
    else:
        # 단일 키워드 검색 테스트
        print("\n[1] 키워드 검색: 탈모")
        results = search_baropharm("탈모", per_page=5, max_pages=1)
        print("  결과: {}개".format(len(results)))
        for p in results[:3]:
            print("  [{}] {} | {} | {:,}원".format(
                p["type"], p["name"][:30], p["manufacturer"], p["lowest_price"],
            ))

        # 전체 수집 테스트
        print("\n[2] 건강/의약품 전체 수집")
        data = fetch_all_baropharm_data()
        products = data["products"]
        analysis = data["analysis"]

        print("  총 상품: {}개".format(analysis["total_products"]))
        print("  의약품: {}개".format(analysis["drug_count"]))
        print("  건강식품: {}개".format(analysis["product_count"]))

        if not analysis["top_manufacturers"].empty:
            print("\n  [인기 제조사 TOP 5]")
            for _, row in analysis["top_manufacturers"].head(5).iterrows():
                print("    {} — {}개 상품, 평균 {:,}원".format(
                    row["제조사"], row["상품수"], row["평균가격"],
                ))
