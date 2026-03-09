"""
홈쇼핑모아 트렌드 분석 모듈

[데이터 소스]
1. hsmoa.com 검색 API (인증 불필요)
   - 건강기능식품 키워드 검색 → 인기 상품/브랜드/카테고리 추출
   - past(방송 완료), future(방송 예정), best(인기), ep0~3(가격비교)

2. trend.hsmoa-ad.com API (인증 필요)
   - 카테고리별 인기 상품/브랜드/키워드 랭킹
   - 현재 auth_level=1(무료)에서는 전체 랭킹만 조회 가능

[핵심 목적]
홈쇼핑 채널에서 어떤 건강기능식품이 뜨고 있는지 파악하여
마트약국 SKU에 없는 인기 상품/브랜드를 발견 → SKU 추가 제안
"""

import json
import re
import time
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

from trend_config import (
    HSMOA_EMAIL,
    HSMOA_PASSWORD,
    HSMOA_TREND_URL,
    HSMOA_SEARCH_URL,
    HSMOA_HEALTH_KEYWORDS,
    HSMOA_CATEGORIES,
    TREND_DATA_DIR,
    TREND_CACHE_HOURS,
)

from sku_enrichment import enriched_keyword_match, expand_keyword_synonyms

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / TREND_DATA_DIR

# ──────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def is_hsmoa_configured() -> bool:
    """홈쇼핑모아 계정이 설정되어 있는지 확인"""
    return bool(HSMOA_EMAIL) and bool(HSMOA_PASSWORD)


def ensure_data_dir():
    DATA_DIR.mkdir(exist_ok=True)


def get_hsmoa_cache_path(cache_key: str) -> Path:
    return DATA_DIR / "hsmoa_{}.json".format(cache_key)


def is_cache_valid(cache_key: str) -> bool:
    cache_path = get_hsmoa_cache_path(cache_key)
    if not cache_path.exists():
        return False
    mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
    return (datetime.now() - mtime) < timedelta(hours=TREND_CACHE_HOURS)


def save_cache(cache_key: str, data: dict):
    ensure_data_dir()
    cache_path = get_hsmoa_cache_path(cache_key)
    cache_data = {
        "fetched_at": datetime.now().isoformat(),
        "data": data,
    }
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, ensure_ascii=False, indent=2)


def load_cache(cache_key: str) -> Optional[dict]:
    cache_path = get_hsmoa_cache_path(cache_key)
    if not cache_path.exists():
        return None
    with open(cache_path, "r", encoding="utf-8") as f:
        return json.load(f).get("data")


# ──────────────────────────────────────
# 1. hsmoa.com 검색 API (인증 불필요)
# ──────────────────────────────────────


def search_hsmoa(keyword: str) -> Optional[Dict]:
    """
    hsmoa.com 검색 → __NEXT_DATA__ 에서 상품 데이터 추출

    Returns:
        {
            "past": [...],     # 방송 완료 상품
            "future": [...],   # 방송 예정 상품
            "best": [...],     # 인기 상품
            "ep0"~"ep3": [...] # 가격비교 상품
        }
    """
    encoded = urllib.parse.quote(keyword)
    url = "{}/search?query={}".format(HSMOA_SEARCH_URL, encoded)

    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=15,
        )
        resp.raise_for_status()

        # __NEXT_DATA__ JSON 추출
        match = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            resp.text,
        )
        if not match:
            return None

        next_data = json.loads(match.group(1))
        agg = next_data.get("props", {}).get("pageProps", {}).get("aggregatedData", {})
        return agg

    except Exception as e:
        print("[hsmoa 검색 오류] {}: {}".format(keyword, e))
        return None


def fetch_health_products(
    keywords: Optional[List[str]] = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """
    건강기능식품 관련 키워드로 홈쇼핑모아 검색 → 인기 상품 종합

    Returns:
        DataFrame with: name, brand, category1/2/3, price, sale_price,
                       review_count, review_rating, site, keyword, section
    """
    cache_key = "health_products"
    if not force_refresh and is_cache_valid(cache_key):
        cached = load_cache(cache_key)
        if cached:
            return pd.DataFrame(cached)

    if keywords is None:
        keywords = HSMOA_HEALTH_KEYWORDS

    all_products = []
    seen_pdids = set()  # 중복 제거용

    for kw in keywords:
        data = search_hsmoa(kw)
        if data is None:
            time.sleep(1)
            continue

        # best, past, future, ep0~ep3 섹션에서 상품 추출
        for section in ["best", "past", "future", "ep0", "ep1", "ep2", "ep3"]:
            items = data.get(section, [])
            if not items:
                continue

            for item in items:
                pdid = item.get("pdid", "")
                if pdid in seen_pdids:
                    continue
                seen_pdids.add(pdid)

                # 건강식품 필터: category2가 '건강식품' 이거나 관련 카테고리
                cat2 = item.get("category2", "")
                cat1 = item.get("category1", "")
                if cat2 not in ("건강식품", "헬스보조식품") and cat1 != "건강":
                    # ep 섹션의 경우 ep_cate2도 확인
                    ep_cat2 = item.get("ep_cate2", "")
                    if ep_cat2 not in ("건강식품", "멀티비타민", "비타민/미네랄"):
                        continue

                product = {
                    "pdid": pdid,
                    "name": item.get("name", ""),
                    "brand": item.get("brand", ""),
                    "category1": item.get("category1", item.get("ep_cate1", "")),
                    "category2": item.get("category2", item.get("ep_cate2", "")),
                    "category3": item.get("category3", item.get("ep_cate3", "")),
                    "price": item.get("price", 0),
                    "sale_price": item.get("sale_price", 0),
                    "review_count": item.get("review_count", 0),
                    "review_rating": item.get("review_rating", 0),
                    "site": item.get("site", ""),
                    "steady_score": item.get("steady_score", 0),
                    "recent_score": item.get("recent_score", 0),
                    "keyword": kw,
                    "section": section,
                }

                # 가격이력 요약
                action_log = item.get("product_action_log", {})
                if action_log:
                    avg_price = action_log.get("avg_sale_price", {}).get("price", 0)
                    min_price = action_log.get("min_sale_price", {}).get("price", 0)
                    product["avg_price_30d"] = avg_price
                    product["min_price_30d"] = min_price

                all_products.append(product)

        time.sleep(0.5)  # 요청 간 딜레이

    df = pd.DataFrame(all_products)
    if not df.empty:
        save_cache(cache_key, all_products)

    return df


# ──────────────────────────────────────
# 2. trend.hsmoa-ad.com API (인증 필요)
# ──────────────────────────────────────


class HsmoaTrendClient:
    """홈쇼핑모아 트렌드 사이트 API 클라이언트"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._authenticated = False

    def login(self) -> bool:
        """Next-Auth 기반 로그인"""
        if not is_hsmoa_configured():
            print("[홈쇼핑모아] 계정 정보 미설정")
            return False

        try:
            # 1) CSRF 토큰 가져오기
            csrf_resp = self.session.get(
                "{}/api/auth/csrf".format(HSMOA_TREND_URL),
                timeout=10,
            )
            csrf_token = csrf_resp.json().get("csrfToken", "")

            # 2) 로그인
            login_resp = self.session.post(
                "{}/api/auth/callback/sign-in".format(HSMOA_TREND_URL),
                data={
                    "mail": HSMOA_EMAIL,
                    "password": HSMOA_PASSWORD,
                    "csrfToken": csrf_token,
                },
                allow_redirects=False,
                timeout=10,
            )

            # 302 리다이렉트 + 세션 쿠키 존재 = 로그인 성공
            session_cookie = None
            for cookie in self.session.cookies:
                if "session-token" in cookie.name:
                    session_cookie = cookie.value
                    break

            if session_cookie:
                self._authenticated = True
                print("[홈쇼핑모아] 로그인 성공")
                return True
            else:
                print("[홈쇼핑모아] 로그인 실패 - 세션 토큰 없음")
                return False

        except Exception as e:
            print("[홈쇼핑모아] 로그인 오류: {}".format(e))
            return False

    def _ensure_auth(self) -> bool:
        """인증 상태 확인, 필요시 로그인"""
        if not self._authenticated:
            return self.login()
        return True

    def get_categories(self, parent_id: Optional[int] = None) -> List[Dict]:
        """카테고리 목록 조회 (인증 불필요)"""
        try:
            if parent_id is None:
                url = "{}/api/trend/v1/category".format(HSMOA_TREND_URL)
                resp = self.session.get(url, timeout=10)
                return resp.json().get("results", [])
            else:
                url = "{}/api/trend/v1/category/bulk?parent_ids={}".format(
                    HSMOA_TREND_URL, parent_id
                )
                resp = self.session.get(url, timeout=10)
                results = resp.json().get("results", [])
                # bulk 결과는 이중 리스트
                flat = []
                for cat_list in results:
                    if isinstance(cat_list, list):
                        flat.extend(cat_list)
                    else:
                        flat.append(cat_list)
                return flat
        except Exception as e:
            print("[홈쇼핑모아] 카테고리 조회 오류: {}".format(e))
            return []

    def get_popular_products(
        self,
        category1: Optional[int] = None,
        days_back: int = 30,
    ) -> List[Dict]:
        """인기 상품 랭킹 조회"""
        if not self._ensure_auth():
            return []

        end = datetime.now() - timedelta(days=1)
        start = end - timedelta(days=days_back)

        params = {
            "interval": "daily",
            "is_popular": "false",
            "start_datetime": start.strftime("%Y-%m-%dT00:00:00+09:00"),
            "end_datetime": end.strftime("%Y-%m-%dT23:59:59+09:00"),
        }
        if category1:
            params["category1"] = str(category1)

        try:
            url = "{}/api/trend/v1/trend/category/popular/product/rank".format(
                HSMOA_TREND_URL
            )
            resp = self.session.get(url, params=params, timeout=15)
            data = resp.json()
            return data.get("results", [])
        except Exception as e:
            print("[홈쇼핑모아] 인기상품 조회 오류: {}".format(e))
            return []

    def get_popular_keywords(
        self,
        category1: Optional[int] = None,
        days_back: int = 30,
    ) -> List[Dict]:
        """인기 키워드 랭킹 조회"""
        if not self._ensure_auth():
            return []

        end = datetime.now() - timedelta(days=1)
        start = end - timedelta(days=days_back)

        params = {
            "interval": "daily",
            "is_popular": "false",
            "start_datetime": start.strftime("%Y-%m-%dT00:00:00+09:00"),
            "end_datetime": end.strftime("%Y-%m-%dT23:59:59+09:00"),
        }
        if category1:
            params["category1"] = str(category1)

        try:
            url = "{}/api/trend/v1/trend/category/popular/keyword/rank".format(
                HSMOA_TREND_URL
            )
            resp = self.session.get(url, params=params, timeout=15)
            data = resp.json()
            return data.get("results", [])
        except Exception as e:
            print("[홈쇼핑모아] 인기키워드 조회 오류: {}".format(e))
            return []

    def get_popular_brands(
        self,
        category1: Optional[int] = None,
        days_back: int = 30,
    ) -> List[Dict]:
        """인기 브랜드 랭킹 조회"""
        if not self._ensure_auth():
            return []

        end = datetime.now() - timedelta(days=1)
        start = end - timedelta(days=days_back)

        params = {
            "interval": "daily",
            "is_popular": "false",
            "start_datetime": start.strftime("%Y-%m-%dT00:00:00+09:00"),
            "end_datetime": end.strftime("%Y-%m-%dT23:59:59+09:00"),
        }
        if category1:
            params["category1"] = str(category1)

        try:
            url = "{}/api/trend/v1/trend/category/popular/brand/rank".format(
                HSMOA_TREND_URL
            )
            resp = self.session.get(url, params=params, timeout=15)
            data = resp.json()
            return data.get("results", [])
        except Exception as e:
            print("[홈쇼핑모아] 인기브랜드 조회 오류: {}".format(e))
            return []


# ──────────────────────────────────────
# 3. 데이터 분석 및 가공
# ──────────────────────────────────────


def analyze_trending_brands(products_df: pd.DataFrame) -> pd.DataFrame:
    """인기 브랜드 분석 - 브랜드별 등장 횟수, 평균 가격, 카테고리"""
    if products_df.empty:
        return pd.DataFrame()

    brand_stats = (
        products_df.groupby("brand")
        .agg(
            등장횟수=("pdid", "count"),
            평균가격=("sale_price", "mean"),
            리뷰평균=("review_rating", "mean"),
            주요카테고리=("category3", lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else ""),
            인기점수=("steady_score", "sum"),
            상품목록=("name", lambda x: " | ".join(x.head(3).tolist())),
        )
        .reset_index()
    )

    brand_stats["평균가격"] = brand_stats["평균가격"].round(0).astype(int)
    brand_stats["리뷰평균"] = brand_stats["리뷰평균"].round(1)
    brand_stats = brand_stats.sort_values("등장횟수", ascending=False).reset_index(drop=True)

    return brand_stats


def analyze_trending_categories(products_df: pd.DataFrame) -> pd.DataFrame:
    """인기 카테고리(세부분류) 분석"""
    if products_df.empty:
        return pd.DataFrame()

    cat_stats = (
        products_df.groupby("category3")
        .agg(
            상품수=("pdid", "count"),
            평균가격=("sale_price", "mean"),
            대표브랜드=("brand", lambda x: ", ".join(x.value_counts().head(3).index.tolist())),
        )
        .reset_index()
    )
    cat_stats.columns = ["세부카테고리", "상품수", "평균가격", "대표브랜드"]
    cat_stats["평균가격"] = cat_stats["평균가격"].round(0).astype(int)
    cat_stats = cat_stats.sort_values("상품수", ascending=False).reset_index(drop=True)

    return cat_stats


def generate_hsmoa_sku_suggestions(
    products_df: pd.DataFrame,
    sku_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    홈쇼핑 인기 상품/브랜드 vs 현재 SKU 대조 → SKU 추가 제안

    [로직]
    1. 홈쇼핑에서 인기 있는 브랜드/상품 식별
    2. 현재 SKU 상품명에 해당 브랜드/키워드가 있는지 검사
    3. 없으면 = 홈쇼핑에서 잘 나가는데 우리가 안 갖고 있음 → 추가 제안
    """
    if products_df.empty or sku_df.empty:
        return pd.DataFrame()

    # 브랜드별 분석
    brand_analysis = analyze_trending_brands(products_df)
    suggestions = []

    for _, row in brand_analysis.iterrows():
        brand = row["brand"]
        if not brand or len(brand) < 2:
            continue

        # enriched_keyword_match: 동의어 + 성분 매핑 포함 매칭
        matched_skus = enriched_keyword_match(brand, sku_df)
        has_brand = len(matched_skus) > 0

        if not has_brand and row["등장횟수"] >= 2:
            suggestions.append({
                "유형": "브랜드",
                "추천항목": brand,
                "근거": "홈쇼핑 {}회 등장, 리뷰 {:.1f}점".format(
                    row["등장횟수"], row["리뷰평균"]
                ),
                "카테고리": row["주요카테고리"],
                "평균가격": "{:,.0f}원".format(row["평균가격"]),
                "대표상품": row["상품목록"][:80],
                "현재보유": "❌ 미보유",
            })
        elif has_brand and row["등장횟수"] >= 2:
            # 보유하고 있지만 성분/동의어 매칭인 경우 → 정보 표시
            pass  # 보유 중이면 제안하지 않음

    # 카테고리(세부분류) 분석 - 홈쇼핑에서 많이 팔리는데 우리가 SKU 적은 분류
    cat_analysis = analyze_trending_categories(products_df)
    for _, row in cat_analysis.iterrows():
        cat3 = row["세부카테고리"]
        if not cat3 or row["상품수"] < 3:
            continue

        # enriched_keyword_match: 동의어 + 성분 매핑 포함 매칭
        matched_skus = enriched_keyword_match(cat3, sku_df)
        matched_count = len(matched_skus)

        # 카테고리명이 길면 일부만으로도 매칭 시도
        if matched_count == 0:
            cat_short = cat3.replace("/", "").replace(" ", "")[:4]
            if len(cat_short) >= 2:
                matched_skus = enriched_keyword_match(cat_short, sku_df)
                matched_count = len(matched_skus)

        if matched_count < 3:
            suggestions.append({
                "유형": "카테고리",
                "추천항목": cat3,
                "근거": "홈쇼핑 인기 상품 {}개, 대표: {}".format(
                    row["상품수"], row["대표브랜드"]
                ),
                "카테고리": cat3,
                "평균가격": "{:,.0f}원".format(row["평균가격"]),
                "대표상품": row["대표브랜드"],
                "현재보유": "{}개 SKU".format(matched_count) if matched_count > 0 else "❌ 미보유",
            })

    result = pd.DataFrame(suggestions)
    if not result.empty:
        result = result.sort_values("유형").reset_index(drop=True)
    return result


# ──────────────────────────────────────
# 4. 통합 데이터 수집
# ──────────────────────────────────────


def fetch_all_hsmoa_data(force_refresh: bool = False) -> Dict:
    """
    홈쇼핑모아 전체 데이터 수집

    Returns:
        {
            "products": pd.DataFrame,     # 건강식품 인기 상품
            "popular_ranking": list,       # 전체 인기 랭킹 (trend API)
            "fetched_at": str,
        }
    """
    result = {
        "products": pd.DataFrame(),
        "popular_ranking": [],
        "fetched_at": datetime.now().isoformat(),
    }

    # 1) hsmoa.com 검색 기반 건강식품 데이터
    print("[홈쇼핑모아] 건강식품 검색 데이터 수집 중...")
    products = fetch_health_products(force_refresh=force_refresh)
    result["products"] = products
    print("  → {}개 건강식품 상품 수집".format(len(products)))

    # 2) trend.hsmoa-ad.com 인기 랭킹 (인증 필요)
    if is_hsmoa_configured():
        print("[홈쇼핑모아] 트렌드 사이트 인기 랭킹 수집 중...")
        cache_key = "popular_ranking"
        if not force_refresh and is_cache_valid(cache_key):
            cached = load_cache(cache_key)
            if cached:
                result["popular_ranking"] = cached
                print("  → 캐시에서 로드 ({}개)".format(len(cached)))
                return result

        client = HsmoaTrendClient()
        if client.login():
            ranking = client.get_popular_products(days_back=30)
            result["popular_ranking"] = ranking
            if ranking:
                save_cache(cache_key, ranking)
            print("  → {}개 인기 상품 랭킹".format(len(ranking)))
    else:
        print("[홈쇼핑모아] 계정 미설정 - 검색 데이터만 사용")

    return result


# ──────────────────────────────────────
# 테스트
# ──────────────────────────────────────

if __name__ == "__main__":
    print("=" * 50)
    print("홈쇼핑모아 트렌드 분석 테스트")
    print("=" * 50)

    # 검색 테스트 (키워드 3개만)
    test_keywords = ["비타민", "오메가3", "유산균"]
    print("\n[1] 검색 테스트 (키워드: {})".format(test_keywords))
    products = fetch_health_products(keywords=test_keywords, force_refresh=True)
    print("  수집된 상품: {}개".format(len(products)))

    if not products.empty:
        print("\n[2] 인기 브랜드 분석")
        brands = analyze_trending_brands(products)
        print(brands.head(10).to_string(index=False))

        print("\n[3] 인기 카테고리 분석")
        cats = analyze_trending_categories(products)
        print(cats.to_string(index=False))

    # 트렌드 사이트 테스트
    if is_hsmoa_configured():
        print("\n[4] 트렌드 사이트 로그인 테스트")
        client = HsmoaTrendClient()
        if client.login():
            print("  카테고리 목록:")
            cats = client.get_categories()
            for c in cats:
                print("    [{id}] {name}".format(**c))

            print("\n  전체 인기 상품 TOP 5:")
            ranking = client.get_popular_products(days_back=7)
            for item in ranking[:5]:
                print("    {}. {}".format(item.get("rank"), item.get("result", "")))
