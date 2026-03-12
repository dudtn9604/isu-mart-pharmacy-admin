"""
Supabase ERP DB 연동 모듈
이수마트약국 ERP 데이터에 접근하기 위한 클라이언트

[주의사항]
1. 대부분의 쿼리에 store_id 필터 필수 (products 테이블 제외)
2. 1000행 이상 조회 시 .range() 페이지네이션 사용
3. erp_category만 사용 (toss 카테고리 번호 붙은 것 사용 금지)
4. SERVICE_ROLE_KEY는 서버 사이드에서만 사용

[테이블 구조]
- products (~2,000): store_id 없음, erp_category/erp_subcategory 사용
- toss_orders (~24,000): order_items는 JSON 배열로 임베디드
- sale_cost_records (~42,000): product_id로 products 조인, FIFO 원가/매출총이익
- receiving_lots: 입고/발주 기록
"""

import pandas as pd
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from trend_config import (
    SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY,
    SUPABASE_STORE_ID,
)

# ──────────────────────────────────────
# Supabase 클라이언트 싱글턴
# ──────────────────────────────────────
_client = None


def _get_client():
    """Supabase 클라이언트 싱글턴 반환"""
    global _client
    if _client is None:
        from supabase import create_client
        _client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    return _client


def is_supabase_configured() -> bool:
    """Supabase 설정이 완료되었는지 확인"""
    return bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY and SUPABASE_STORE_ID)


# ──────────────────────────────────────
# 페이지네이션 헬퍼
# ──────────────────────────────────────
def _fetch_all_rows(query_builder, page_size: int = 1000) -> List[Dict]:
    """
    1000행 제한을 우회하여 전체 데이터를 페이지네이션으로 가져오기
    주의: query_builder는 .range()를 아직 호출하지 않은 상태여야 함
    """
    all_rows = []
    offset = 0

    while True:
        # 매번 새로운 쿼리를 만들지 않고, range만 적용
        response = query_builder.range(offset, offset + page_size - 1).execute()
        rows = response.data
        all_rows.extend(rows)

        if len(rows) < page_size:
            break
        offset += page_size

    return all_rows


# ──────────────────────────────────────
# 테이블별 데이터 조회 함수
# ──────────────────────────────────────

def fetch_products() -> pd.DataFrame:
    """
    상품 마스터 조회 (products 테이블)
    주의: products 테이블에는 store_id 컬럼이 없음
    """
    client = _get_client()
    query = client.table("products").select("*")
    rows = _fetch_all_rows(query)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def fetch_orders(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> pd.DataFrame:
    """
    주문 데이터 조회 (toss_orders 테이블)
    date_from, date_to: 'YYYY-MM-DD' 형식
    order_items는 JSON 배열로 임베디드 (별도 테이블 아님)
    """
    client = _get_client()
    query = (
        client.table("toss_orders")
        .select("id, store_id, toss_order_id, order_number, order_date, "
                "order_state, total_amount, discount_amount, actual_amount, "
                "item_count, order_items, synced_at")
        .eq("store_id", SUPABASE_STORE_ID)
        .eq("order_state", "COMPLETED")
    )

    if date_from:
        query = query.gte("order_date", date_from)
    if date_to:
        query = query.lte("order_date", date_to + "T23:59:59")

    query = query.order("order_date", desc=True)
    rows = _fetch_all_rows(query)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    # 동일 주문이 중복 저장된 경우 제거 (동기화 중복 방지)
    if "toss_order_id" in df.columns:
        df = df.drop_duplicates(subset=["toss_order_id"], keep="last")
    return df


def fetch_sale_cost_records(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> pd.DataFrame:
    """
    매출원가 기록 조회 (sale_cost_records 테이블)
    product_id로 products 조인하여 상품 정보 연결 가능
    """
    client = _get_client()
    query = (
        client.table("sale_cost_records")
        .select("*")
        .eq("store_id", SUPABASE_STORE_ID)
    )

    if date_from:
        query = query.gte("sale_date", date_from)
    if date_to:
        query = query.lte("sale_date", date_to)

    rows = _fetch_all_rows(query)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def fetch_receiving_lots(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> pd.DataFrame:
    """입고/발주 기록 조회 (receiving_lots 테이블)"""
    client = _get_client()
    query = (
        client.table("receiving_lots")
        .select("*")
        .eq("store_id", SUPABASE_STORE_ID)
    )

    if date_from:
        query = query.gte("received_date", date_from)
    if date_to:
        query = query.lte("received_date", date_to)

    rows = _fetch_all_rows(query)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def fetch_store_info() -> Dict:
    """매장 정보 조회"""
    client = _get_client()
    response = (
        client.table("stores")
        .select("*")
        .eq("id", SUPABASE_STORE_ID)
        .execute()
    )
    if response.data:
        return response.data[0]
    return {}


# ──────────────────────────────────────
# Toss 카테고리 → ERP 카테고리 매핑
# ──────────────────────────────────────
TOSS_TO_ERP_CATEGORY = {
    "01 감기.해열.진통": "감기약",
    "02 알레르기/비염/안약": "알레르기・비염약",
    "03 소화. 장 건강": "위장 건강",
    "04 피로회복 에너지": "피로회복・종합영양",
    "05 근육.파스": "근육・파스",
    "06 상처/피부질환": "피부 건강",
    "07 여성건강": "여성건강",
    "08 생활건강": "생활건강",
    "09 한방케어": "한방 영양",
    "10 뇌건강/스트레스/수면": "뇌건강・수면・스트레스",
    "12 반려동물": "반려동물",
    "13 구강": "구강",
    "13 눈.관절": "관절",
    "14 다이어트/혈당/뷰티": "이너뷰티",
    "16 어린이 건강": "어린이 건강",
    "동물약": "반려동물",
    "변비/정맥순환": "위장 건강",
    "조제": "조제",
    "기본": "기타",
    "기타": "기타",
    "세트": "기타",
}


def _normalize_category(cat_str: str) -> str:
    """
    toss_orders의 category 문자열을 ERP 카테고리로 정규화
    1. _비적립/_적립 접미사 제거 (ERP 형식)
    2. Toss 번호 접두사 형식 → ERP 카테고리 매핑
    """
    if not cat_str or pd.isna(cat_str):
        return "기타"

    # 먼저 _비적립/_적립 접미사 제거 시도
    cleaned = cat_str.replace("_비적립", "").replace("_적립", "")

    # Toss 형식인지 확인 (숫자로 시작)
    if cleaned in TOSS_TO_ERP_CATEGORY:
        return TOSS_TO_ERP_CATEGORY[cleaned]

    # 이미 ERP 형식이면 그대로 반환
    return cleaned


# ──────────────────────────────────────
# 주문 아이템 플래트닝 (toss_orders → 개별 아이템)
# ──────────────────────────────────────
def flatten_order_items(
    orders_df: pd.DataFrame,
    products_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    toss_orders의 임베디드 order_items JSON을 플래트닝하여
    개별 상품 단위의 DataFrame으로 변환

    Args:
        orders_df: toss_orders DataFrame
        products_df: products DataFrame (선택사항, product_id → erp_category 매핑용)

    Returns: DataFrame with columns:
        order_id, order_date, order_datetime_kst, product_id, product_name,
        erp_category, quantity, unit_price, total_price
    """
    if orders_df.empty:
        return pd.DataFrame()

    # product_id → erp_category / erp_subcategory 매핑 (products 테이블 기반, 가장 정확)
    product_category_map = {}
    product_subcategory_map = {}
    if products_df is not None and not products_df.empty:
        for _, p in products_df.iterrows():
            if p.get("id") and p.get("erp_category"):
                product_category_map[p["id"]] = p["erp_category"]
            if p.get("id") and p.get("erp_subcategory"):
                product_subcategory_map[p["id"]] = p["erp_subcategory"]

    rows = []
    for _, order in orders_df.iterrows():
        items = order.get("order_items", [])
        if not items:
            continue

        order_dt = pd.to_datetime(order["order_date"])
        # UTC → KST (+9시간)
        order_dt_kst = order_dt + timedelta(hours=9)
        order_date_str = order_dt_kst.strftime("%Y-%m-%d")

        for item in items:
            product_id = item.get("productId", "")
            raw_category = item.get("category", "")

            # erp_category 결정: product_id 매핑 > 카테고리 정규화
            if product_id in product_category_map:
                erp_cat = product_category_map[product_id]
            else:
                erp_cat = _normalize_category(raw_category)

            erp_subcat = product_subcategory_map.get(product_id, "")

            rows.append({
                "order_id": order.get("toss_order_id", ""),
                "order_date": order_date_str,
                "order_datetime_kst": order_dt_kst,
                "product_id": product_id,
                "product_name": item.get("name", ""),
                "raw_category": raw_category,
                "erp_category": erp_cat,
                "erp_subcategory": erp_subcat,
                "quantity": item.get("quantity", 0),
                "unit_price": item.get("price", 0),
                "total_price": item.get("totalPrice", 0),
            })

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    result["order_date"] = pd.to_datetime(result["order_date"])

    return result
