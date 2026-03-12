"""
교차판매(크로스셀링) 분석 모듈
- 주문 단위 동시구매 분석 (Apriori 불필요, 단순 동시출현 카운팅)
- support / confidence / lift 지표 계산
- 매대 배치 제안 (물리적 거리 기반)
"""

import pandas as pd
import numpy as np
from itertools import combinations
from typing import Optional

# 제외 카테고리/상품 (조제, 키인결제)
EXCLUDE_CATEGORIES = ["조제"]
EXCLUDE_PRODUCTS = ["키인결제"]


# ──────────────────────────────────────
# Step 1: 바스켓 데이터 준비
# ──────────────────────────────────────

def prepare_basket_data(
    date_from: str,
    date_to: str,
) -> pd.DataFrame:
    """
    주문 데이터 로드 → 플래트닝 → 조제/키인결제 제외
    Returns: DataFrame[order_id, product_name, product_id, erp_category, quantity]
    """
    from supabase_client import fetch_orders, flatten_order_items, fetch_products

    products = fetch_products()
    orders = fetch_orders(date_from=date_from, date_to=date_to)

    if orders.empty:
        return pd.DataFrame()

    items = flatten_order_items(orders, products_df=products)

    if items.empty:
        return pd.DataFrame()

    # 조제/키인결제 제외
    items = items[~items["erp_category"].isin(EXCLUDE_CATEGORIES)]
    items = items[~items["product_name"].isin(EXCLUDE_PRODUCTS)]

    # 필요 컬럼만 반환
    cols = ["order_id", "product_name", "product_id", "erp_category", "quantity"]
    return items[cols].copy()


# ──────────────────────────────────────
# Step 2: 동시출현 분석
# ──────────────────────────────────────

def compute_cooccurrence(
    items_df: pd.DataFrame,
    level: str = "product",
) -> pd.DataFrame:
    """
    주문별 동시출현 쌍 계산 + support/confidence/lift

    Args:
        items_df: prepare_basket_data() 결과
        level: "product" (상품명 기준) 또는 "category" (erp_category 기준)

    Returns: DataFrame[item_a, item_b, count, support_ab, support_a, support_b,
                        confidence_a_to_b, confidence_b_to_a, lift]
    """
    if items_df.empty:
        return pd.DataFrame()

    col = "product_name" if level == "product" else "erp_category"

    # 주문별 고유 아이템
    basket = items_df.groupby("order_id")[col].apply(lambda x: frozenset(x.unique())).reset_index()
    basket.columns = ["order_id", "items"]

    total_orders = len(basket)
    if total_orders == 0:
        return pd.DataFrame()

    # 개별 아이템 support (출현 주문 수)
    item_counts = {}
    pair_counts = {}

    for _, row in basket.iterrows():
        items = row["items"]
        for item in items:
            item_counts[item] = item_counts.get(item, 0) + 1
        if len(items) >= 2:
            for a, b in combinations(sorted(items), 2):
                pair = (a, b)
                pair_counts[pair] = pair_counts.get(pair, 0) + 1

    if not pair_counts:
        return pd.DataFrame()

    rows = []
    for (a, b), count in pair_counts.items():
        support_ab = count / total_orders
        support_a = item_counts[a] / total_orders
        support_b = item_counts[b] / total_orders
        conf_a_to_b = count / item_counts[a] if item_counts[a] > 0 else 0
        conf_b_to_a = count / item_counts[b] if item_counts[b] > 0 else 0
        lift = support_ab / (support_a * support_b) if (support_a * support_b) > 0 else 0

        rows.append({
            "item_a": a,
            "item_b": b,
            "count": count,
            "support_ab": round(support_ab, 6),
            "support_a": round(support_a, 6),
            "support_b": round(support_b, 6),
            "confidence_a_to_b": round(conf_a_to_b, 4),
            "confidence_b_to_a": round(conf_b_to_a, 4),
            "lift": round(lift, 4),
        })

    result = pd.DataFrame(rows)
    result = result.sort_values("lift", ascending=False).reset_index(drop=True)
    return result


# ──────────────────────────────────────
# Step 3: 특정 상품 교차판매 후보
# ──────────────────────────────────────

def get_cross_sell_candidates(
    items_df: pd.DataFrame,
    target_product: str,
    cooccurrence_df: Optional[pd.DataFrame] = None,
    top_n: int = 15,
    min_count: int = 3,
) -> pd.DataFrame:
    """
    특정 상품과 함께 구매된 상품 목록 + 지표

    Args:
        items_df: prepare_basket_data() 결과
        target_product: 타깃 상품명
        cooccurrence_df: 사전 계산된 동시출현 결과 (없으면 자동 계산)
        top_n: 반환할 상위 N개
        min_count: 최소 동시구매 횟수

    Returns: DataFrame[product, category, count, confidence, lift]
    """
    if cooccurrence_df is None:
        cooccurrence_df = compute_cooccurrence(items_df, level="product")

    if cooccurrence_df.empty:
        return pd.DataFrame()

    # 타깃 상품이 포함된 쌍 필터
    mask_a = cooccurrence_df["item_a"] == target_product
    mask_b = cooccurrence_df["item_b"] == target_product

    matches_a = cooccurrence_df[mask_a].copy()
    matches_b = cooccurrence_df[mask_b].copy()

    # 통합: a→b일 때 상대방은 b, b→a일 때 상대방은 a
    rows = []
    for _, r in matches_a.iterrows():
        rows.append({
            "product": r["item_b"],
            "count": r["count"],
            "confidence": r["confidence_a_to_b"],
            "lift": r["lift"],
        })
    for _, r in matches_b.iterrows():
        rows.append({
            "product": r["item_a"],
            "count": r["count"],
            "confidence": r["confidence_b_to_a"],
            "lift": r["lift"],
        })

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)

    # 카테고리 매핑
    cat_map = items_df.drop_duplicates("product_name").set_index("product_name")["erp_category"].to_dict()
    result["category"] = result["product"].map(cat_map).fillna("")

    # 필터
    result = result[result["count"] >= min_count]
    result = result.sort_values("lift", ascending=False).head(top_n)
    result = result[["product", "category", "count", "confidence", "lift"]].reset_index(drop=True)
    return result


# ──────────────────────────────────────
# Step 4: 카테고리 교차분석
# ──────────────────────────────────────

def get_category_cross_sell(
    items_df: pd.DataFrame,
    target_category: Optional[str] = None,
) -> pd.DataFrame:
    """
    카테고리 레벨 교차분석

    Args:
        items_df: prepare_basket_data() 결과
        target_category: 특정 카테고리만 (None이면 전체 카테고리 쌍)

    Returns: DataFrame[item_a, item_b, count, support_ab, confidence_a_to_b, lift]
    """
    cooc = compute_cooccurrence(items_df, level="category")

    if cooc.empty:
        return pd.DataFrame()

    if target_category:
        mask = (cooc["item_a"] == target_category) | (cooc["item_b"] == target_category)
        cooc = cooc[mask]

    return cooc


def get_products_by_category_pair(
    items_df: pd.DataFrame,
    cat_a: str,
    cat_b: str,
    top_n: int = 20,
    min_count: int = 3,
) -> pd.DataFrame:
    """
    두 카테고리 간 교차구매된 구체적 상품 쌍 반환

    Args:
        items_df: prepare_basket_data() 결과
        cat_a, cat_b: 카테고리 쌍
        top_n: 상위 N개

    Returns: DataFrame[product_a, cat_a, product_b, cat_b, count, lift]
    """
    if items_df.empty:
        return pd.DataFrame()

    # 주문별 상품 그룹
    basket = items_df.groupby("order_id").apply(
        lambda g: list(zip(g["product_name"], g["erp_category"]))
    ).reset_index(name="items")

    total_orders = len(basket)
    if total_orders == 0:
        return pd.DataFrame()

    # 개별 상품 출현 횟수
    product_counts = items_df.groupby("product_name")["order_id"].nunique().to_dict()

    pair_counts = {}
    for _, row in basket.iterrows():
        items = row["items"]
        # cat_a 상품과 cat_b 상품 분리
        prods_a = set(p for p, c in items if c == cat_a)
        prods_b = set(p for p, c in items if c == cat_b)

        if cat_a == cat_b:
            # 같은 카테고리 내 쌍
            for a, b in combinations(sorted(prods_a), 2):
                pair_counts[(a, b)] = pair_counts.get((a, b), 0) + 1
        else:
            for a in prods_a:
                for b in prods_b:
                    key = tuple(sorted([a, b]))
                    pair_counts[key] = pair_counts.get(key, 0) + 1

    if not pair_counts:
        return pd.DataFrame()

    # 카테고리 매핑
    cat_map = items_df.drop_duplicates("product_name").set_index("product_name")["erp_category"].to_dict()

    rows = []
    for (a, b), count in pair_counts.items():
        sup_a = product_counts.get(a, 0) / total_orders
        sup_b = product_counts.get(b, 0) / total_orders
        sup_ab = count / total_orders
        lift = sup_ab / (sup_a * sup_b) if (sup_a * sup_b) > 0 else 0
        conf_a = count / product_counts.get(a, 1)
        conf_b = count / product_counts.get(b, 1)
        rows.append({
            "product_a": a,
            "cat_a": cat_map.get(a, ""),
            "product_b": b,
            "cat_b": cat_map.get(b, ""),
            "count": count,
            "confidence_a": round(conf_a, 4),
            "confidence_b": round(conf_b, 4),
            "lift": round(lift, 2),
        })

    result = pd.DataFrame(rows)
    result = result[result["count"] >= min_count]
    result = result.sort_values("count", ascending=False).head(top_n).reset_index(drop=True)
    return result


def _compute_category_product_count(
    items_df: pd.DataFrame,
    min_count: int = 3,
) -> pd.DataFrame:
    """
    상품 쌍 레벨에서 min_count 이상인 쌍만 추려,
    카테고리 쌍별로 합산한 동시구매 건수를 반환

    Returns: DataFrame[cat_a, cat_b, count]
    """
    product_cooc = compute_cooccurrence(items_df, level="product")
    if product_cooc.empty:
        return pd.DataFrame()

    # min_count 이상인 상품 쌍만
    product_cooc = product_cooc[product_cooc["count"] >= min_count]
    if product_cooc.empty:
        return pd.DataFrame()

    # 상품명 → 카테고리 매핑
    cat_map = items_df.drop_duplicates("product_name").set_index("product_name")["erp_category"].to_dict()

    rows = []
    for _, r in product_cooc.iterrows():
        cat_a = cat_map.get(r["item_a"], "")
        cat_b = cat_map.get(r["item_b"], "")
        if not cat_a or not cat_b:
            continue
        key = tuple(sorted([cat_a, cat_b]))
        rows.append({"cat_a": key[0], "cat_b": key[1], "count": r["count"]})

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # 같은 카테고리 쌍의 상품 쌍 건수 합산
    df = df.groupby(["cat_a", "cat_b"], as_index=False)["count"].sum()
    return df


def get_category_heatmap_data(
    items_df: pd.DataFrame,
    metric: str = "lift",
    min_count: int = 3,
) -> pd.DataFrame:
    """
    카테고리 × 카테고리 매트릭스 반환 (heatmap용)

    Args:
        metric: "lift", "confidence", 또는 "count"
        min_count: 동시구매 횟수가 이 값 이상인 쌍만 포함 (기본 3)

    count 메트릭: 상품 쌍 레벨에서 min_count 이상인 쌍의 건수를 카테고리별 합산
    lift/confidence: 카테고리 레벨 동시출현 기반 (min_count 필터 적용)
    """
    cooc = compute_cooccurrence(items_df, level="category")

    if cooc.empty:
        return pd.DataFrame()

    categories = sorted(set(cooc["item_a"].tolist() + cooc["item_b"].tolist()))

    if metric == "count":
        matrix = pd.DataFrame(0, index=categories, columns=categories)
        for _, r in cooc.iterrows():
            matrix.loc[r["item_a"], r["item_b"]] = r["count"]
            matrix.loc[r["item_b"], r["item_a"]] = r["count"]
        return matrix

    if metric == "confidence":
        matrix = pd.DataFrame(0.0, index=categories, columns=categories)
        for _, r in cooc.iterrows():
            matrix.loc[r["item_a"], r["item_b"]] = r["confidence_a_to_b"]
            matrix.loc[r["item_b"], r["item_a"]] = r["confidence_b_to_a"]
        for c in categories:
            matrix.loc[c, c] = 1.0
    else:
        matrix = pd.DataFrame(1.0, index=categories, columns=categories)
        for _, r in cooc.iterrows():
            matrix.loc[r["item_a"], r["item_b"]] = r["lift"]
            matrix.loc[r["item_b"], r["item_a"]] = r["lift"]

    return matrix


# ──────────────────────────────────────
# Step 5: 배치 제안 (인접 매대 기반)
# ──────────────────────────────────────

def _build_adjacency_map(fixture_positions_df: pd.DataFrame):
    """
    fixture_positions를 x_pos 기준으로 열(column) 그룹핑 후,
    같은 열 내에서 y_pos 순서로 정렬하여 인접 매대 관계를 구축.
    Returns: dict[(shelf_type, fixture_no)] → set of (shelf_type, fixture_no)
    """
    if fixture_positions_df.empty:
        return {}

    fp = fixture_positions_df.copy()
    fp["fixture_key"] = list(zip(fp["shelf_type"], fp["fixture_no"].astype(int)))

    # x_pos 기준으로 열 그룹핑 (±300mm 이내면 같은 열)
    fp = fp.sort_values("x_pos")
    columns = []  # list of lists
    for _, row in fp.iterrows():
        placed = False
        for col in columns:
            if abs(row["x_pos"] - col[0]["x_pos"]) <= 300:
                col.append(row)
                placed = True
                break
        if not placed:
            columns.append([row])

    adjacency = {}
    for col in columns:
        # 같은 열 내에서 y_pos 순 정렬
        col_sorted = sorted(col, key=lambda r: r["y_pos"])
        keys = [r["fixture_key"] for r in col_sorted]
        for i, key in enumerate(keys):
            neighbors = set()
            if i > 0:
                neighbors.add(keys[i - 1])
            if i < len(keys) - 1:
                neighbors.add(keys[i + 1])
            if key in adjacency:
                adjacency[key].update(neighbors)
            else:
                adjacency[key] = neighbors

    return adjacency


def generate_placement_suggestions(
    cross_sell_df: pd.DataFrame,
    placements_df: pd.DataFrame,
    fixture_positions_df: pd.DataFrame,
    target_product: str,
) -> pd.DataFrame:
    """
    교차판매 상위 상품의 현재 배치 위치 확인 + 인접 매대 여부 판정

    Args:
        cross_sell_df: get_cross_sell_candidates() 결과
        placements_df: get_current_placements() 결과
        fixture_positions_df: get_fixture_positions() 결과
        target_product: 타깃 상품명

    Returns: DataFrame[product, category, lift, count,
                        target_location, product_location, is_adjacent]
    """
    if cross_sell_df.empty or placements_df.empty or fixture_positions_df.empty:
        return pd.DataFrame()

    # 인접 매대 맵 구축
    adjacency = _build_adjacency_map(fixture_positions_df)

    # 상품명 → (display_label, fixture_key) 매핑
    def _get_product_fixture(product_name):
        match = placements_df[placements_df["product_name"] == product_name]
        if match.empty:
            return None, None
        row = match.iloc[0]
        key = (row["shelf_type"], int(row["fixture_no"]))
        return row["display_label"], key

    target_label, target_key = _get_product_fixture(target_product)

    rows = []
    for _, cs in cross_sell_df.iterrows():
        prod = cs["product"]
        prod_label, prod_key = _get_product_fixture(prod)

        # 인접 판정: 같은 매대 또는 인접 매대
        is_adjacent = None
        if target_key and prod_key:
            if target_key == prod_key:
                is_adjacent = True  # 같은 매대
            else:
                is_adjacent = prod_key in adjacency.get(target_key, set())

        rows.append({
            "product": prod,
            "category": cs.get("category", ""),
            "lift": cs["lift"],
            "count": cs["count"],
            "target_location": target_label or "미배치",
            "product_location": prod_label or "미배치",
            "is_adjacent": is_adjacent,
        })

    return pd.DataFrame(rows)
