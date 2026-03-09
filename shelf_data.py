"""
Shelf 배치 데이터 관리 모듈
SQLite CRUD + Supabase 매출 연동
"""

import sqlite3
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Dict, Any

from shelf_config import SHELF_CONFIGS, generate_display_label

DB_PATH = Path(__file__).parent / "shelf_data.db"
DIMS_EXCEL = Path(__file__).parent / "skuc치수.xlsx"


# ──────────────────────────────────────
# Supabase 클라이언트 (배치 데이터 공유용)
# ──────────────────────────────────────
_sb_client = None


def _get_sb():
    """Supabase 클라이언트 반환 (없으면 None)"""
    global _sb_client
    if _sb_client is None:
        try:
            from trend_config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
            if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
                from supabase import create_client
                _sb_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        except Exception:
            pass
    return _sb_client


# ──────────────────────────────────────
# DB 초기화
# ──────────────────────────────────────

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    """테이블 생성 + shelf_locations 시드"""
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS shelf_locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shelf_type TEXT NOT NULL,
            fixture_no INTEGER NOT NULL,
            tier INTEGER NOT NULL,
            tier_height INTEGER NOT NULL,
            display_label TEXT NOT NULL,
            UNIQUE(shelf_type, fixture_no, tier)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS product_dimensions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL UNIQUE,
            width REAL,
            height REAL,
            depth REAL,
            size_class TEXT,
            dual_row INTEGER DEFAULT 0,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS shelf_placements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shelf_location_id INTEGER NOT NULL REFERENCES shelf_locations(id),
            product_name TEXT NOT NULL,
            product_id TEXT,
            erp_category TEXT,
            start_date DATE NOT NULL,
            end_date DATE,
            notes TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 마이그레이션: position, span 컬럼 추가 (레거시)
    try:
        cur.execute("ALTER TABLE shelf_placements ADD COLUMN position INTEGER NOT NULL DEFAULT 1")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE shelf_placements ADD COLUMN span INTEGER NOT NULL DEFAULT 1")
    except Exception:
        pass
    # 마이그레이션: position_start, position_end 컬럼 추가 (range 기반)
    try:
        cur.execute("ALTER TABLE shelf_placements ADD COLUMN position_start INTEGER NOT NULL DEFAULT 1")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE shelf_placements ADD COLUMN position_end INTEGER NOT NULL DEFAULT 1")
    except Exception:
        pass

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fixture_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shelf_type TEXT NOT NULL,
            fixture_no INTEGER NOT NULL,
            x_pos REAL NOT NULL DEFAULT 0,
            y_pos REAL NOT NULL DEFAULT 0,
            orientation TEXT NOT NULL DEFAULT 'V',
            zone TEXT,
            custom_label TEXT,
            UNIQUE(shelf_type, fixture_no)
        )
    """)

    # 마이그레이션: shelf_locations에 enabled 칼럼 추가
    try:
        cur.execute("ALTER TABLE shelf_locations ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1")
    except Exception:
        pass

    # 시드: 이미 존재하면 스킵
    existing = cur.execute("SELECT COUNT(*) FROM shelf_locations").fetchone()[0]
    if existing == 0:
        _seed_locations(cur)

    # 배치도 시드
    fp_count = cur.execute("SELECT COUNT(*) FROM fixture_positions").fetchone()[0]
    if fp_count == 0:
        _seed_fixture_positions(cur)

    # 치수 데이터 시드: Excel이 있고 테이블이 비어있으면 자동 로드
    dims_count = cur.execute("SELECT COUNT(*) FROM product_dimensions").fetchone()[0]
    if dims_count == 0 and DIMS_EXCEL.exists():
        _seed_dimensions(cur)

    conn.commit()
    conn.close()


def _seed_locations(cur):
    """매대 구조에 따라 shelf_locations 시드 데이터 삽입 (단면)"""
    for shelf_type, cfg in SHELF_CONFIGS.items():
        tiers = cfg["tiers"]
        count = cfg["count"]
        for fixture_no in range(1, count + 1):
            for tier_idx, tier_height in enumerate(tiers):
                tier = tier_idx + 1  # 1-based
                label = generate_display_label(shelf_type, fixture_no, tier)
                cur.execute(
                    """INSERT INTO shelf_locations
                       (shelf_type, fixture_no, tier, tier_height, display_label)
                       VALUES (?, ?, ?, ?, ?)""",
                    (shelf_type, fixture_no, tier, tier_height, label),
                )


def _seed_fixture_positions(cur):
    """도면 기반 매대 초기 배치 시드 (mm 단위, 상단 우측부터 번호)"""
    # 매장: 12,215mm × 17,848mm
    # 입구(AUTO DOOR): 상단 중앙
    # 매대는 3개 존에 배치

    # 도면 분석 결과:
    # Zone 1 (상단, 입구 근처): y≈12500~16500
    # Zone 2 (중단): y≈8500~11500
    # Zone 3 (하단, POS 근처): y≈5000~8000

    # 매대 물리 크기(mm): A=900×360, B=930×360, C=636×360
    # V(수직) = 너비가 Y축 방향, H(수평) = 너비가 X축 방향

    positions = []
    # 번호는 상단 우측부터 부여
    # 우측→좌측, 위→아래 순서로 번호 부여

    # ── Zone 1 (상단) ──
    # 4열 × 4~5행, 열은 x 방향, 행은 y 방향
    zone1_cols = [5400, 4000, 2600, 1500]  # x좌표 (우→좌)
    zone1_y_start = 16200
    zone1_fixtures = [
        # col0 (x=5400, 우측열): A, A, B, A
        [("A", "V"), ("A", "V"), ("B", "V"), ("A", "V")],
        # col1 (x=4000): A, B, A, B, C
        [("A", "V"), ("B", "V"), ("A", "V"), ("B", "V"), ("C", "V")],
        # col2 (x=2600): A, A, B, A, C
        [("A", "V"), ("A", "V"), ("B", "V"), ("A", "V"), ("C", "V")],
        # col3 (x=1500): C, C, C
        [("C", "V"), ("C", "V"), ("C", "V")],
    ]

    for ci, col_x in enumerate(zone1_cols):
        fixtures = zone1_fixtures[ci]
        for ri, (stype, orient) in enumerate(fixtures):
            w = SHELF_CONFIGS[stype]["width"] * 10  # cm→mm
            y = zone1_y_start - ri * (w + 100)
            positions.append((stype, col_x, y, orient, "상단"))

    # ── Zone 2 (중단) ──
    zone2_cols = [5000, 3600, 2200, 1500]
    zone2_y_start = 11500
    zone2_fixtures = [
        [("A", "V"), ("B", "V"), ("A", "V")],
        [("A", "V"), ("A", "V"), ("B", "V"), ("A", "V")],
        [("B", "V"), ("A", "V"), ("B", "V"), ("C", "V")],
        [("C", "V"), ("C", "V"), ("B", "V")],
    ]

    for ci, col_x in enumerate(zone2_cols):
        fixtures = zone2_fixtures[ci]
        for ri, (stype, orient) in enumerate(fixtures):
            w = SHELF_CONFIGS[stype]["width"] * 10
            y = zone2_y_start - ri * (w + 100)
            positions.append((stype, col_x, y, orient, "중단"))

    # ── Zone 3 (하단) ──
    zone3_cols = [7500, 6100, 4700, 3300, 1800]
    zone3_y_start = 7800
    zone3_fixtures = [
        [("A", "V"), ("B", "V"), ("A", "V"), ("C", "V")],
        [("B", "V"), ("A", "V"), ("B", "V"), ("C", "V")],
        [("A", "V"), ("A", "V"), ("B", "V"), ("B", "V")],
        [("A", "V"), ("B", "V"), ("A", "V"), ("C", "V")],
        [("C", "V"), ("C", "V"), ("C", "V")],
    ]

    for ci, col_x in enumerate(zone3_cols):
        fixtures = zone3_fixtures[ci]
        for ri, (stype, orient) in enumerate(fixtures):
            w = SHELF_CONFIGS[stype]["width"] * 10
            y = zone3_y_start - ri * (w + 100)
            positions.append((stype, col_x, y, orient, "하단"))

    # 타입별 번호 부여 (상단 우측부터: y 큰 순 → x 큰 순)
    positions.sort(key=lambda p: (-p[2], -p[1]))

    counters = {"A": 0, "B": 0, "C": 0}
    for stype, x, y, orient, zone in positions:
        counters[stype] += 1
        cur.execute(
            """INSERT OR IGNORE INTO fixture_positions
               (shelf_type, fixture_no, x_pos, y_pos, orientation, zone)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (stype, counters[stype], x, y, orient, zone),
        )


# ──────────────────────────────────────
# 배치도 조회/관리
# ──────────────────────────────────────

def get_fixture_positions() -> pd.DataFrame:
    sb = _get_sb()
    if sb:
        try:
            r = sb.table('fixture_positions').select('*').order('shelf_type').order('fixture_no').execute()
            return pd.DataFrame(r.data) if r.data else pd.DataFrame()
        except Exception:
            pass
    conn = _get_conn()
    df = pd.read_sql_query(
        "SELECT * FROM fixture_positions ORDER BY shelf_type, fixture_no", conn
    )
    conn.close()
    return df


def update_fixture_position(
    shelf_type: str, fixture_no: int,
    x_pos: float, y_pos: float,
    orientation: str = "V",
    zone: str = "",
    custom_label: str = "",
):
    sb = _get_sb()
    if sb:
        try:
            sb.table('fixture_positions').update({
                'x_pos': x_pos, 'y_pos': y_pos, 'orientation': orientation,
                'zone': zone, 'custom_label': custom_label,
            }).eq('shelf_type', shelf_type).eq('fixture_no', fixture_no).execute()
            return
        except Exception:
            pass
    conn = _get_conn()
    conn.execute(
        """UPDATE fixture_positions
           SET x_pos=?, y_pos=?, orientation=?, zone=?, custom_label=?
           WHERE shelf_type=? AND fixture_no=?""",
        (x_pos, y_pos, orientation, zone, custom_label, shelf_type, fixture_no),
    )
    conn.commit()
    conn.close()


def swap_fixture_positions(
    type1: str, no1: int,
    type2: str, no2: int,
):
    """두 매대의 위치를 교환"""
    sb = _get_sb()
    if sb:
        try:
            r1 = sb.table('fixture_positions').select('x_pos,y_pos,orientation,zone').eq('shelf_type', type1).eq('fixture_no', no1).execute()
            r2 = sb.table('fixture_positions').select('x_pos,y_pos,orientation,zone').eq('shelf_type', type2).eq('fixture_no', no2).execute()
            if r1.data and r2.data:
                p1, p2 = r1.data[0], r2.data[0]
                sb.table('fixture_positions').update({
                    'x_pos': p2['x_pos'], 'y_pos': p2['y_pos'],
                    'orientation': p2['orientation'], 'zone': p2['zone'],
                }).eq('shelf_type', type1).eq('fixture_no', no1).execute()
                sb.table('fixture_positions').update({
                    'x_pos': p1['x_pos'], 'y_pos': p1['y_pos'],
                    'orientation': p1['orientation'], 'zone': p1['zone'],
                }).eq('shelf_type', type2).eq('fixture_no', no2).execute()
            return
        except Exception:
            pass
    conn = _get_conn()
    cur = conn.cursor()
    pos1 = cur.execute(
        "SELECT x_pos, y_pos, orientation, zone FROM fixture_positions WHERE shelf_type=? AND fixture_no=?",
        (type1, no1),
    ).fetchone()
    pos2 = cur.execute(
        "SELECT x_pos, y_pos, orientation, zone FROM fixture_positions WHERE shelf_type=? AND fixture_no=?",
        (type2, no2),
    ).fetchone()
    if pos1 and pos2:
        cur.execute(
            "UPDATE fixture_positions SET x_pos=?, y_pos=?, orientation=?, zone=? WHERE shelf_type=? AND fixture_no=?",
            (pos2[0], pos2[1], pos2[2], pos2[3], type1, no1),
        )
        cur.execute(
            "UPDATE fixture_positions SET x_pos=?, y_pos=?, orientation=?, zone=? WHERE shelf_type=? AND fixture_no=?",
            (pos1[0], pos1[1], pos1[2], pos1[3], type2, no2),
        )
    conn.commit()
    conn.close()


def bulk_update_fixture_positions(fixture_list: List[Dict[str, Any]]):
    """에디터에서 받은 매대 배치 데이터를 일괄 업데이트.
    fixture_list: [{'type':'A', 'no':1, 'x':1000, 'y':2000, 'orient':'V', 'zone':'상단', 'label':''}, ...]
    """
    sb = _get_sb()
    if sb:
        try:
            for fx in fixture_list:
                sb.table('fixture_positions').update({
                    'x_pos': fx["x"], 'y_pos': fx["y"],
                    'orientation': fx.get("orient", "V"),
                    'zone': fx.get("zone", ""),
                    'custom_label': fx.get("label", ""),
                }).eq('shelf_type', fx["type"]).eq('fixture_no', fx["no"]).execute()
            return
        except Exception:
            pass
    conn = _get_conn()
    cur = conn.cursor()
    for fx in fixture_list:
        cur.execute(
            """UPDATE fixture_positions
               SET x_pos=?, y_pos=?, orientation=?, zone=?, custom_label=?
               WHERE shelf_type=? AND fixture_no=?""",
            (fx["x"], fx["y"], fx.get("orient", "V"), fx.get("zone", ""),
             fx.get("label", ""), fx["type"], fx["no"]),
        )
    conn.commit()
    conn.close()


def _classify_size(height: float) -> str:
    """높이 기준 사이즈 분류 (선반 높이 25cm 기준, 여유 2cm)"""
    if height > 23:
        return "tall"    # 25cm 선반에 안 들어감 → A타입 5단(제한없음) 전용
    elif height > 15:
        return "medium"  # 일반 선반 OK
    else:
        return "short"   # 여유 있게 들어감


def _seed_dimensions(cur):
    """skuc치수.xlsx에서 치수 데이터 자동 로드"""
    try:
        df = pd.read_excel(str(DIMS_EXCEL), engine="openpyxl")
        # 컬럼 정리 (Unnamed 제거)
        df = df[[c for c in df.columns if not c.startswith("Unnamed")]]
        # 컬럼명 통일
        col_map = {}
        for c in df.columns:
            cl = c.strip()
            if cl in ("상품명",):
                col_map[c] = "상품명"
            elif cl in ("가로",):
                col_map[c] = "가로"
            elif cl in ("높이", "세로"):
                col_map[c] = "높이"
            elif cl in ("깊이", "폭"):
                col_map[c] = "깊이"
        df = df.rename(columns=col_map)

        for _, row in df.iterrows():
            name = row.get("상품명")
            w = row.get("가로")
            h = row.get("높이")
            d = row.get("깊이")
            if pd.isna(name) or pd.isna(h):
                continue
            w = float(w) if pd.notna(w) else None
            h = float(h)
            d = float(d) if pd.notna(d) else None
            size_class = _classify_size(h)
            dual_row = 1 if (d is not None and d <= 14.0) else 0
            cur.execute(
                """INSERT OR IGNORE INTO product_dimensions
                   (product_name, width, height, depth, size_class, dual_row)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (str(name), w, h, d, size_class, dual_row),
            )
    except Exception as e:
        print(f"치수 데이터 로드 실패: {e}")


# ──────────────────────────────────────
# 치수 조회/관리
# ──────────────────────────────────────

def get_all_dimensions() -> pd.DataFrame:
    sb = _get_sb()
    if sb:
        try:
            r = sb.table('product_dimensions').select('*').order('product_name').execute()
            return pd.DataFrame(r.data) if r.data else pd.DataFrame()
        except Exception:
            pass
    conn = _get_conn()
    df = pd.read_sql_query(
        "SELECT * FROM product_dimensions ORDER BY product_name", conn
    )
    conn.close()
    return df


def get_dimension(product_name: str) -> Optional[Dict]:
    sb = _get_sb()
    if sb:
        try:
            r = sb.table('product_dimensions').select('*').eq('product_name', product_name).execute()
            return r.data[0] if r.data else None
        except Exception:
            pass
    conn = _get_conn()
    row = conn.execute(
        "SELECT * FROM product_dimensions WHERE product_name = ?",
        (product_name,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def upsert_dimension(
    product_name: str,
    width: Optional[float],
    height: float,
    depth: Optional[float],
):
    """치수 추가 또는 업데이트"""
    size_class = _classify_size(height)
    dual_row = 1 if (depth is not None and depth <= 14.0) else 0
    sb = _get_sb()
    if sb:
        try:
            sb.table('product_dimensions').upsert({
                'product_name': product_name,
                'width': width, 'height': height, 'depth': depth,
                'size_class': size_class, 'dual_row': dual_row,
            }, on_conflict='product_name').execute()
            return
        except Exception:
            pass
    conn = _get_conn()
    conn.execute(
        """INSERT INTO product_dimensions
           (product_name, width, height, depth, size_class, dual_row, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(product_name) DO UPDATE SET
             width=excluded.width, height=excluded.height,
             depth=excluded.depth, size_class=excluded.size_class,
             dual_row=excluded.dual_row, updated_at=CURRENT_TIMESTAMP""",
        (product_name, width, height, depth, size_class, dual_row),
    )
    conn.commit()
    conn.close()


def delete_dimension(product_name: str):
    sb = _get_sb()
    if sb:
        try:
            sb.table('product_dimensions').delete().eq('product_name', product_name).execute()
            return
        except Exception:
            pass
    conn = _get_conn()
    conn.execute(
        "DELETE FROM product_dimensions WHERE product_name = ?",
        (product_name,),
    )
    conn.commit()
    conn.close()


def bulk_upsert_dimensions(records: List[Dict]) -> int:
    """일괄 치수 추가/갱신"""
    sb = _get_sb()
    if sb:
        try:
            rows = []
            for r in records:
                h = float(r["height"])
                d = float(r["depth"]) if r.get("depth") else None
                rows.append({
                    'product_name': r["product_name"],
                    'width': float(r["width"]) if r.get("width") else None,
                    'height': h, 'depth': d,
                    'size_class': _classify_size(h),
                    'dual_row': 1 if (d is not None and d <= 14.0) else 0,
                })
            sb.table('product_dimensions').upsert(rows, on_conflict='product_name').execute()
            return len(rows)
        except Exception:
            pass
    conn = _get_conn()
    cur = conn.cursor()
    count = 0
    for r in records:
        h = float(r["height"])
        d = float(r["depth"]) if r.get("depth") else None
        size_class = _classify_size(h)
        dual_row = 1 if (d is not None and d <= 14.0) else 0
        cur.execute(
            """INSERT INTO product_dimensions
               (product_name, width, height, depth, size_class, dual_row, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(product_name) DO UPDATE SET
                 width=excluded.width, height=excluded.height,
                 depth=excluded.depth, size_class=excluded.size_class,
                 dual_row=excluded.dual_row, updated_at=CURRENT_TIMESTAMP""",
            (r["product_name"],
             float(r["width"]) if r.get("width") else None,
             h, d, size_class, dual_row),
        )
        count += 1
    conn.commit()
    conn.close()
    return count


# ──────────────────────────────────────
# 배치 추천
# ──────────────────────────────────────

def recommend_locations(product_name: str, top_n: int = 5) -> List[Dict]:
    """
    상품 치수 기반으로 적합한 빈 선반 위치 추천
    - 높이 여유 (tier_height - product_height)가 적절한 곳 우선
    - 너무 큰 선반은 낭비, 너무 작은 선반은 불가
    """
    dim = get_dimension(product_name)
    if not dim or not dim.get("height"):
        return []

    prod_h = dim["height"]
    margin = 2  # 최소 여유 공간(cm)

    conn = _get_conn()
    # 빈 선반만 대상
    rows = conn.execute("""
        SELECT l.*
        FROM shelf_locations l
        WHERE l.id NOT IN (
            SELECT DISTINCT shelf_location_id
            FROM shelf_placements WHERE end_date IS NULL
        )
        AND l.tier_height >= ?
        ORDER BY l.tier_height ASC, l.shelf_type, l.fixture_no, l.tier
    """, (prod_h + margin,)).fetchall()
    conn.close()

    results = []
    for r in rows:
        r = dict(r)
        waste = r["tier_height"] - prod_h
        r["height_waste"] = round(waste, 1)
        r["fit_score"] = round(max(0, 100 - waste * 5), 1)  # 낭비 적을수록 높은 점수
        results.append(r)

    # 점수 높은 순 정렬
    results.sort(key=lambda x: -x["fit_score"])
    return results[:top_n]


# ──────────────────────────────────────
# 선반 수요 예측
# ──────────────────────────────────────

def predict_shelf_demand(extra_products: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
    """
    현재 치수 데이터 기반으로 필요한 선반 수 예측

    Args:
        extra_products: 추가할 상품 DataFrame (columns: product_name, height 필수)

    Returns:
        size_class별 상품수, 필요 선반 수, 현재 보유 선반 대비 과부족
    """
    dims = get_all_dimensions()
    if dims.empty:
        return {"error": "치수 데이터가 없습니다"}

    if extra_products is not None and not extra_products.empty:
        extra = extra_products.copy()
        extra["size_class"] = extra["height"].apply(_classify_size)
        all_products = pd.concat([dims, extra], ignore_index=True)
    else:
        all_products = dims

    # 사이즈별 상품 수
    size_counts = all_products["size_class"].value_counts().to_dict()
    tall_count = size_counts.get("tall", 0)
    medium_count = size_counts.get("medium", 0)
    short_count = size_counts.get("short", 0)
    total_products = len(all_products)

    # 2열 가능 비율
    dual_row_rate = all_products["dual_row"].mean() if "dual_row" in all_products.columns else 0.97

    # 선반당 수용량 (평균 가로 기준, 선반 가로 90cm)
    avg_width = all_products["width"].dropna().mean() if all_products["width"].notna().any() else 8.0
    per_shelf_single = max(1, int(90 / (avg_width + 0.3)))
    per_shelf_avg = per_shelf_single * (1 + dual_row_rate)  # 2열 가능 비율 반영

    # 사이즈별 필요 선반 수
    # tall(>23cm) → A타입 5단(높이 제한 없음) 전용
    # medium/short → 모든 일반 선반(25cm) OK
    shelves_for_tall = max(0, -(-tall_count // int(per_shelf_avg)))
    normal_count = medium_count + short_count
    shelves_for_normal = max(0, -(-normal_count // int(per_shelf_avg)))
    total_shelves_needed = shelves_for_tall + shelves_for_normal

    # 현재 보유 선반 수 (단면)
    current_shelves = {}
    total_current = 0
    for stype, cfg in SHELF_CONFIGS.items():
        n = cfg["count"] * len(cfg["tiers"])  # 단면
        current_shelves[stype] = n
        total_current += n

    # A타입 5단(높이 무제한) 선반 수: A 매대 수 × 1
    a_unlimited_shelves = SHELF_CONFIGS["A"]["count"]

    # 타입별 필요 매대 수 추정
    # A타입: tall 상품용 (5단 높이 무제한, 매대당 1선반)
    a_needed = max(0, -(-shelves_for_tall // 1))
    # B타입 + C타입: normal 상품용 — 일반 25cm 선반
    # B: 5단 = 매대당 5선반
    # C: 5단 = 매대당 5선반 (가로가 좁으므로 수용량 적음)
    # A의 1~4단도 일반 선반으로 활용 가능
    a_normal_shelves = max(0, a_needed) * 4  # A 매대의 1~4단
    remaining_normal = max(0, shelves_for_normal - a_normal_shelves)
    # B와 C의 비율을 현재 비율로 유지
    b_ratio = SHELF_CONFIGS["B"]["count"] / (SHELF_CONFIGS["B"]["count"] + SHELF_CONFIGS["C"]["count"])
    b_needed = max(0, -(-int(remaining_normal * b_ratio) // 5))
    c_needed = max(0, -(-int(remaining_normal * (1 - b_ratio)) // 5))

    return {
        "total_products": total_products,
        "size_counts": {
            "tall": tall_count,
            "medium": medium_count,
            "short": short_count,
        },
        "dual_row_rate": round(dual_row_rate * 100, 1),
        "avg_width": round(avg_width, 1),
        "per_shelf_avg": round(per_shelf_avg, 1),
        "shelves_needed": {
            "tall_unlimited": shelves_for_tall,
            "normal_25cm": shelves_for_normal,
            "total": total_shelves_needed,
        },
        "fixtures_needed": {
            "A": max(a_needed, SHELF_CONFIGS["A"]["count"]),  # 최소 현재 보유
            "B": b_needed,
            "C": c_needed,
            "total": max(a_needed, SHELF_CONFIGS["A"]["count"]) + b_needed + c_needed,
        },
        "current_shelves": current_shelves,
        "current_total": total_current,
        "surplus": total_current - total_shelves_needed,
        "tall_note": f"키 큰 상품({tall_count}개)은 A타입 5단(높이 무제한)에만 배치 가능. "
                     f"A타입 5단 {a_unlimited_shelves}선반 보유.",
    }


# ──────────────────────────────────────
# 조회
# ──────────────────────────────────────

def get_all_locations(include_disabled: bool = False) -> pd.DataFrame:
    sb = _get_sb()
    if sb:
        try:
            q = sb.table('shelf_locations').select('*').order('shelf_type').order('fixture_no').order('tier')
            if not include_disabled:
                q = q.eq('enabled', 1)
            r = q.execute()
            return pd.DataFrame(r.data) if r.data else pd.DataFrame()
        except Exception:
            pass
    conn = _get_conn()
    where = "" if include_disabled else "WHERE enabled = 1"
    df = pd.read_sql_query(f"SELECT * FROM shelf_locations {where} ORDER BY shelf_type, fixture_no, tier", conn)
    conn.close()
    return df


def get_current_placements() -> pd.DataFrame:
    """현재 배치 중인(end_date IS NULL) 배치 목록"""
    sb = _get_sb()
    if sb:
        try:
            r = sb.table('shelf_placements').select(
                '*, shelf_locations!inner(shelf_type, fixture_no, tier, tier_height, display_label)'
            ).is_('end_date', 'null').execute()
            if not r.data:
                return pd.DataFrame()
            rows = []
            for p in r.data:
                loc = p.pop('shelf_locations', {})
                p.update(loc)
                rows.append(p)
            df = pd.DataFrame(rows)
            sort_cols = [c for c in ['shelf_type', 'fixture_no', 'tier', 'position_start'] if c in df.columns]
            if sort_cols:
                df = df.sort_values(sort_cols).reset_index(drop=True)
            return df
        except Exception:
            pass
    conn = _get_conn()
    df = pd.read_sql_query("""
        SELECT p.id, p.shelf_location_id, p.product_name, p.product_id,
               p.erp_category, p.start_date, p.end_date, p.notes, p.created_at,
               p.position_start, p.position_end,
               l.shelf_type, l.fixture_no, l.tier, l.tier_height, l.display_label
        FROM shelf_placements p
        JOIN shelf_locations l ON p.shelf_location_id = l.id
        WHERE p.end_date IS NULL
        ORDER BY l.shelf_type, l.fixture_no, l.tier, p.position_start
    """, conn)
    conn.close()
    return df


def get_vacant_locations() -> pd.DataFrame:
    """현재 비어있는(배치 없는) 선반 위치 (enabled만)"""
    sb = _get_sb()
    if sb:
        try:
            # 현재 배치된 location id 목록
            plc_r = sb.table('shelf_placements').select('shelf_location_id').is_('end_date', 'null').execute()
            used_ids = set(p['shelf_location_id'] for p in (plc_r.data or []))
            # 전체 활성 위치
            loc_r = sb.table('shelf_locations').select('*').eq('enabled', 1).order('shelf_type').order('fixture_no').order('tier').execute()
            if not loc_r.data:
                return pd.DataFrame()
            vacant = [l for l in loc_r.data if l['id'] not in used_ids]
            return pd.DataFrame(vacant) if vacant else pd.DataFrame()
        except Exception:
            pass
    conn = _get_conn()
    df = pd.read_sql_query("""
        SELECT l.*
        FROM shelf_locations l
        WHERE l.enabled = 1
          AND l.id NOT IN (
            SELECT DISTINCT shelf_location_id
            FROM shelf_placements
            WHERE end_date IS NULL
        )
        ORDER BY l.shelf_type, l.fixture_no, l.tier
    """, conn)
    conn.close()
    return df


def set_location_enabled(location_id: int, enabled: bool):
    """개별 위치 활성/비활성 설정"""
    sb = _get_sb()
    if sb:
        try:
            sb.table('shelf_locations').update({'enabled': 1 if enabled else 0}).eq('id', location_id).execute()
            return
        except Exception:
            pass
    conn = _get_conn()
    conn.execute("UPDATE shelf_locations SET enabled = ? WHERE id = ?", (1 if enabled else 0, location_id))
    conn.commit()
    conn.close()


def set_fixture_tiers_enabled(shelf_type: str, fixture_no: int, enabled_tiers: List[int]):
    """특정 매대의 사용 단 설정. enabled_tiers에 포함된 단만 활성화, 나머지 비활성화."""
    sb = _get_sb()
    if sb:
        try:
            # 해당 매대의 모든 단 비활성화
            sb.table('shelf_locations').update({'enabled': 0}).eq('shelf_type', shelf_type).eq('fixture_no', fixture_no).execute()
            # 지정된 단만 활성화
            for t in enabled_tiers:
                sb.table('shelf_locations').update({'enabled': 1}).eq('shelf_type', shelf_type).eq('fixture_no', fixture_no).eq('tier', t).execute()
            return
        except Exception:
            pass
    conn = _get_conn()
    conn.execute(
        "UPDATE shelf_locations SET enabled = 0 WHERE shelf_type = ? AND fixture_no = ?",
        (shelf_type, fixture_no),
    )
    if enabled_tiers:
        placeholders = ",".join("?" * len(enabled_tiers))
        conn.execute(
            f"UPDATE shelf_locations SET enabled = 1 WHERE shelf_type = ? AND fixture_no = ? AND tier IN ({placeholders})",
            (shelf_type, fixture_no, *enabled_tiers),
        )
    conn.commit()
    conn.close()


def get_fixture_tier_status(shelf_type: str, fixture_no: int) -> pd.DataFrame:
    """특정 매대의 단별 활성/비활성 상태 조회"""
    sb = _get_sb()
    if sb:
        try:
            r = sb.table('shelf_locations').select('*').eq('shelf_type', shelf_type).eq('fixture_no', fixture_no).order('tier').execute()
            return pd.DataFrame(r.data) if r.data else pd.DataFrame()
        except Exception:
            pass
    conn = _get_conn()
    df = pd.read_sql_query(
        "SELECT * FROM shelf_locations WHERE shelf_type = ? AND fixture_no = ? ORDER BY tier",
        conn, params=(shelf_type, fixture_no),
    )
    conn.close()
    return df


def _sb_placements_join(sb, filters=None) -> pd.DataFrame:
    """Supabase에서 placements + locations 조인 조회 헬퍼"""
    q = sb.table('shelf_placements').select(
        '*, shelf_locations!inner(shelf_type, fixture_no, tier, tier_height, display_label)'
    )
    if filters:
        for method, args in filters:
            q = getattr(q, method)(*args)
    q = q.order('start_date', desc=True)
    r = q.execute()
    if not r.data:
        return pd.DataFrame()
    rows = []
    for p in r.data:
        loc = p.pop('shelf_locations', {})
        p.update(loc)
        rows.append(p)
    return pd.DataFrame(rows)


def get_placement_history(shelf_location_id: int) -> pd.DataFrame:
    """특정 선반 위치의 배치 이력"""
    sb = _get_sb()
    if sb:
        try:
            return _sb_placements_join(sb, [('eq', ('shelf_location_id', shelf_location_id))])
        except Exception:
            pass
    conn = _get_conn()
    df = pd.read_sql_query("""
        SELECT p.id, p.shelf_location_id, p.product_name, p.product_id,
               p.erp_category, p.start_date, p.end_date, p.notes, p.created_at,
               p.position_start, p.position_end, l.display_label
        FROM shelf_placements p
        JOIN shelf_locations l ON p.shelf_location_id = l.id
        WHERE p.shelf_location_id = ?
        ORDER BY p.start_date DESC
    """, conn, params=(shelf_location_id,))
    conn.close()
    return df


def get_product_placement_history(product_name: str) -> pd.DataFrame:
    """특정 상품의 위치 이력"""
    sb = _get_sb()
    if sb:
        try:
            return _sb_placements_join(sb, [('eq', ('product_name', product_name))])
        except Exception:
            pass
    conn = _get_conn()
    df = pd.read_sql_query("""
        SELECT p.id, p.shelf_location_id, p.product_name, p.product_id,
               p.erp_category, p.start_date, p.end_date, p.notes, p.created_at,
               p.position_start, p.position_end,
               l.shelf_type, l.fixture_no, l.tier, l.tier_height, l.display_label
        FROM shelf_placements p
        JOIN shelf_locations l ON p.shelf_location_id = l.id
        WHERE p.product_name = ?
        ORDER BY p.start_date DESC
    """, conn, params=(product_name,))
    conn.close()
    return df


def get_all_placements() -> pd.DataFrame:
    """모든 배치 이력 (현재 + 과거)"""
    sb = _get_sb()
    if sb:
        try:
            return _sb_placements_join(sb)
        except Exception:
            pass
    conn = _get_conn()
    df = pd.read_sql_query("""
        SELECT p.id, p.shelf_location_id, p.product_name, p.product_id,
               p.erp_category, p.start_date, p.end_date, p.notes, p.created_at,
               p.position_start, p.position_end,
               l.shelf_type, l.fixture_no, l.tier, l.tier_height, l.display_label
        FROM shelf_placements p
        JOIN shelf_locations l ON p.shelf_location_id = l.id
        ORDER BY p.start_date DESC
    """, conn)
    conn.close()
    return df


# ──────────────────────────────────────
# 배치 관리
# ──────────────────────────────────────

def add_placement(
    shelf_location_id: int,
    product_name: str,
    start_date: date,
    product_id: Optional[str] = None,
    erp_category: Optional[str] = None,
    notes: Optional[str] = None,
    position_start: int = 1,
    position_end: int = 1,
) -> int:
    """새 배치 추가. 반환: placement_id"""
    sb = _get_sb()
    if sb:
        try:
            r = sb.table('shelf_placements').insert({
                'shelf_location_id': shelf_location_id,
                'product_name': product_name,
                'product_id': product_id,
                'erp_category': erp_category,
                'start_date': start_date.isoformat(),
                'notes': notes,
                'position_start': position_start,
                'position_end': position_end,
            }).execute()
            return r.data[0]['id']
        except Exception:
            pass
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO shelf_placements
           (shelf_location_id, product_name, product_id, erp_category, start_date, notes, position_start, position_end)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (shelf_location_id, product_name, product_id, erp_category,
         start_date.isoformat(), notes, position_start, position_end),
    )
    pid = cur.lastrowid
    conn.commit()
    conn.close()
    return pid


def end_placement(placement_id: int, end_date: date):
    """배치 종료 (end_date 설정)"""
    sb = _get_sb()
    if sb:
        try:
            sb.table('shelf_placements').update({
                'end_date': end_date.isoformat()
            }).eq('id', placement_id).execute()
            return
        except Exception:
            pass
    conn = _get_conn()
    conn.execute(
        "UPDATE shelf_placements SET end_date = ? WHERE id = ?",
        (end_date.isoformat(), placement_id),
    )
    conn.commit()
    conn.close()


def swap_placement(
    placement_id: int,
    end_date: date,
    new_shelf_location_id: int,
    new_product_name: str,
    new_start_date: date,
    new_product_id: Optional[str] = None,
    new_erp_category: Optional[str] = None,
    new_notes: Optional[str] = None,
    new_position_start: int = 1,
    new_position_end: int = 1,
) -> int:
    """기존 배치 종료 + 새 배치 원자적 처리"""
    sb = _get_sb()
    if sb:
        try:
            sb.table('shelf_placements').update({
                'end_date': end_date.isoformat()
            }).eq('id', placement_id).execute()
            r = sb.table('shelf_placements').insert({
                'shelf_location_id': new_shelf_location_id,
                'product_name': new_product_name,
                'product_id': new_product_id,
                'erp_category': new_erp_category,
                'start_date': new_start_date.isoformat(),
                'notes': new_notes,
                'position_start': new_position_start,
                'position_end': new_position_end,
            }).execute()
            return r.data[0]['id']
        except Exception:
            pass
    conn = _get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE shelf_placements SET end_date = ? WHERE id = ?",
            (end_date.isoformat(), placement_id),
        )
        cur.execute(
            """INSERT INTO shelf_placements
               (shelf_location_id, product_name, product_id, erp_category, start_date, notes, position_start, position_end)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (new_shelf_location_id, new_product_name, new_product_id,
             new_erp_category, new_start_date.isoformat(), new_notes,
             new_position_start, new_position_end),
        )
        new_id = cur.lastrowid
        conn.commit()
        return new_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def bulk_add_placements(records: List[Dict[str, Any]]) -> int:
    """일괄 배치 추가. records: [{shelf_location_id, product_name, start_date, ...}]"""
    sb = _get_sb()
    if sb:
        try:
            rows = [{
                'shelf_location_id': r["shelf_location_id"],
                'product_name': r["product_name"],
                'product_id': r.get("product_id"),
                'erp_category': r.get("erp_category"),
                'start_date': r["start_date"],
                'notes': r.get("notes"),
                'position_start': r.get("position_start", 1),
                'position_end': r.get("position_end", 1),
            } for r in records]
            sb.table('shelf_placements').insert(rows).execute()
            return len(rows)
        except Exception:
            pass
    conn = _get_conn()
    cur = conn.cursor()
    count = 0
    for r in records:
        cur.execute(
            """INSERT INTO shelf_placements
               (shelf_location_id, product_name, product_id, erp_category, start_date, notes, position_start, position_end)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (r["shelf_location_id"], r["product_name"],
             r.get("product_id"), r.get("erp_category"),
             r["start_date"], r.get("notes"),
             r.get("position_start", 1), r.get("position_end", 1)),
        )
        count += 1
    conn.commit()
    conn.close()
    return count


def delete_placement(placement_id: int):
    """배치 레코드 삭제 (잘못 입력 시)"""
    sb = _get_sb()
    if sb:
        try:
            sb.table('shelf_placements').delete().eq('id', placement_id).execute()
            return
        except Exception:
            pass
    conn = _get_conn()
    conn.execute("DELETE FROM shelf_placements WHERE id = ?", (placement_id,))
    conn.commit()
    conn.close()


# ──────────────────────────────────────
# 매출 연동 (Supabase)
# ──────────────────────────────────────

def fetch_sales_for_placements(
    date_from: str,
    date_to: str,
) -> pd.DataFrame:
    """
    배치된 상품들의 매출 데이터를 Supabase에서 가져와서
    placement 정보와 결합하여 반환
    """
    from supabase_client import (
        is_supabase_configured,
        fetch_sale_cost_records,
        fetch_products,
    )

    if not is_supabase_configured():
        return pd.DataFrame()

    # 매출 데이터 조회
    sales_df = fetch_sale_cost_records(date_from=date_from, date_to=date_to)
    if sales_df.empty:
        return pd.DataFrame()

    # 상품 마스터 (product_id → product_name 매핑)
    products_df = fetch_products()
    if products_df.empty:
        return pd.DataFrame()

    # product_id → product_name 매핑
    prod_map = dict(zip(products_df["id"], products_df["name"]))
    sales_df["product_name"] = sales_df["product_id"].map(prod_map)

    # 현재 배치 데이터
    placements = get_current_placements()
    if placements.empty:
        return pd.DataFrame()

    # product_name 기준으로 매출 집계
    sales_agg = sales_df.groupby("product_name").agg(
        total_revenue=("selling_price_total", "sum"),
        total_cost=("fifo_cost_total", "sum"),
        total_profit=("gross_profit", "sum"),
        sale_count=("id", "count"),
    ).reset_index()

    # 배치 데이터와 조인
    merged = placements.merge(sales_agg, on="product_name", how="left")
    merged["total_revenue"] = merged["total_revenue"].fillna(0)
    merged["total_cost"] = merged["total_cost"].fillna(0)
    merged["total_profit"] = merged["total_profit"].fillna(0)
    merged["sale_count"] = merged["sale_count"].fillna(0).astype(int)

    return merged


def fetch_sales_for_placement_history(
    product_name: str,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """특정 상품의 특정 기간 매출 요약"""
    from supabase_client import (
        is_supabase_configured,
        fetch_sale_cost_records,
        fetch_products,
    )

    if not is_supabase_configured():
        return {"total_revenue": 0, "total_profit": 0, "sale_count": 0, "days": 0}

    products_df = fetch_products()
    if products_df.empty:
        return {"total_revenue": 0, "total_profit": 0, "sale_count": 0, "days": 0}

    # product_name → product_id 찾기
    match = products_df[products_df["name"] == product_name]
    if match.empty:
        return {"total_revenue": 0, "total_profit": 0, "sale_count": 0, "days": 0}

    product_id = match.iloc[0]["id"]

    sales_df = fetch_sale_cost_records(date_from=start_date, date_to=end_date)
    if sales_df.empty:
        return {"total_revenue": 0, "total_profit": 0, "sale_count": 0, "days": 0}

    product_sales = sales_df[sales_df["product_id"] == product_id]

    d_from = pd.to_datetime(start_date)
    d_to = pd.to_datetime(end_date)
    days = max(1, (d_to - d_from).days + 1)

    return {
        "total_revenue": product_sales["selling_price_total"].sum() if not product_sales.empty else 0,
        "total_profit": product_sales["gross_profit"].sum() if not product_sales.empty else 0,
        "sale_count": len(product_sales),
        "days": days,
    }
