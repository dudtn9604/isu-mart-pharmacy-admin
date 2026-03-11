-- 매대 위치 마스터
CREATE TABLE IF NOT EXISTS shelf_locations (
    id SERIAL PRIMARY KEY,
    shelf_type TEXT NOT NULL,
    fixture_no INTEGER NOT NULL,
    tier INTEGER NOT NULL,
    tier_height INTEGER NOT NULL DEFAULT 25,
    display_label TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    UNIQUE(shelf_type, fixture_no, tier)
);

-- 매대 배치 이력
CREATE TABLE IF NOT EXISTS shelf_placements (
    id SERIAL PRIMARY KEY,
    shelf_location_id INTEGER NOT NULL REFERENCES shelf_locations(id),
    product_name TEXT NOT NULL,
    product_id TEXT,
    erp_category TEXT,
    start_date DATE NOT NULL DEFAULT CURRENT_DATE,
    end_date DATE,
    notes TEXT,
    position_start INTEGER NOT NULL DEFAULT 1,
    position_end INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 매대 배치도 위치
CREATE TABLE IF NOT EXISTS fixture_positions (
    id SERIAL PRIMARY KEY,
    shelf_type TEXT NOT NULL,
    fixture_no INTEGER NOT NULL,
    x_pos REAL NOT NULL DEFAULT 0,
    y_pos REAL NOT NULL DEFAULT 0,
    orientation TEXT NOT NULL DEFAULT 'V',
    zone TEXT,
    custom_label TEXT,
    UNIQUE(shelf_type, fixture_no)
);

-- RLS 정책: 누구나 읽기/쓰기 가능 (알바 접근용)
ALTER TABLE shelf_locations ENABLE ROW LEVEL SECURITY;
ALTER TABLE shelf_placements ENABLE ROW LEVEL SECURITY;
ALTER TABLE fixture_positions ENABLE ROW LEVEL SECURITY;

CREATE POLICY "shelf_locations_read" ON shelf_locations FOR SELECT USING (true);
CREATE POLICY "shelf_locations_update" ON shelf_locations FOR UPDATE USING (true);
CREATE POLICY "shelf_placements_read" ON shelf_placements FOR SELECT USING (true);
CREATE POLICY "shelf_placements_insert" ON shelf_placements FOR INSERT WITH CHECK (true);
CREATE POLICY "shelf_placements_update" ON shelf_placements FOR UPDATE USING (true);
CREATE POLICY "fixture_positions_read" ON fixture_positions FOR SELECT USING (true);
