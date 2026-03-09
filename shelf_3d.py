"""
양면 매대 진열 3D 시각화 — 타입A/B/C 비교
앞뒤 양면에 선반이 있는 실제 매대 구조
마우스로 회전/확대 가능한 인터랙티브 3D HTML
"""
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import random

random.seed(42)

df = pd.read_csv('/tmp/product_dims.csv')
df['2열가능'] = df['폭'] <= 14.0

# 높이별 분류
tall = df[df['세로'] > 23].copy()
medium = df[(df['세로'] > 15) & (df['세로'] <= 23)].copy()
short = df[df['세로'] <= 15].copy()

print(f"총 제품: {len(df)} (키큰:{len(tall)}, 중간:{len(medium)}, 작은:{len(short)})")
print(f"2열 가능: {(df['2열가능'].mean()*100):.1f}%")


# ─── 선반 필요수 계산 ───
def calc_shelves_needed(grp, per_shelf):
    return max(1, -(-len(grp) // per_shelf))  # ceil division

tall_shelves = calc_shelves_needed(tall, 10)
medium_shelves = calc_shelves_needed(medium, 12)
short_shelves = calc_shelves_needed(short, 16)
total_shelves = tall_shelves + medium_shelves + short_shelves

print(f"\n── 선반 필요수 ──")
print(f"  30cm 선반 (키큰): {tall_shelves}개")
print(f"  26cm 선반 (중간): {medium_shelves}개")
print(f"  21cm 선반 (작은): {short_shelves}개")
print(f"  총 필요: {total_shelves}개")

# ─── 양면 매대 최적 구성 ───
# 양면이므로 매대 1대 = 전면 + 후면, 선반 수 2배
# Type A (4단 양면): 전/후 각 4선반 = 8선반, 높이: 30+26+26+21 = 103cm
# Type B (4단 양면): 전/후 각 4선반 = 8선반, 높이: 26×4 = 104cm
# Type C (5단 양면): 전/후 각 5선반 = 10선반, 높이: 21×5 = 105cm

# 배치 로직: 키큰→A, 중간→A+B, 작은→A+C
type_a = 2   # 각각 30cm×2 + 26cm×4 + 21cm×2 = 8선반/대
type_b = 4   # 각각 26cm×8 = 8선반/대
type_c = 7   # 각각 21cm×10 = 10선반/대
total_fixtures = type_a + type_b + type_c

# 선반 수 합산
shelves_30 = type_a * 2 * 1  # A타입 양면, 최하단(30cm) 1개씩
shelves_26 = type_a * 2 * 2 + type_b * 2 * 4  # A타입 중간 2개 + B타입 전부
shelves_21 = type_a * 2 * 1 + type_c * 2 * 5  # A타입 최상단 + C타입 전부
total_shelves_avail = shelves_30 + shelves_26 + shelves_21

print(f"\n── 양면 매대 최적 구성 ──")
print(f"  타입A (4단 양면): {type_a}대 × 8선반 = {type_a*8}선반")
print(f"  타입B (4단 양면): {type_b}대 × 8선반 = {type_b*8}선반")
print(f"  타입C (5단 양면): {type_c}대 × 10선반 = {type_c*10}선반")
print(f"  총 매대: {total_fixtures}대")
print(f"  총 선반: {total_shelves_avail}개 (필요: {total_shelves})")
print(f"  활용률: {total_shelves/total_shelves_avail*100:.1f}%")

# ─── 색상 ───
PRODUCT_COLORS = [
    'rgb(255,182,193)', 'rgb(173,216,230)', 'rgb(144,238,144)', 'rgb(255,218,185)',
    'rgb(221,160,221)', 'rgb(255,255,224)', 'rgb(176,224,230)', 'rgb(255,228,196)',
    'rgb(240,230,140)', 'rgb(230,230,250)', 'rgb(255,192,203)', 'rgb(175,238,238)',
    'rgb(152,251,152)', 'rgb(255,228,225)', 'rgb(216,191,216)', 'rgb(245,222,179)',
    'rgb(188,238,104)', 'rgb(135,206,235)', 'rgb(244,164,96)', 'rgb(255,160,122)',
]

def product_color():
    return random.choice(PRODUCT_COLORS)


# ─── 3D 박스 ───
def box(fig, x, y, z, dx, dy, dz, color, name="", op=0.85):
    v = np.array([
        [x,y,z],[x+dx,y,z],[x+dx,y+dy,z],[x,y+dy,z],
        [x,y,z+dz],[x+dx,y,z+dz],[x+dx,y+dy,z+dz],[x,y+dy,z+dz],
    ])
    faces = [[0,1,5],[0,5,4],[1,2,6],[1,6,5],[2,3,7],[2,7,6],[3,0,4],[3,4,7],[0,1,2],[0,2,3],[4,5,6],[4,6,7]]
    fig.add_trace(go.Mesh3d(
        x=v[:,0],y=v[:,1],z=v[:,2],
        i=[f[0] for f in faces],j=[f[1] for f in faces],k=[f[2] for f in faces],
        color=color,opacity=op,name=name,showlegend=False,
        hoverinfo='text',hovertext=name,flatshading=True,
    ))


# ─── 선반에 제품 배치 ───
def place_products_on_shelf(fig, products, shelf_x, shelf_y_start, shelf_z, max_h,
                            shelf_w=90, shelf_d=28, face='front'):
    """선반 하나에 제품 배치. 배치된 수 반환"""
    placed = 0
    x1 = shelf_x + 0.5
    x2 = shelf_x + 0.5

    if face == 'front':
        row1_y = shelf_y_start + 0.5
        row2_y = shelf_y_start + 14
    else:  # back — 뒤쪽에서 앞으로 진열 (고객 시선 방향)
        row1_y = shelf_y_start + shelf_d - 0.5
        row2_y = shelf_y_start + shelf_d - 14

    for _, p in products.iterrows():
        w, h, d = p['가로'], p['세로'], min(p['폭'], 13)
        if h > max_h:
            continue

        if face == 'back':
            # 뒤쪽 진열은 y를 반대로
            y1 = row1_y - d
            y2 = row2_y - min(d, 12)
        else:
            y1 = row1_y
            y2 = row2_y

        if x1 + w <= shelf_x + shelf_w - 0.5:
            box(fig, x1, y1, shelf_z, w, d if face=='front' else d, h, product_color(), p['상품명'][:15])
            x1 += w + 0.3
            placed += 1
        elif p['2열가능'] and x2 + w <= shelf_x + shelf_w - 0.5:
            d2 = min(d, 12)
            box(fig, x2, y2, shelf_z, w, d2, h, product_color(), p['상품명'][:15])
            x2 += w + 0.3
            placed += 1

    return placed


# ─── 종합 뷰: 3개 양면 매대 타입 나란히 ───
fig = go.Figure()

SW = 90          # 선반 가로
SD = 28          # 선반 깊이 (한쪽)
CENTER = 2       # 중앙 칸막이 두께
TOTAL_D = SD * 2 + CENTER  # 양면 총 깊이: 58cm
FH = 105         # 매대 총 높이
GAP = 25         # 매대 간 간격

configs = [
    {
        'type': 'A', 'heights': [30, 26, 26, 21], 'x': 0,
        'product_groups': [tall, medium, medium, short],
        'samples_front': [3, 8, 8, 6],
        'samples_back': [3, 8, 8, 6],
    },
    {
        'type': 'B', 'heights': [26, 26, 26, 26], 'x': SW + GAP,
        'product_groups': [medium, medium, medium, medium],
        'samples_front': [7, 7, 7, 7],
        'samples_back': [7, 7, 7, 7],
    },
    {
        'type': 'C', 'heights': [21, 21, 21, 21, 21], 'x': 2 * (SW + GAP),
        'product_groups': [short, short, short, short, short],
        'samples_front': [7, 7, 7, 7, 7],
        'samples_back': [7, 7, 7, 7, 7],
    },
]

for cfg in configs:
    x_off = cfg['x']
    ftype = cfg['type']

    # ── 매대 프레임 ──
    pillar_color = 'rgb(140,120,100)'
    center_color = 'rgb(180,165,150)'

    # 왼쪽 기둥
    box(fig, x_off - 2, 0, 0, 2, TOTAL_D, FH, pillar_color, f"타입{ftype} 기둥", 0.6)
    # 오른쪽 기둥
    box(fig, x_off + SW, 0, 0, 2, TOTAL_D, FH, pillar_color, f"타입{ftype} 기둥", 0.6)
    # 중앙 칸막이 (양면의 등판)
    box(fig, x_off, SD, 0, SW, CENTER, FH, center_color, f"타입{ftype} 중앙칸막이", 0.4)

    z = 0
    total_placed_front = 0
    total_placed_back = 0

    for si, sh in enumerate(cfg['heights']):
        # ── 전면 선반판 ──
        shelf_color = 'rgb(200,185,170)'
        box(fig, x_off, 0, z, SW, SD, 1.5, shelf_color, f"타입{ftype} 전면선반{si+1}", 0.7)
        # ── 후면 선반판 ──
        box(fig, x_off, SD + CENTER, z, SW, SD, 1.5, shelf_color, f"타입{ftype} 후면선반{si+1}", 0.7)

        # 제품 배치 - 전면
        grp = cfg['product_groups'][si]
        n_front = cfg['samples_front'][si]
        if len(grp) > 0:
            sample_f = grp.sample(min(n_front, len(grp)))
            max_h = sh - 2
            placed = place_products_on_shelf(fig, sample_f, x_off, 0, z + 1.5, max_h,
                                            face='front')
            total_placed_front += placed

        # 제품 배치 - 후면
        n_back = cfg['samples_back'][si]
        if len(grp) > 0:
            sample_b = grp.sample(min(n_back, len(grp)))
            max_h = sh - 2
            placed = place_products_on_shelf(fig, sample_b, x_off, SD + CENTER, z + 1.5, max_h,
                                            face='back')
            total_placed_back += placed

        z += sh

    total_placed = total_placed_front + total_placed_back

    # 상단 라벨
    n_shelves = len(cfg['heights'])
    heights_str = "+".join([str(h) for h in cfg['heights']])
    fig.add_trace(go.Scatter3d(
        x=[x_off + SW/2], y=[TOTAL_D/2], z=[FH + 10],
        mode='text',
        text=[f"<b>타입{ftype} (양면 {n_shelves}단)</b><br>{heights_str}cm<br>선반 {n_shelves*2}개 | 진열 {total_placed}개"],
        textfont=dict(size=12, color='black'),
        showlegend=False,
    ))

    # 전면/후면 표시
    fig.add_trace(go.Scatter3d(
        x=[x_off + SW/2], y=[-5], z=[FH/2],
        mode='text', text=["◀ 전면"],
        textfont=dict(size=10, color='rgb(50,50,200)'),
        showlegend=False,
    ))
    fig.add_trace(go.Scatter3d(
        x=[x_off + SW/2], y=[TOTAL_D + 5], z=[FH/2],
        mode='text', text=["후면 ▶"],
        textfont=dict(size=10, color='rgb(200,50,50)'),
        showlegend=False,
    ))

    print(f"타입{ftype} 양면 ({n_shelves}단×2): 전면 {total_placed_front} + 후면 {total_placed_back} = {total_placed}개 진열")


# ─── 레이아웃 ───
fig.update_layout(
    title=dict(
        text="📦 양면 매대 3개 타입 비교 — 3D 진열 시뮬레이션 (마우스로 회전 가능)",
        font=dict(size=16),
    ),
    scene=dict(
        xaxis=dict(title='가로 (cm)', range=[-15, 3*(SW+GAP)+10]),
        yaxis=dict(title='깊이 (cm)', range=[-15, TOTAL_D+15]),
        zaxis=dict(title='높이 (cm)', range=[-5, FH+25]),
        aspectmode='data',
        camera=dict(
            eye=dict(x=1.2, y=-1.8, z=0.7),
            up=dict(x=0, y=0, z=1),
        ),
    ),
    width=1500, height=850,
    margin=dict(l=0, r=0, t=60, b=0),
    showlegend=False,
    paper_bgcolor='rgb(248,248,248)',
    annotations=[
        dict(
            text=(
                f"<b>양면 매대 최적 구성: 총 {total_fixtures}대</b><br>"
                f"타입A {type_a}대(4단양면) + 타입B {type_b}대(4단양면) + 타입C {type_c}대(5단양면)<br>"
                f"총 선반: {total_shelves_avail}개 | 제품: {len(df)}개 | 활용률: {total_shelves/total_shelves_avail*100:.1f}%"
            ),
            showarrow=False, x=0.5, y=-0.02, xref='paper', yref='paper',
            font=dict(size=13, color='rgb(60,60,60)'),
            align='center',
        ),
    ],
)

out = '/Users/ys_hanah1/Desktop/마트약국_시스템/shelf_3d_all.html'
fig.write_html(out)
print(f"\n✅ 3D 시각화 저장: {out}")
print("브라우저에서 열면 마우스로 자유롭게 회전/확대 가능!")
