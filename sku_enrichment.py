"""
SKU 성분 매핑 + 동의어 사전 모듈

[목적]
기존 매칭 로직은 '상품명'만 보고 키워드 매칭을 했기 때문에
상품명에 원료/성분이 드러나지 않는 제품을 놓치는 문제가 있었다.

예) "리버락 골드 플러스" → 실리마린/밀크씨슬 제품인데 상품명에 안 보임
예) "콘드로이친" vs "콘드로이틴" → 같은 성분인데 다른 단어로 취급

[구성]
1. KEYWORD_SYNONYMS: 동의어/이형 표기 사전 (canonical → [variants])
2. PRODUCT_INGREDIENTS: 상품명 → 숨겨진 성분/키워드 매핑
3. enriched_keyword_match(): 동의어 + 성분 매핑을 적용한 향상된 매칭 함수
"""

from pathlib import Path
from typing import Dict, List, Set, Tuple
import pandas as pd


# ──────────────────────────────────────
# 1. 동의어 사전 (Synonym Dictionary)
#    key = 대표 표기 (canonical)
#    value = [대표 표기 포함 모든 이형 표기]
# ──────────────────────────────────────

KEYWORD_SYNONYMS: Dict[str, List[str]] = {
    # 관절 관련
    "콘드로이틴": ["콘드로이틴", "콘드로이친", "chondroitin"],
    "글루코사민": ["글루코사민", "글루코스아민", "glucosamine"],
    "msm": ["msm", "엠에스엠", "메틸설포닐메탄"],
    "보스웰리아": ["보스웰리아", "보스웰리야", "boswellia"],

    # 간 건강
    "실리마린": ["실리마린", "밀크씨슬", "밀크시슬", "실리마린밀크씨슬", "silymarin", "milk thistle"],
    "udca": ["udca", "우르소데옥시콜산", "우르소"],

    # 뇌건강 / 혈액순환
    "징코빌로바": ["징코빌로바", "징코", "은행잎", "은행잎추출물", "기넥신", "ginkgo"],

    # 항산화 / 이너뷰티
    "글루타치온": ["글루타치온", "글루타티온", "glutathione"],
    "콜라겐": ["콜라겐", "콜라젠", "collagen"],
    "히알루론산": ["히알루론산", "히알루론", "hyaluronic"],
    "nmn": ["nmn", "니코틴아미드", "니코틴아마이드"],
    "코엔자임q10": ["코엔자임q10", "코큐텐", "coq10", "코엔자임"],
    "아스타잔틴": ["아스타잔틴", "아스타산틴", "astaxanthin"],

    # 장 건강
    "프로바이오틱스": ["프로바이오틱스", "유산균", "프로바이오틱", "probiotics", "lactobacillus"],
    "프리바이오틱스": ["프리바이오틱스", "프리바이오틱", "prebiotics"],

    # 비타민 / 미네랄
    "비타민d": ["비타민d", "비타민디", "vitamin d", "비타민d3"],
    "비타민c": ["비타민c", "비타민씨", "vitamin c", "아스코르브산"],
    "비타민b": ["비타민b", "비타민비", "vitamin b", "비타민b군"],
    "비오틴": ["비오틴", "바이오틴", "biotin", "비타민h", "비타민b7"],
    "셀레늄": ["셀레늄", "셀렌", "셀레니움", "selenium"],
    "아연": ["아연", "징크", "zinc"],
    "철분": ["철분", "헴철", "iron"],
    "마그네슘": ["마그네슘", "마그네시움", "magnesium"],
    "칼슘": ["칼슘", "calcium"],

    # 오메가3 / 지방산
    "오메가3": ["오메가3", "오메가쓰리", "omega3", "omega-3", "epa", "dha", "피쉬오일", "어유"],
    "크릴오일": ["크릴오일", "크릴", "krill"],

    # 수면
    "멜라토닌": ["멜라토닌", "melatonin"],
    "테아닌": ["테아닌", "l-테아닌", "theanine"],
    "가바": ["가바", "gaba", "감마아미노부틸산"],

    # 다이어트
    "가르시니아": ["가르시니아", "garcinia", "hca"],
    "키토산": ["키토산", "키토삼", "chitosan"],
    "cla": ["cla", "공액리놀레산", "공액리놀렌산"],

    # 혈관 / 순환
    "디오스민": ["디오스민", "diosmin"],
    "나토키나제": ["나토키나제", "나토키나아제", "nattokinase"],

    # 눈건강
    "루테인": ["루테인", "루테인지아잔틴", "lutein"],
    "지아잔틴": ["지아잔틴", "제아잔틴", "zeaxanthin"],
    "빌베리": ["빌베리", "bilberry"],

    # 여성건강
    "이소플라본": ["이소플라본", "대두이소플라본", "isoflavone"],
    "석류": ["석류", "석류추출물"],
    "달맞이꽃종자유": ["달맞이꽃종자유", "달맞이꽃", "감마리놀렌산", "gla"],
    "엽산": ["엽산", "폴산", "폴릭산", "folic acid"],
    "피크노제놀": ["피크노제놀", "pycnogenol", "프랑스해송"],

    # 남성건강
    "쏘팔메토": ["쏘팔메토", "소팔메토", "saw palmetto"],
    "아르기닌": ["아르기닌", "l-아르기닌", "arginine"],

    # 탈모
    "미녹시딜": ["미녹시딜", "minoxidil"],

    # 면역
    "베타글루칸": ["베타글루칸", "beta-glucan", "베타글루간"],
    "프로폴리스": ["프로폴리스", "propolis"],

    # 한방
    "홍삼": ["홍삼", "인삼", "정관장"],
    "녹용": ["녹용", "녹각"],

    # 피부
    "세라마이드": ["세라마이드", "세라미드", "ceramide"],

    # 혈당
    "크롬": ["크롬", "크로뮴", "chromium"],
    "바나바잎": ["바나바잎", "바나바", "코로솔산"],
}


# ──────────────────────────────────────
# 2. 상품-성분 매핑 (Product → Ingredients)
#    상품명의 일부(검색 키)와 매칭하여 숨겨진 성분 키워드를 반환
#    key는 상품명에 포함된 고유 문자열, value는 관련 성분/키워드 리스트
# ──────────────────────────────────────

PRODUCT_INGREDIENTS: Dict[str, List[str]] = {
    # ── 간건강 ──
    "리버락": ["실리마린", "밀크씨슬", "간건강"],
    "헤파토스": ["udca", "간건강"],
    "우루사": ["udca", "간건강"],
    "레버엑스": ["간건강"],
    "레드리버": ["밀크씨슬", "실리마린", "간건강"],
    "액티리버": ["밀크씨슬", "실리마린", "간건강"],
    "리바이탈": ["간건강"],

    # ── 뇌건강 / 혈액순환 ──
    "타나민": ["징코빌로바", "은행잎", "혈액순환", "뇌건강"],
    "타나솔": ["징코빌로바", "은행잎", "혈액순환", "뇌건강"],
    "기넥신": ["징코빌로바", "은행잎", "혈액순환", "뇌건강"],
    "징코써클": ["징코빌로바", "은행잎", "혈액순환"],
    "징코메디": ["징코빌로바", "은행잎", "혈액순환"],
    "써큐란": ["징코빌로바", "은행잎", "혈액순환"],

    # ── 수면 ──
    "멜라케어": ["멜라토닌", "수면"],
    "멜라메디": ["멜라토닌", "수면"],
    "멜라그린": ["멜라토닌", "수면"],
    "슬립케어": ["멜라토닌", "수면", "테아닌"],
    "나이트케어": ["수면", "테아닌"],
    "잠온다": ["수면", "테아닌"],

    # ── 관절 ──
    "콘티포르테": ["콘드로이틴", "관절"],
    "오스테민": ["글루코사민", "관절"],
    "조인트락": ["msm", "글루코사민", "관절"],
    "무브프리": ["글루코사민", "콘드로이틴", "관절"],
    "트라스트": ["콘드로이틴", "관절"],
    "관절팔팔": ["관절", "글루코사민"],
    "아킬": ["관절"],

    # ── 이너뷰티 / 항산화 ──
    "에바치온": ["글루타치온", "항산화", "이너뷰티"],
    "타치온": ["글루타치온", "항산화", "이너뷰티"],
    "글루타": ["글루타치온", "항산화"],
    "파이토s": ["nmn", "항노화"],
    "센시아": ["디오스민", "정맥순환", "여성건강"],
    "센테라": ["디오스민", "정맥순환", "여성건강"],
    "이뮨셀": ["면역", "베타글루칸"],
    "엘라스틴": ["콜라겐", "이너뷰티"],

    # ── 탈모 / 두피 ──
    "판시딜": ["비오틴", "탈모", "두피건강"],
    "케라티모": ["비오틴", "탈모", "두피건강"],
    "모발엔": ["비오틴", "탈모"],
    "폴리젠": ["탈모", "두피건강"],
    "미녹시딜": ["미녹시딜", "탈모"],
    "마이녹실": ["미녹시딜", "탈모"],

    # ── 여성건강 ──
    "프리페민": ["이소플라본", "갱년기", "여성건강"],
    "프리비": ["이소플라본", "갱년기", "여성건강"],
    "페미닌케어": ["여성건강", "갱년기"],
    "레이디스": ["여성건강"],
    "파이토에스트로": ["이소플라본", "갱년기"],
    "유한 피크노": ["피크노제놀", "갱년기", "여성건강"],
    "엘레나": ["프로바이오틱스", "유산균", "여성건강", "질건강"],
    "와이셀": ["이소플라본", "갱년기"],

    # ── 눈건강 ──
    "아이클리어": ["루테인", "눈건강"],
    "오큐바이트": ["루테인", "지아잔틴", "눈건강"],
    "아이루테인": ["루테인", "눈건강"],
    "프리저비전": ["루테인", "지아잔틴", "눈건강"],
    "눈에좋은": ["루테인", "눈건강"],

    # ── 위장건강 ──
    "훼스탈": ["소화효소", "위장건강"],
    "베아제": ["소화효소", "위장건강"],
    "가스디": ["위장건강"],
    "속앤위": ["위장건강"],
    "겔포스": ["위장건강", "제산제"],
    "알마겔": ["위장건강", "제산제"],
    "개비스콘": ["위장건강", "역류성"],
    "위청수": ["위장건강"],

    # ── 피로회복 / 종합영양 ──
    "삐콤씨": ["비타민b", "비타민c", "피로회복"],
    "아로나민": ["비타민b", "피로회복"],
    "벤포벨": ["비타민b", "피로회복"],
    "임팩타민": ["비타민b", "피로회복"],
    "센트룸": ["종합비타민", "멀티비타민"],
    "얼라이브": ["종합비타민", "멀티비타민"],
    "멀티비타": ["종합비타민", "멀티비타민"],
    "피로회복": ["비타민b", "타우린", "피로회복"],
    "박카스": ["타우린", "피로회복"],
    "비타500": ["비타민c", "피로회복"],

    # ── 면역 ──
    "에키나세아": ["에키나세아", "면역"],
    "면역보감": ["면역", "베타글루칸"],

    # ── 남성건강 ──
    "프로스타": ["쏘팔메토", "전립선", "남성건강"],
    "팔메토": ["쏘팔메토", "전립선", "남성건강"],

    # ── 혈관 / 순환 ──
    "오메가알티지": ["오메가3", "epa", "dha"],
    "알티지오메가": ["오메가3", "epa", "dha"],
    "슈퍼오메가": ["오메가3", "epa", "dha"],

    # ── 혈당 / 혈압 ──
    "글루코케어": ["혈당", "크롬"],
    "바나바": ["바나바잎", "혈당"],
    "혈당엔": ["혈당", "바나바잎"],

    # ── 한방 ──
    "정관장": ["홍삼", "인삼", "면역"],
    "천녹": ["녹용", "녹각"],

    # ── 반려동물 ──
    "뉴트리벳": ["반려동물", "영양제"],
    "펫시럽": ["반려동물", "영양제"],
}


# ──────────────────────────────────────
# 3. 역방향 인덱스 구축 (자동 생성)
#    키워드/성분명 → [해당 제품명 패턴 리스트]
# ──────────────────────────────────────

def _build_reverse_synonym_map() -> Dict[str, str]:
    """
    모든 동의어 변형 → 대표 표기(canonical)로 변환하는 역매핑 생성
    예) "밀크씨슬" → "실리마린", "콘드로이친" → "콘드로이틴"
    """
    reverse = {}
    for canonical, variants in KEYWORD_SYNONYMS.items():
        for v in variants:
            reverse[v.lower()] = canonical
    return reverse


def _build_ingredient_to_products() -> Dict[str, List[str]]:
    """
    성분 키워드 → [해당 성분을 포함하는 상품명 패턴 리스트]
    예) "실리마린" → ["리버락", "레드리버", "액티리버"]
    """
    ing_to_prods: Dict[str, List[str]] = {}
    for product_key, ingredients in PRODUCT_INGREDIENTS.items():
        for ing in ingredients:
            ing_lower = ing.lower()
            if ing_lower not in ing_to_prods:
                ing_to_prods[ing_lower] = []
            if product_key not in ing_to_prods[ing_lower]:
                ing_to_prods[ing_lower].append(product_key)
    return ing_to_prods


# 모듈 로드 시 1회 구축
REVERSE_SYNONYM_MAP = _build_reverse_synonym_map()
INGREDIENT_TO_PRODUCTS = _build_ingredient_to_products()


# ──────────────────────────────────────
# 4. 핵심 매칭 함수
# ──────────────────────────────────────


def normalize_keyword(keyword: str) -> str:
    """
    키워드를 대표 표기(canonical)로 정규화
    예) "밀크씨슬" → "실리마린", "콘드로이친" → "콘드로이틴"

    매칭 안 되면 원본 그대로 반환
    """
    kw_lower = keyword.strip().lower()
    return REVERSE_SYNONYM_MAP.get(kw_lower, keyword)


def expand_keyword_synonyms(keyword: str) -> Set[str]:
    """
    키워드의 모든 동의어/이형 표기를 반환
    예) "실리마린" → {"실리마린", "밀크씨슬", "밀크시슬", "silymarin", "milk thistle"}
    """
    kw_lower = keyword.strip().lower()

    # 1) canonical 찾기
    canonical = REVERSE_SYNONYM_MAP.get(kw_lower, kw_lower)

    # 2) canonical의 모든 변형 반환
    if canonical in KEYWORD_SYNONYMS:
        return set(v.lower() for v in KEYWORD_SYNONYMS[canonical])

    return {kw_lower}


def get_product_ingredients(product_name: str) -> List[str]:
    """
    상품명으로부터 숨겨진 성분/키워드 목록 추출
    예) "리버락 골드 플러스" → ["실리마린", "밀크씨슬", "간건강"]
    """
    name_lower = product_name.lower()
    ingredients = []

    for product_key, ing_list in PRODUCT_INGREDIENTS.items():
        if product_key.lower() in name_lower:
            ingredients.extend(ing_list)

    return list(set(ingredients))


def enriched_keyword_match(
    keyword: str,
    sku_df: pd.DataFrame,
    product_col: str = "상품명",
) -> pd.DataFrame:
    """
    동의어 + 성분 매핑을 적용한 향상된 SKU 매칭

    [매칭 로직 - 3단계]
    1. 직접 매칭: 키워드가 상품명에 직접 포함
    2. 동의어 매칭: 키워드의 동의어/이형 표기가 상품명에 포함
    3. 성분 매칭: 키워드가 특정 제품의 숨겨진 성분인 경우,
                 해당 제품명 패턴이 상품명에 포함

    Args:
        keyword: 검색 키워드 (예: "실리마린")
        sku_df: SKU 데이터프레임
        product_col: 상품명 컬럼명

    Returns:
        매칭된 SKU DataFrame (중복 제거)
    """
    if sku_df.empty or product_col not in sku_df.columns:
        return pd.DataFrame()

    matched_indices = set()

    # 1단계: 직접 매칭
    kw_lower = keyword.strip().lower()
    direct = sku_df[sku_df[product_col].str.lower().str.contains(kw_lower, na=False)]
    matched_indices.update(direct.index.tolist())

    # 2단계: 동의어 매칭
    synonyms = expand_keyword_synonyms(keyword)
    for syn in synonyms:
        if syn == kw_lower:
            continue  # 이미 1단계에서 검사
        syn_matched = sku_df[sku_df[product_col].str.lower().str.contains(syn, na=False)]
        matched_indices.update(syn_matched.index.tolist())

    # 3단계: 성분→상품 역매핑
    # "실리마린" 검색 시 → "리버락", "레드리버" 등 성분을 가진 제품도 매칭
    # canonical로 정규화한 뒤 검색
    canonical = REVERSE_SYNONYM_MAP.get(kw_lower, kw_lower)

    # canonical과 모든 동의어에 대해 ingredient_to_products 조회
    all_search_terms = synonyms | {canonical}
    product_patterns = set()
    for term in all_search_terms:
        if term in INGREDIENT_TO_PRODUCTS:
            product_patterns.update(INGREDIENT_TO_PRODUCTS[term])

    for pattern in product_patterns:
        pat_lower = pattern.lower()
        pat_matched = sku_df[sku_df[product_col].str.lower().str.contains(pat_lower, na=False)]
        matched_indices.update(pat_matched.index.tolist())

    if not matched_indices:
        return pd.DataFrame(columns=sku_df.columns)

    return sku_df.loc[sorted(matched_indices)].copy()


def enriched_keyword_check(
    keyword: str,
    sku_df: pd.DataFrame,
    product_col: str = "상품명",
) -> dict:
    """
    키워드의 SKU 보유 현황을 상세 분석

    Returns:
        {
            "keyword": 원본 키워드,
            "canonical": 대표 표기,
            "matched_count": 매칭 SKU 수,
            "matched_products": 매칭 상품 리스트 (상위 5개),
            "match_types": { "direct": n, "synonym": n, "ingredient": n },
            "has_match": True/False,
        }
    """
    kw_lower = keyword.strip().lower()
    canonical = REVERSE_SYNONYM_MAP.get(kw_lower, kw_lower)

    result = {
        "keyword": keyword,
        "canonical": canonical,
        "matched_count": 0,
        "matched_products": [],
        "match_types": {"direct": 0, "synonym": 0, "ingredient": 0},
        "has_match": False,
    }

    if sku_df.empty or product_col not in sku_df.columns:
        return result

    matched_all = set()
    match_details = {"direct": set(), "synonym": set(), "ingredient": set()}

    # 1) 직접 매칭
    direct = sku_df[sku_df[product_col].str.lower().str.contains(kw_lower, na=False)]
    match_details["direct"].update(direct.index.tolist())
    matched_all.update(direct.index.tolist())

    # 2) 동의어 매칭
    synonyms = expand_keyword_synonyms(keyword)
    for syn in synonyms:
        if syn == kw_lower:
            continue
        syn_matched = sku_df[sku_df[product_col].str.lower().str.contains(syn, na=False)]
        new_matches = set(syn_matched.index.tolist()) - matched_all
        match_details["synonym"].update(new_matches)
        matched_all.update(new_matches)

    # 3) 성분→상품 역매핑
    all_search_terms = synonyms | {canonical}
    product_patterns = set()
    for term in all_search_terms:
        if term in INGREDIENT_TO_PRODUCTS:
            product_patterns.update(INGREDIENT_TO_PRODUCTS[term])

    for pattern in product_patterns:
        pat_lower = pattern.lower()
        pat_matched = sku_df[sku_df[product_col].str.lower().str.contains(pat_lower, na=False)]
        new_matches = set(pat_matched.index.tolist()) - matched_all
        match_details["ingredient"].update(new_matches)
        matched_all.update(new_matches)

    result["matched_count"] = len(matched_all)
    result["has_match"] = len(matched_all) > 0
    result["match_types"] = {
        "direct": len(match_details["direct"]),
        "synonym": len(match_details["synonym"]),
        "ingredient": len(match_details["ingredient"]),
    }

    if matched_all:
        matched_df = sku_df.loc[sorted(matched_all)]
        result["matched_products"] = matched_df[product_col].head(5).tolist()

    return result


# ──────────────────────────────────────
# 5. SKU DataFrame 성분 보강 함수
# ──────────────────────────────────────


def enrich_sku_dataframe(sku_df: pd.DataFrame, product_col: str = "상품명") -> pd.DataFrame:
    """
    SKU DataFrame에 '성분키워드' 컬럼 추가
    각 상품의 이름에서 추출한 숨겨진 성분을 쉼표로 구분하여 표시

    예) "리버락 골드 플러스" → 성분키워드: "실리마린, 밀크씨슬, 간건강"
    """
    if sku_df.empty or product_col not in sku_df.columns:
        return sku_df

    df = sku_df.copy()

    def _extract_ingredients(name):
        ings = get_product_ingredients(str(name))
        return ", ".join(ings) if ings else ""

    df["성분키워드"] = df[product_col].apply(_extract_ingredients)
    return df


# ──────────────────────────────────────
# 테스트
# ──────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("SKU 성분 매핑 + 동의어 사전 테스트")
    print("=" * 60)

    # 동의어 테스트
    print("\n[1] 동의어 정규화 테스트")
    test_synonyms = [
        ("콘드로이친", "콘드로이틴"),
        ("밀크씨슬", "실리마린"),
        ("코큐텐", "코엔자임q10"),
        ("글루타티온", "글루타치온"),
        ("콘드로이틴", "콘드로이틴"),
        ("유산균", "프로바이오틱스"),
        ("셀렌", "셀레늄"),
        ("은행잎", "징코빌로바"),
    ]
    for original, expected in test_synonyms:
        result = normalize_keyword(original)
        status = "✅" if result == expected else "❌"
        print("  {} '{}' → '{}' (기대: '{}')".format(status, original, result, expected))

    # 동의어 확장 테스트
    print("\n[2] 동의어 확장 테스트")
    test_expand = ["실리마린", "콘드로이틴", "오메가3"]
    for kw in test_expand:
        expanded = expand_keyword_synonyms(kw)
        print("  '{}' → {}".format(kw, expanded))

    # 성분 추출 테스트
    print("\n[3] 상품명→성분 추출 테스트")
    test_products = [
        "리버락 골드 플러스",
        "타나민 80mg",
        "콘티포르테 정",
        "에바치온 250mg",
        "판시딜 캡슐",
        "프리페민 캡슐",
        "대웅 우루사 100mg",
        "멜라케어 수면",
    ]
    for prod in test_products:
        ings = get_product_ingredients(prod)
        print("  '{}' → {}".format(prod, ings))

    # 실제 SKU 매칭 테스트
    print("\n[4] 실제 SKU 매칭 테스트")
    try:
        sku_path = Path(__file__).parent / "신규_sku분류_정제.xlsx"
        if sku_path.exists():
            sku_df = pd.read_excel(sku_path)
            print("  SKU 로드 완료: {}개".format(len(sku_df)))

            test_keywords = [
                "실리마린",     # 리버락 등 매칭되어야 함
                "밀크씨슬",     # 실리마린 동의어 → 같은 결과
                "콘드로이친",    # 콘드로이틴 동의어
                "징코빌로바",    # 타나민, 기넥신 등 매칭되어야 함
                "글루타치온",    # 에바치온 등 매칭
                "비오틴",       # 판시딜, 케라티모 등 매칭
                "멜라토닌",     # 멜라케어, 멜라메디 등 매칭
                "udca",        # 우루사, 헤파토스 등 매칭
            ]

            for kw in test_keywords:
                info = enriched_keyword_check(kw, sku_df)
                if info["has_match"]:
                    types = info["match_types"]
                    print("  ✅ '{}' (→{}) : {}개 매칭 [직접:{}, 동의어:{}, 성분:{}]".format(
                        kw, info["canonical"], info["matched_count"],
                        types["direct"], types["synonym"], types["ingredient"]
                    ))
                    for p in info["matched_products"][:3]:
                        print("     - {}".format(p))
                else:
                    print("  ❌ '{}' (→{}) : 매칭 없음".format(kw, info["canonical"]))
        else:
            print("  SKU 파일 없음 - 스킵")

    except Exception as e:
        print("  오류: {}".format(e))

    # 역방향 인덱스 확인
    print("\n[5] 성분→상품 역인덱스 (일부)")
    for ing, prods in sorted(INGREDIENT_TO_PRODUCTS.items())[:10]:
        print("  '{}' → {}".format(ing, prods))

    print("\n=== 통계 ===")
    print("동의어 그룹: {}개".format(len(KEYWORD_SYNONYMS)))
    print("역방향 매핑: {}개".format(len(REVERSE_SYNONYM_MAP)))
    print("상품-성분 매핑: {}개 상품".format(len(PRODUCT_INGREDIENTS)))
    print("성분→상품 역인덱스: {}개 성분".format(len(INGREDIENT_TO_PRODUCTS)))
