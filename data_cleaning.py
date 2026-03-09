"""
마트약국 SKU 데이터 정제 스크립트
- 원본: 신규_sku분류.xlsx
- 결과: 신규_sku분류_정제.xlsx
"""

import pandas as pd
from pathlib import Path

# 경로 설정
BASE_DIR = Path(__file__).parent
INPUT_FILE = BASE_DIR / "신규_sku분류.xlsx"
OUTPUT_FILE = BASE_DIR / "신규_sku분류_정제.xlsx"


def load_data(filepath: Path) -> pd.DataFrame:
    """엑셀 파일 로드 (헤더가 2행째에 있음)"""
    df = pd.read_excel(filepath, engine="openpyxl", header=1)
    # 불필요한 Unnamed 컬럼 제거
    df = df.drop(columns=[c for c in df.columns if "Unnamed" in str(c)], errors="ignore")
    # 상품명이 없는 행 제거
    df = df.dropna(subset=["상품명"])
    return df


def clean_categories(df: pd.DataFrame) -> pd.DataFrame:
    """대분류명 불일치 수정"""
    replacements = {
        "두피・탈모": "두피・탈모_비적립",         # 접미사 누락
        "두피・탈모 _비적립": "두피・탈모_비적립",  # 공백 불일치
        "알레르기・비염약 _비적립": "알레르기・비염약_비적립",  # 공백 불일치
        "여성 건강_적립": "여성건강_적립",          # 띄어쓰기 불일치
    }

    for old, new in replacements.items():
        mask = df["신규 대분류"] == old
        count = mask.sum()
        if count > 0:
            df.loc[mask, "신규 대분류"] = new
            print(f"  ✅ '{old}' → '{new}' ({count}건 수정)")

    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """완전 중복 행 제거"""
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    removed = before - after
    if removed > 0:
        print(f"  ✅ 중복 행 {removed}건 제거")
    else:
        print(f"  ℹ️  중복 행 없음")
    return df


def print_summary(df: pd.DataFrame, label: str):
    """데이터 요약 출력"""
    print(f"\n{'='*50}")
    print(f"📊 {label}")
    print(f"{'='*50}")
    print(f"  총 SKU 수: {len(df)}")
    print(f"  대분류 수: {df['신규 대분류'].nunique()}")
    print(f"  세부분류 수: {df['신규 세부분류'].nunique()}")

    med = df[df["신규 대분류"].str.contains("비적립", na=False)]
    non_med = df[
        df["신규 대분류"].str.contains("적립", na=False)
        & ~df["신규 대분류"].str.contains("비적립", na=False)
    ]
    print(f"  의약품(비적립): {len(med)}개 ({len(med)/len(df)*100:.1f}%)")
    print(f"  비의약품(적립): {len(non_med)}개 ({len(non_med)/len(df)*100:.1f}%)")

    print(f"\n  [대분류 목록]")
    for cat in sorted(df["신규 대분류"].unique()):
        count = len(df[df["신규 대분류"] == cat])
        print(f"    {cat}: {count}개")


def main():
    print("🔧 마트약국 SKU 데이터 정제 시작\n")

    # 1. 데이터 로드
    print("1️⃣  데이터 로드")
    df = load_data(INPUT_FILE)
    print(f"  📄 {INPUT_FILE.name} 로드 완료 ({len(df)}행)")

    # 정제 전 요약
    print_summary(df, "정제 전 데이터")

    # 2. 대분류명 정제
    print(f"\n2️⃣  대분류명 정제")
    df = clean_categories(df)

    # 3. 중복 제거
    print(f"\n3️⃣  중복 행 제거")
    df = remove_duplicates(df)

    # 정제 후 요약
    print_summary(df, "정제 후 데이터")

    # 4. 저장
    print(f"\n4️⃣  저장")
    df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")
    print(f"  💾 {OUTPUT_FILE.name} 저장 완료")

    print(f"\n✨ 데이터 정제 완료!")


if __name__ == "__main__":
    main()
