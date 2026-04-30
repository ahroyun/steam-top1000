#!/usr/bin/env python3
"""
Steam Chart Monitor
steamcharts.com 스크래핑으로 상위 1000개 게임을 매일 수집합니다.

수집 데이터:
  - 순위 / 동시접속자(CCU) / 전일대비 CCU 증감
  - 개발사 / 퍼블리셔 / 퍼블리셔 규모
  - 장르 / 가격 / 할인율
  - 긍정 리뷰 비율

롱런 기준:
  - 1주(7일) / 2주(14일) / 4주(28일) 이상 상위 1000위 유지
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
from datetime import date
import time
import os
import json

# ── 설정 ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
EXCEL_PATH  = os.path.join(SCRIPT_DIR, "steam_chart_monitor.xlsx")
JSON_PATH   = os.path.join(SCRIPT_DIR, "docs", "data.json")
TOP_N       = 1000
LONGRUN_1W  = 7
LONGRUN_2W  = 14
LONGRUN_4W  = 28

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ── 퍼블리셔 규모 분류 ────────────────────────────────────────────────────────
MAJOR_PUBLISHERS = {
    "Valve", "Valve Software",
    "Electronic Arts", "EA Games", "EA Sports",
    "Ubisoft", "Ubisoft Entertainment",
    "Activision", "Blizzard Entertainment", "Activision Blizzard",
    "Microsoft Studios", "Xbox Game Studios", "Microsoft Game Studios",
    "Take-Two Interactive", "2K Games", "2K", "Rockstar Games",
    "Bethesda Softworks", "Bethesda Game Studios", "ZeniMax Media",
    "Sony Interactive Entertainment", "PlayStation Studios", "PlayStation PC LLC",
    "Nintendo",
    "Square Enix", "SQUARE ENIX",
    "BANDAI NAMCO Entertainment", "Bandai Namco Games", "BANDAI NAMCO Entertainment Europe",
    "Capcom", "CAPCOM Co., Ltd.",
    "SEGA", "Sega",
    "Konami Digital Entertainment", "KONAMI",
    "Warner Bros. Games", "Warner Bros. Interactive Entertainment", "WB Games",
    "CD PROJEKT RED",
    "Riot Games",
    "Epic Games",
    "NEXON Korea", "Nexon", "NEXON",
    "NCSOFT", "NCSoft",
    "KRAFTON, Inc.", "PUBG Corporation", "Krafton",
    "miHoYo", "COGNOSPHERE", "HoYoverse",
    "Tencent Games", "Level Infinite",
    "NetEase Games",
    "Perfect World", "Perfect World Entertainment",
    "Wargaming",
    "Gaijin Entertainment",
    "505 Games",
}

MID_PUBLISHERS = {
    "Paradox Interactive",
    "Focus Entertainment", "Focus Home Interactive",
    "THQ Nordic",
    "Deep Silver",
    "Devolver Digital",
    "Team17", "Team17 Digital",
    "Kalypso Media",
    "Nacon",
    "Curve Games", "Curve Digital",
    "Raw Fury",
    "Humble Games",
    "Gearbox Publishing", "Gearbox Software",
    "Private Division",
    "tinyBuild",
    "Wired Productions",
    "Rebellion", "Rebellion Developments",
    "CI Games",
    "Tripwire Interactive",
    "1C Entertainment",
    "Daedalic Entertainment",
    "Giants Software",
    "Fatshark",
    "Frontier Developments",
    "Saber Interactive",
    "Aspyr",
    "Merge Games",
    "Maximum Games",
    "Modus Games",
    "Toplitz Productions",
    "Assemble Entertainment",
    "Microids",
    "astragon Entertainment",
    "Ravenscourt",
    "Thunderful Publishing",
    "Headup",
    "Bigben Interactive",
    "Focus Entertainment",
    "Fulqrum Publishing",
    "Libredia Entertainment",
}


def classify_publisher(pub: str) -> str:
    if not pub:
        return "미확인"
    if pub in MAJOR_PUBLISHERS:
        return "대형"
    if pub in MID_PUBLISHERS:
        return "중형"
    return "인디/소형"


# ── steamcharts 스크래핑 ──────────────────────────────────────────────────────

def scrape_steamcharts(n: int = 1000) -> list:
    """steamcharts.com에서 상위 N개 게임 수집 (25개/페이지)"""
    pages = (n + 24) // 25
    games = []

    for page in range(1, pages + 1):
        url = (
            "https://steamcharts.com/top"
            if page == 1
            else f"https://steamcharts.com/top/p/{page}"
        )
        print(f"  steamcharts 페이지 {page}/{pages} 수집 중...")

        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")

            table = soup.find("table", {"id": "top-table"})
            if not table:
                table = soup.find("table")
            if not table:
                print(f"    ⚠ 테이블을 찾을 수 없음 (페이지 {page})")
                break

            tbody = table.find("tbody") or table
            rows = tbody.find_all("tr")

            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 4:
                    continue

                link = row.find("a", href=lambda h: h and "/app/" in str(h))
                if not link:
                    continue

                try:
                    appid = int(link["href"].split("/app/")[-1].rstrip("/"))
                except (ValueError, IndexError):
                    continue

                def parse_num(text):
                    t = text.replace(",", "").replace("+", "").strip()
                    try:
                        return int(t)
                    except ValueError:
                        return 0

                rank      = parse_num(cells[0].get_text())
                name_sc   = link.get_text(strip=True)
                ccu       = parse_num(cells[2].get_text())
                peak_ccu  = parse_num(cells[3].get_text())

                if rank == 0:
                    rank = len(games) + 1

                games.append({
                    "rank":     rank,
                    "appid":    appid,
                    "name_sc":  name_sc,
                    "ccu":      ccu,
                    "peak_ccu": peak_ccu,
                })

                if len(games) >= n:
                    break

        except Exception as e:
            print(f"    ⚠ 스크래핑 오류 (페이지 {page}): {e}")

        time.sleep(1.2)

        if len(games) >= n:
            break

    return games[:n]


# ── Steam Store API ───────────────────────────────────────────────────────────

def fetch_store_details(appid: int) -> dict:
    """Steam Store API: 공식 이름, 개발사, 퍼블리셔, 장르, 가격"""
    url = (
        f"https://store.steampowered.com/api/appdetails/"
        f"?appids={appid}&cc=kr&filters=basic,genres,price_overview"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        app = r.json().get(str(appid), {})
        if app.get("success") and app.get("data"):
            d = app["data"]
            p = d.get("price_overview", {})
            genres = ", ".join(g["description"] for g in d.get("genres", []))
            devs   = ", ".join(d.get("developers", []))
            pubs   = d.get("publishers", [])
            pub    = pubs[0] if pubs else None
            return {
                "name":               d.get("name"),
                "developer":          devs or None,
                "publisher":          pub,
                "publisher_size":     classify_publisher(pub),
                "genres":             genres or None,
                "price_krw":          p.get("final", 0) // 100 if p else None,
                "discount_pct":       p.get("discount_percent", 0) if p else 0,
                "original_price_krw": p.get("initial", 0) // 100 if p else None,
            }
    except Exception:
        pass
    return {
        "name": None, "developer": None, "publisher": None,
        "publisher_size": "미확인", "genres": None,
        "price_krw": None, "discount_pct": 0, "original_price_krw": None,
    }


# ── Steam Reviews API ─────────────────────────────────────────────────────────

def fetch_reviews(appid: int) -> dict:
    """Steam Reviews API: 긍정/부정 리뷰 수"""
    url = (
        f"https://store.steampowered.com/appreviews/{appid}"
        f"?json=1&language=all&purchase_type=all&num_per_page=0"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        qs = r.json().get("query_summary", {})
        pos   = qs.get("total_positive", 0) or 0
        neg   = qs.get("total_negative", 0) or 0
        total = pos + neg
        pct   = round(pos / total * 100, 1) if total > 0 else 0
        return {
            "review_score_pct": pct,
            "positive_reviews": pos,
            "total_reviews":    total,
        }
    except Exception:
        pass
    return {"review_score_pct": 0, "positive_reviews": 0, "total_reviews": 0}


# ── 전일 대비 CCU 증감 계산 ───────────────────────────────────────────────────

def add_ccu_change(today_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    """오늘 CCU와 전일 CCU를 비교하여 증감 컬럼 추가"""
    today_df = today_df.copy()

    if existing_df.empty:
        today_df["ccu_change"]     = None
        today_df["ccu_change_pct"] = None
        return today_df

    today_str = date.today().isoformat()
    prev_dates = existing_df[existing_df["date"] != today_str]["date"].unique()

    if len(prev_dates) == 0:
        today_df["ccu_change"]     = None
        today_df["ccu_change_pct"] = None
        return today_df

    prev_date = max(prev_dates)
    prev_df   = existing_df[existing_df["date"] == prev_date][["appid", "ccu"]].copy()
    prev_df   = prev_df.rename(columns={"ccu": "ccu_prev"})

    merged = today_df.merge(prev_df, on="appid", how="left")
    merged["ccu_change"] = (merged["ccu"] - merged["ccu_prev"]).where(
        merged["ccu_prev"].notna()
    ).astype("Int64")
    merged["ccu_change_pct"] = (
        (merged["ccu"] - merged["ccu_prev"]) / merged["ccu_prev"] * 100
    ).where(merged["ccu_prev"].notna()).round(1)

    return merged.drop(columns=["ccu_prev"])


# ── 데이터 수집 ───────────────────────────────────────────────────────────────

def collect_today_data() -> pd.DataFrame:
    print("▶ steamcharts 상위 1000 게임 스크래핑 중...")
    sc_games = scrape_steamcharts(TOP_N)
    print(f"  총 {len(sc_games)}개 게임 수집 완료")

    today_str = date.today().isoformat()
    rows = []

    for i, g in enumerate(sc_games, 1):
        print(f"  [{i:4d}/{len(sc_games)}] AppID {g['appid']} ({g['name_sc'][:30]})")

        store   = fetch_store_details(g["appid"])
        reviews = fetch_reviews(g["appid"])
        time.sleep(0.5)

        name = store["name"] or g["name_sc"]

        rows.append({
            "date":               today_str,
            "rank":               g["rank"],
            "appid":              g["appid"],
            "name":               name,
            "developer":          store["developer"],
            "publisher":          store["publisher"],
            "publisher_size":     store["publisher_size"],
            "genres":             store["genres"],
            "ccu":                g["ccu"],
            "peak_ccu":           g["peak_ccu"],
            "review_score_pct":   reviews["review_score_pct"],
            "total_reviews":      reviews["total_reviews"],
            "positive_reviews":   reviews["positive_reviews"],
            "price_krw":          store["price_krw"],
            "discount_pct":       store["discount_pct"],
            "original_price_krw": store["original_price_krw"],
        })

    return pd.DataFrame(rows)


# ── 롱런 분석 ─────────────────────────────────────────────────────────────────

def analyze_longrun(df: pd.DataFrame, min_days: int) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    # publisher_size는 마지막 값으로
    stats = df.groupby(["appid", "name"]).agg(
        days_in_top         = ("date",             "nunique"),
        avg_rank            = ("rank",             "mean"),
        best_rank           = ("rank",             "min"),
        developer           = ("developer",        "last"),
        publisher           = ("publisher",        "last"),
        publisher_size      = ("publisher_size",   "last"),
        genres              = ("genres",           "last"),
        avg_ccu             = ("ccu",              "mean"),
        latest_ccu          = ("ccu",              "last"),
        avg_review_score    = ("review_score_pct", "mean"),
        latest_review_score = ("review_score_pct", "last"),
        total_reviews       = ("total_reviews",    "last"),
        latest_price        = ("price_krw",        "last"),
        max_discount        = ("discount_pct",     "max"),
        first_seen          = ("date",             "min"),
        last_seen           = ("date",             "max"),
    ).reset_index()

    result = stats[stats["days_in_top"] >= min_days].copy()
    result.sort_values("days_in_top", ascending=False, inplace=True)

    result["avg_rank"]         = result["avg_rank"].round(1)
    result["avg_ccu"]          = result["avg_ccu"].round(0).astype(int)
    result["avg_review_score"] = result["avg_review_score"].round(1)
    result["first_seen"]       = result["first_seen"].dt.strftime("%Y-%m-%d")
    result["last_seen"]        = result["last_seen"].dt.strftime("%Y-%m-%d")

    return result


# ── Excel 출력 ────────────────────────────────────────────────────────────────

HDR_FILL  = PatternFill("solid", start_color="1F4E79")
HDR_FONT  = Font(bold=True, color="FFFFFF", size=10)
ALT_FILL  = PatternFill("solid", start_color="EBF3FB")
DIS_FILL  = PatternFill("solid", start_color="C6EFCE")
LRN1_FILL = PatternFill("solid", start_color="E8F5E9")
LRN2_FILL = PatternFill("solid", start_color="FFF3CD")
LRN4_FILL = PatternFill("solid", start_color="FFD700")
UP_FILL   = PatternFill("solid", start_color="E8F5E9")
DOWN_FILL = PatternFill("solid", start_color="FDECEA")
CENTER    = Alignment(horizontal="center", vertical="center")


def _style_header(ws, row, ncols):
    for c in range(1, ncols + 1):
        cell = ws.cell(row=row, column=c)
        cell.fill = HDR_FILL
        cell.font = HDR_FONT
        cell.alignment = CENTER


def _set_col_widths(ws, widths):
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w


def build_excel(all_df, today_df, lr1, lr2, lr4):
    wb = Workbook()

    # ── 시트 1: 일별 스냅샷 ─────────────────────────────────────────────────
    ws1 = wb.active
    ws1.title = "일별 스냅샷"
    SNAP_COLS = {
        "date": "날짜", "rank": "순위", "appid": "AppID",
        "name": "게임명", "developer": "개발사", "publisher": "퍼블리셔",
        "publisher_size": "규모", "genres": "장르",
        "ccu": "동접자", "ccu_change": "전일증감", "ccu_change_pct": "증감(%)",
        "peak_ccu": "최고동접(오늘)",
        "review_score_pct": "긍정리뷰(%)", "total_reviews": "총 리뷰수",
        "price_krw": "가격(₩)", "discount_pct": "할인(%)", "original_price_krw": "정가(₩)",
    }
    ws1.append(list(SNAP_COLS.values()))
    _style_header(ws1, 1, len(SNAP_COLS))
    keys = list(SNAP_COLS.keys())
    for ri, row in enumerate(all_df.itertuples(index=False), 2):
        for ci, k in enumerate(keys, 1):
            ws1.cell(ri, ci, value=getattr(row, k, None))
        if ri % 2 == 0:
            for ci in range(1, len(keys) + 1):
                ws1.cell(ri, ci).fill = ALT_FILL
    _set_col_widths(ws1, [12,5,10,34,24,24,8,22,12,10,8,14,12,12,10,8,10])
    ws1.freeze_panes = "A2"

    # ── 시트 2: 오늘의 차트 ─────────────────────────────────────────────────
    ws2 = wb.create_sheet("오늘의 차트")
    ws2["A1"] = f"Steam 인기 차트 — {date.today().isoformat()}"
    ws2["A1"].font = Font(bold=True, size=13, color="1F4E79")
    ws2.append([])
    T_COLS = ["순위","게임명","개발사","퍼블리셔","규모","장르","동접자","전일증감","증감(%)","긍정리뷰(%)","가격(₩)","할인(%)"]
    T_KEYS = ["rank","name","developer","publisher","publisher_size","genres","ccu","ccu_change","ccu_change_pct","review_score_pct","price_krw","discount_pct"]
    ws2.append(T_COLS)
    _style_header(ws2, 3, len(T_COLS))
    for ri, row in enumerate(today_df.itertuples(index=False), 4):
        for ci, k in enumerate(T_KEYS, 1):
            ws2.cell(ri, ci, value=getattr(row, k, None))
        disc = getattr(row, "discount_pct", 0) or 0
        chg  = getattr(row, "ccu_change", None)
        if disc > 0:
            fill = DIS_FILL
        elif chg is not None and chg > 0:
            fill = UP_FILL
        elif ri % 2 == 0:
            fill = ALT_FILL
        else:
            fill = None
        if fill:
            for ci in range(1, len(T_KEYS) + 1):
                ws2.cell(ri, ci).fill = fill
    _set_col_widths(ws2, [5,34,24,24,8,22,12,10,8,12,10,8])
    ws2.freeze_panes = "A4"

    # ── 시트 3~5: 롱런 분석 ─────────────────────────────────────────────────
    LR_COLS = {
        "name": "게임명", "days_in_top": "유지 일수",
        "avg_rank": "평균 순위", "best_rank": "최고 순위",
        "developer": "개발사", "publisher": "퍼블리셔", "publisher_size": "규모",
        "genres": "장르",
        "avg_ccu": "평균 동접", "latest_ccu": "최근 동접",
        "avg_review_score": "평균 긍정리뷰(%)", "latest_review_score": "최근 긍정리뷰(%)",
        "total_reviews": "총 리뷰수",
        "latest_price": "현재 가격(₩)", "max_discount": "최대 할인(%)",
        "first_seen": "첫 관측일", "last_seen": "최근 관측일",
    }
    LR_WIDTHS = [34,10,10,10,24,24,8,22,12,12,16,16,12,14,10,14,14]

    def write_lr(ws, title, df, fill, min_days):
        ws["A1"] = title
        ws["A1"].font = Font(bold=True, size=13, color="B8860B")
        ws.append([])
        ws.append(list(LR_COLS.values()))
        _style_header(ws, 3, len(LR_COLS))
        if not df.empty:
            for ri, row in enumerate(df.itertuples(index=False), 4):
                for ci, k in enumerate(LR_COLS.keys(), 1):
                    ws.cell(ri, ci, value=getattr(row, k, None))
                for ci in range(1, len(LR_COLS) + 1):
                    ws.cell(ri, ci).fill = fill
        else:
            ws["A4"] = f"아직 {min_days}일 분량의 데이터가 쌓이지 않았습니다."
            ws["A4"].font = Font(italic=True, color="888888")
        _set_col_widths(ws, LR_WIDTHS)
        ws.freeze_panes = "A4"

    write_lr(wb.create_sheet("1주+ 롱런"),
             f"1주(7일)+ 상위 1000위 유지 — {date.today().isoformat()}", lr1, LRN1_FILL, LONGRUN_1W)
    write_lr(wb.create_sheet("2주+ 롱런"),
             f"2주(14일)+ 상위 1000위 유지 — {date.today().isoformat()}", lr2, LRN2_FILL, LONGRUN_2W)
    write_lr(wb.create_sheet("4주+ 롱런"),
             f"4주(28일)+ 상위 1000위 유지 — {date.today().isoformat()}", lr4, LRN4_FILL, LONGRUN_4W)

    wb.save(EXCEL_PATH)
    print(f"✔ Excel 저장: {EXCEL_PATH}")


# ── JSON 출력 ─────────────────────────────────────────────────────────────────

def write_json(today_df, lr1, lr2, lr4):
    def to_records(df, cols=None):
        if df.empty:
            return []
        d = df[cols] if cols else df
        return json.loads(d.to_json(orient="records", force_ascii=False))

    TODAY_COLS = [
        "rank","appid","name","developer","publisher","publisher_size",
        "genres","ccu","ccu_change","ccu_change_pct","peak_ccu",
        "review_score_pct","total_reviews","price_krw","discount_pct",
    ]
    data = {
        "updated":     date.today().isoformat(),
        "today_chart": to_records(today_df, TODAY_COLS),
        "longrun_1w":  to_records(lr1),
        "longrun_2w":  to_records(lr2),
        "longrun_4w":  to_records(lr4),
    }
    os.makedirs(os.path.dirname(JSON_PATH), exist_ok=True)
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✔ JSON 저장: {JSON_PATH}")


# ── 기존 데이터 로드 ──────────────────────────────────────────────────────────

def load_existing() -> pd.DataFrame:
    if not os.path.exists(EXCEL_PATH):
        return pd.DataFrame()
    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name="일별 스냅샷")
        df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
        return df
    except Exception as e:
        print(f"기존 파일 로드 실패 ({e}), 새로 시작합니다.")
        return pd.DataFrame()


# ── 메인 ──────────────────────────────────────────────────────────────────────

def main():
    today_str = date.today().isoformat()
    print(f"\n{'='*60}")
    print(f"  Steam Chart Monitor  |  {today_str}")
    print(f"{'='*60}")

    # 1. 오늘 데이터 수집
    today_df = collect_today_data()

    # 2. 기존 데이터 로드
    existing = load_existing()
    if not existing.empty:
        existing = existing[existing["date"] != today_str]

    # 3. 전일대비 CCU 증감 계산
    today_df = add_ccu_change(today_df, existing)

    # 4. 전체 데이터 합치기
    all_df = (
        pd.concat([existing, today_df], ignore_index=True)
        if not existing.empty
        else today_df
    )

    # 5. 롱런 분석
    lr1 = analyze_longrun(all_df.copy(), LONGRUN_1W)
    lr2 = analyze_longrun(all_df.copy(), LONGRUN_2W)
    lr4 = analyze_longrun(all_df.copy(), LONGRUN_4W)
    print(f"\n▶ 1주+ 롱런 게임: {len(lr1)}개")
    print(f"▶ 2주+ 롱런 게임: {len(lr2)}개")
    print(f"▶ 4주+ 롱런 게임: {len(lr4)}개")

    # 6. 저장
    build_excel(all_df, today_df, lr1, lr2, lr4)
    write_json(today_df, lr1, lr2, lr4)
    print("  완료!\n")


if __name__ == "__main__":
    main()
