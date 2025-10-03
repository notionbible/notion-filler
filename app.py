import os, time, math
import requests
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, Query
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# ---------- ENV ----------
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "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")
NOTION_DB_ID = os.getenv("NOTION_DB_ID", "280fa4f6a016801aa035ebf830af16a1")

PROP_PASSAGE   = os.getenv("NOTION_PROP_PASSAGE", "PassageKey")
PROP_VERSION   = os.getenv("NOTION_PROP_VERSION", "version")
PROP_TEXT      = os.getenv("NOTION_PROP_TEXT", "B_Text")
PROP_LOAD      = os.getenv("NOTION_PROP_LOAD", "Load")
PROP_LASTSYNC  = os.getenv("NOTION_PROP_LASTSYNCED", "LastSynced")

SUPA_URL   = os.getenv("SUPABASE_URL", "https://qjkhvclskqzdvxeuepor.supabase.co")
SUPA_KEY   = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFqa2h2Y2xza3F6ZHZ4ZXVlcG9yIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1OTIzNTYxOSwiZXhwIjoyMDc0ODExNjE5fQ.jMebE0xoT7sAefCIURdGg5U9ggdhWXAz4mdrS_Q3fB4")
SUPA_TABLE = os.getenv("SUPA_TABLE", "노션DB")
COL_PASS   = os.getenv("SUPA_COL_PASSAGE", "PassageKey")
COL_VER    = os.getenv("SUPA_COL_VERSION", "version")
COL_TEXT   = os.getenv("SUPA_COL_TEXT", "B_Text")


DEFAULT_VERSION = os.getenv("DEFAULT_VERSION", "KJV")
MAX_RICH_TEXT   = int(os.getenv("MAX_RICH_TEXT", "31000"))
SLEEP_MS        = int(os.getenv("SLEEP_MS", "150"))

assert NOTION_TOKEN and NOTION_DB_ID and SUPA_URL and SUPA_KEY, "환경변수(.env) 설정을 확인하세요."

# ---------- Clients ----------
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}
supabase: Client = create_client(SUPA_URL, SUPA_KEY)

# ---------- Helpers ----------
def sleep():
    time.sleep(SLEEP_MS / 1000.0)

def _rich_chunk(s: str) -> Dict[str, Any]:
    """노션 rich_text 한 조각으로 변환"""
    return {"type":"text","text":{"content":s}}

def chunk_text(s: str, chunk_size: int) -> List[Dict[str, Any]]:
    if not s:
        return []
    parts = [s[i:i+chunk_size] for i in range(0, len(s), chunk_size)]
    return [_rich_chunk(p) for p in parts]

def supa_get_text(passage: str, version: str) -> Optional[str]:
    """Supabase에서 본문 1건 가져오기"""
    try:
        res = supabase.table(SUPA_TABLE)\
            .select(COL_TEXT)\
            .eq(COL_PASS, passage)\
            .eq(COL_VER, version)\
            .execute()
        data = getattr(res, "data", []) or []
        if not data:  # version 없으면 fallback
            res2 = supabase.table(SUPA_TABLE)\
                .select(COL_TEXT)\
                .eq(COL_PASS, passage)\
                .execute()
            data = getattr(res2, "data", []) or []
        if data:
            return data[0].get(COL_TEXT) or ""
    except Exception as e:
        print("[Supabase ERROR]", e)
    return None

def notion_get_page(page_id: str) -> Dict[str, Any]:
    r = requests.get(f"https://api.notion.com/v1/pages/{page_id}", headers=NOTION_HEADERS)
    r.raise_for_status()
    return r.json()

def notion_update_page(page_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": properties}
    )
    r.raise_for_status()
    return r.json()

def extract_property(props: Dict[str, Any], key: str) -> Any:
    """Notion 속성 값 안전 추출 (select/text/rich/date/checkbox 등)"""
    if key not in props:
        return None
    val = props[key]
    t = val.get("type")
    if t == "select":
        return (val.get("select") or {}).get("name")
    if t == "multi_select":
        return [x.get("name") for x in val.get("multi_select") or []]
    if t == "rich_text":
        return "".join([x["plain_text"] for x in val.get("rich_text") or []])
    if t == "title":
        return "".join([x["plain_text"] for x in val.get("title") or []])
    if t == "number":
        return val.get("number")
    if t == "checkbox":
        return val.get("checkbox")
    if t == "date":
        return (val.get("date") or {}).get("start")
    if t == "url":
        return val.get("url")
    if t == "people":
        return val.get("people")
    if t == "status":
        return (val.get("status") or {}).get("name")
    # 기본적으로 raw 반환
    return val.get(t)

def notion_query_database(db_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.post(
        f"https://api.notion.com/v1/databases/{db_id}/query",
        headers=NOTION_HEADERS,
        json=payload
    )
    if r.status_code != 200:
        print("QUERY ERROR:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()

def fetch_pages_to_fill(db_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    조건:
      1) Load = true 이거나
      2) 본문(B_Text) 비어 있음
    """
    blocks: List[Dict[str, Any]] = []
    start_cursor = None
    while True:
        filters = {
            "or": [
                {"property": PROP_LOAD, "checkbox": {"equals": True}},
                {"property": PROP_TEXT, "rich_text": {"is_empty": True}}
            ]
        }
        payload = {"page_size": min(limit, 100), "filter": filters}
        if start_cursor:
            payload["start_cursor"] = start_cursor
        data = notion_query_database(db_id, payload)
        results = data.get("results", [])
        blocks.extend(results)
        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")
        sleep()
    return blocks

# ---------- Fillers ----------
def fill_one_page(page_id: str) -> Dict[str, Any]:
    """단건: page_id 기준으로 Passage/Version 읽고 Supabase 본문을 넣는다."""
    page = notion_get_page(page_id)
    props = page.get("properties", {})

    passage = extract_property(props, PROP_PASSAGE)
    version  = extract_property(props, PROP_VERSION) or DEFAULT_VERSION
    if not passage:
        return {"page_id": page_id, "ok": False, "msg": f"'{PROP_PASSAGE}' 없음"}

    body = supa_get_text(passage, version)
    if body is None:
        return {"page_id": page_id, "ok": False, "msg": f"Supabase 본문 없음: {passage}/{version}"}

    rich = chunk_text(body, MAX_RICH_TEXT)
    update_props = {
        PROP_TEXT: {"rich_text": rich},
    }
    if PROP_LASTSYNC:
        update_props[PROP_LASTSYNC] = {"date": {"start": time.strftime("%Y-%m-%dT%H:%M:%S")}}
    if PROP_LOAD in props and props[PROP_LOAD]["type"] == "checkbox":
        update_props[PROP_LOAD] = {"checkbox": False}

    updated = notion_update_page(page_id, update_props)
    return {"page_id": page_id, "ok": True, "chars": len(body or ""), "passage": passage, "version": version}

def fill_batch(db_id: str, dry_run: bool = False, hard_empty_only: bool = False) -> Dict[str, Any]:
    """
    DB 전체 배치:
      - Load=true 또는 B_Text 비어있는 페이지를 수집
      - dry_run=True면 수정 없이 대상 리스트만 반환
      - hard_empty_only=True면 'B_Text 비어있는' 페이지만 강제
    """
    pages = fetch_pages_to_fill(db_id)
    if hard_empty_only:
        pages = [p for p in pages if not extract_property(p.get("properties", {}), PROP_TEXT)]

    result = {"target_count": len(pages), "updated": 0, "items": []}
    if dry_run:
        result["items"] = [p["id"] for p in pages]
        return result

    for p in pages:
        pid = p["id"]
        try:
            info = fill_one_page(pid)
            result["items"].append(info)
            if info.get("ok"):
                result["updated"] += 1
        except Exception as e:
            result["items"].append({"page_id": pid, "ok": False, "error": str(e)})
        sleep()

    return result

# ---------- FastAPI ----------
app = FastAPI(title="Notion Bible Filler", version="1.0.0")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/fill")
def fill(page_id: str = Query(..., description="Notion Page ID")):
    """개별 행 즉시 채움 (Formula용)"""
    try:
        return fill_one_page(page_id)
    except Exception as e:
        return {"ok": False, "error": str(e), "page_id": page_id}

@app.get("/fill-batch")
def fill_batch_api(
    database_id: str = Query(NOTION_DB_ID, description="Notion DB ID"),
    dry_run: bool = Query(False, description="대상만 미리 보기"),
    hard_empty_only: bool = Query(False, description="본문 비어있는 페이지만")
):
    """한 번에 채움: 브라우저에서 단 한 번 호출"""
    try:
        return fill_batch(database_id, dry_run=dry_run, hard_empty_only=hard_empty_only)
    except Exception as e:
        return {"ok": False, "error": str(e), "database_id": database_id}
