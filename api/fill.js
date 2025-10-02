// api/fill.js
import { Client } from "@notionhq/client";
import { createClient } from "@supabase/supabase-js";

export default async function handler(req, res) {
  try {
    // ── 1) 보안 키 확인(임의의 인증키) ───────────────────────────────
    const { query } = req;
    const key = query.key;
    if (!process.env.FILL_SECRET || key !== process.env.FILL_SECRET) {
      return res.status(401).send("unauthorized");
    }

    // ── 2) 환경변수 읽기 ───────────────────────────────────────────
    const NOTION_TOKEN = process.env.NOTION_TOKEN;
    const WORD_DB_ID = query.wordDbId || process.env.NOTION_WORD_DB_ID;
    const PHOTO_DB_ID = query.photoDbId || process.env.NOTION_PHOTOS_DB_ID;

    const ORDER_PROP = query.orderProp || process.env.NOTION_ORDER_PROP || "Date1"; // 순서 열
    const VERSEKEY_PROP = process.env.NOTION_VERSEKEY_PROP || "VerseKey";          // 구절키 열
    const TEXT_PROP = process.env.NOTION_TEXT_PROP || "Text";                      // 본문 Text 열
    const SYNCED_PROP = process.env.NOTION_SYNCED_PROP || "Synced";                // 체크박스(선택)
    const VERSION_PROP = process.env.NOTION_VERSION_PROP || "Version";             // 번역본 열(선택)
    const PHOTOURL_PROP = process.env.NOTION_PHOTOURL_PROP || "PhotoURL";          // 사진 URL 저장(선택)

    const SUPABASE_URL = process.env.SUPABASE_URL;
    const SUPABASE_SECRET = process.env.SUPABASE_SECRET;
    const TEXT_TABLE = process.env.SUPABASE_TEXT_TABLE || "bible_texts";
    const PASSAGE_COL = process.env.SUPABASE_TEXT_PASSAGE_COL || "Passage(en)";
    const VERSION_COL = process.env.SUPABASE_TEXT_VERSION_COL || "version";
    const TEXT_COL = process.env.SUPABASE_TEXT_TEXT_COL || "text";
    const DEFAULT_VERSION = process.env.DEFAULT_VERSION || "KJV";

    // ── 3) 클라이언트 생성 ─────────────────────────────────────────
    const notion = new Client({ auth: NOTION_TOKEN });
    const supabase = createClient(SUPABASE_URL, SUPABASE_SECRET);

    // ── 4) 대상 인덱스 계산 ────────────────────────────────────────
    let index;
    if (query.index) {
      index = parseInt(query.index, 10);
    } else {
      // "오늘 날짜 → 1~365" 형태로 단순 매핑
      const now = new Date();
      const start = new Date(now.getFullYear(), 0, 1);
      const diff = Math.floor((now - start) / (1000 * 60 * 60 * 24)); // 0-based
      index = (diff % 365) + 1; // 1~365
    }

    // ── 5) Word Cards DB에서 index번째 페이지 찾기 ──────────────────
    //    ORDER_PROP이 "Date1"(숫자) 또는 "365Days"(숫자) 같은 열이라고 가정
    const pageList = await notion.databases.query({
      database_id: WORD_DB_ID,
      filter: {
        property: ORDER_PROP,
        number: { equals: index }
      },
      page_size: 1
    });

    if (pageList.results.length === 0) {
      return res.status(404).send(`no page found where ${ORDER_PROP}=${index}`);
    }

    const page = pageList.results[0];
    const pageId = page.id;

    // ── 6) 구절 키 & 번역본 읽기 ───────────────────────────────────
    // VerseKey는 plain text property라 가정(예: "John 3:16")
    const verseKey =
      page.properties[VERSEKEY_PROP]?.rich_text?.[0]?.plain_text ||
      page.properties[VERSEKEY_PROP]?.title?.[0]?.plain_text ||
      page.properties[VERSEKEY_PROP]?.formula?.string ||
      null;

    // Version은 선택(없으면 DEFAULT_VERSION)
    const pageVersion =
      page.properties[VERSION_PROP]?.select?.name ||
      DEFAULT_VERSION;

    if (!verseKey) {
      return res.status(400).send(`page has no "${VERSEKEY_PROP}" value`);
    }

    // ── 7) Supabase에서 본문 텍스트 가져오기 ───────────────────────
    const { data: rows, error } = await supabase
      .from(TEXT_TABLE)
      .select(TEXT_COL)
      .eq(PASSAGE_COL, verseKey)
      .eq(VERSION_COL, pageVersion)
      .limit(1);

    if (error) throw error;
    const verseText = rows?.[0]?.[TEXT_COL] || "";

    // ── 8) 랜덤 사진 고르기(선택) ──────────────────────────────────
    let photoUrl = null;
    if (PHOTO_DB_ID) {
      // 활성 필터가 있다면 적절히 수정(예: checkbox "Active" is true)
      const photos = await notion.databases.query({
        database_id: PHOTO_DB_ID,
        page_size: 50,
        filter: query.activeProp
          ? { property: query.activeProp, checkbox: { equals: true } }
          : undefined
      });
      if (photos.results.length > 0) {
        const pick = photos.results[Math.floor(Math.random() * photos.results.length)];
        // 표지 이미지: 페이지 cover 사용 또는 파일 속성에서 URL 읽기
        const cover = pick.cover;
        if (cover?.external?.url) photoUrl = cover.external.url;
        if (cover?.file?.url) photoUrl = cover.file.url;

        // 파일 속성(예: "Image") 안에 있는 경우
        if (!photoUrl) {
          const props = pick.properties;
          const firstFileProp = Object.values(props).find(p => p.type === "files" && p.files?.length);
          const f = firstFileProp?.files?.[0];
          if (f?.external?.url) photoUrl = f.external.url;
          if (f?.file?.url) photoUrl = f.file.url;
        }
      }
    }

    // ── 9) Word Card 페이지 업데이트(본문 / 사진 / 동기화 체크) ─────
    const updatePayload = { properties: {} };

    // Text 속성(리치텍스트)
    updatePayload.properties[TEXT_PROP] = {
      rich_text: [{ type: "text", text: { content: verseText || "" } }]
    };

    // 선택: PhotoURL 속성에 URL 저장
    if (PHOTOURL_PROP && photoUrl) {
      updatePayload.properties[PHOTOURL_PROP] = {
        url: photoUrl
      };
    }

    // 선택: Synced 체크
    if (SYNCED_PROP && page.properties[SYNCED_PROP]?.type === "checkbox") {
      updatePayload.properties[SYNCED_PROP] = { checkbox: true };
    }

    // 표지 이미지(cover)도 변경(표지 사용시)
    if (photoUrl) {
      updatePayload.cover = { type: "external", external: { url: photoUrl } };
    }

    await notion.pages.update({ page_id: pageId, ...updatePayload });

    return res
      .status(200)
      .json({ ok: true, index, pageId, verseKey, version: pageVersion, photoUrl });
  } catch (e) {
    console.error(e);
    return res.status(500).send(String(e?.message || e));
  }
}
