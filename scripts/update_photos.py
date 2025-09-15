import os
import io
import json
import base64
import hashlib
import logging
import signal
import atexit
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

from PIL import Image
from openpyxl import load_workbook

import argparse

# ---------- Paths / Constants ----------
INBOX = Path("inbox")
EXCEL_PATH = Path("data/photos.xlsx")
SHEET_NAME = "Photos"
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "update.log"
LOCK_FILE = Path(".update.lock")  # PID lock file in repo root
LOCK_STALE_AFTER = timedelta(hours=2)

SUPPORTED_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".tiff", ".heic", ".heif"}

# GPT config
GPT_MODEL = "gpt-4o-mini"  # good quality + cost
MAX_TAGS = 35               # Shutterstock allows up to 50; we keep it lean
PER_IMAGE_SLEEP = 0.3       # soft pacing to avoid rate limits

# ---------- Logging Setup ----------
def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("photo_manager")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        console = logging.StreamHandler()
        console.setFormatter(formatter)
        logger.addHandler(console)

    return logger

log = setup_logging()

# ---------- Locking ----------
_lock_acquired = False

def _remove_lock():
    """Remove lock file on exit if we created it."""
    global _lock_acquired
    try:
        if _lock_acquired and LOCK_FILE.exists():
            LOCK_FILE.unlink()
            log.info("Lock released")
    except Exception as e:
        log.warning(f"Failed to remove lock file: {e}")

def acquire_lock():
    """
    Acquire an exclusive run lock using an atomic PID file.
    If a fresh lock exists, exit. If stale, replace it.
    """
    global _lock_acquired

    if LOCK_FILE.exists():
        try:
            mtime = datetime.fromtimestamp(LOCK_FILE.stat().st_mtime)
            age = datetime.now() - mtime
            if age > LOCK_STALE_AFTER:
                log.warning(f"Existing lock is stale (age {age}). Overriding.")
                LOCK_FILE.unlink(missing_ok=True)
            else:
                try:
                    pid_txt = LOCK_FILE.read_text().strip()
                except Exception:
                    pid_txt = "unknown"
                log.error(f"Another run appears active (lock {LOCK_FILE}, pid {pid_txt}, age {age}). Exiting.")
                raise SystemExit(1)
        except FileNotFoundError:
            pass

    try:
        fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w") as f:
            f.write(str(os.getpid()))
        _lock_acquired = True
        log.info(f"Lock acquired (pid {os.getpid()})")
    except FileExistsError:
        log.error("Lock already exists, exiting.")
        raise SystemExit(1)

    atexit.register(_remove_lock)

    def _signal_handler(signum, frame):
        log.info(f"Received signal {signum}, cleaning up...")
        _remove_lock()
        raise SystemExit(130)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _signal_handler)
        except Exception:
            pass

# ---------- Helpers ----------
def sha256sum(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def get_metadata(file_path: Path) -> Dict:
    """Extract metadata from image file."""
    stat = file_path.stat()
    size_kb = round(stat.st_size / 1024, 2)
    created_time = datetime.fromtimestamp(stat.st_ctime).isoformat()
    modified_time = datetime.fromtimestamp(stat.st_mtime).isoformat()

    width = height = None
    try:
        with Image.open(file_path) as img:
            width, height = img.size
    except Exception as e:
        log.warning(f"Failed to read image dimensions for {file_path}: {e}")

    return {
        "file_name": file_path.name,
        "rel_path": str(file_path.relative_to(INBOX.parent)),
        "abs_path": str(file_path.resolve()),
        "size_kb": size_kb,
        "width": width,
        "height": height,
        "created_time": created_time,
        "modified_time": modified_time,
        "sha256": sha256sum(file_path),
        "phash": "",  # placeholder for future dedupe
        "description": "",
        "tags": "",
        "status": "NEW",
        "notes": "",
        "last_seen": datetime.utcnow().isoformat(),
    }

def load_existing_rows(ws) -> Dict[str, Tuple[int, Dict]]:
    """Read all existing rows into a dict keyed by abs_path -> (rownum, row_dict)."""
    existing = {}
    headers = [cell.value for cell in ws[1]]
    for i, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        if not row:
            continue
        row_dict = dict(zip(headers, row))
        key = row_dict.get("abs_path")
        if key:
            existing[key] = (i, row_dict)
    return existing

def scan_inbox() -> List[Dict]:
    """Scan inbox/ for supported image files."""
    if not INBOX.exists():
        log.error("inbox/ folder not found.")
        return []

    files = []
    for file_path in INBOX.rglob("*"):
        if file_path.is_file() and file_path.suffix.lower() in SUPPORTED_EXTS:
            try:
                files.append(get_metadata(file_path))
            except Exception as e:
                log.error(f"Metadata extraction failed for {file_path}: {e}")
    return files

def reset_excel():
    """Clear all rows except header in Excel file."""
    if not EXCEL_PATH.exists():
        log.error(f"{EXCEL_PATH} not found")
        return
    wb = load_workbook(EXCEL_PATH)
    if SHEET_NAME not in wb.sheetnames:
        log.error(f"Sheet '{SHEET_NAME}' not found in {EXCEL_PATH}")
        return
    ws = wb[SHEET_NAME]
    ws.delete_rows(2, ws.max_row)
    wb.save(EXCEL_PATH)
    log.info(f"Reset {EXCEL_PATH} (headers only kept)")

def open_workbook():
    if not EXCEL_PATH.exists():
        raise FileNotFoundError(f"{EXCEL_PATH} not found")
    wb = load_workbook(EXCEL_PATH)
    if SHEET_NAME not in wb.sheetnames:
        raise RuntimeError(f"Sheet '{SHEET_NAME}' not found in {EXCEL_PATH}")
    ws = wb[SHEET_NAME]
    headers = [cell.value for cell in ws[1]]
    return wb, ws, headers

def update_excel(file_metadata_list: List[Dict]) -> Tuple[int, int]:
    """Update Excel with new/changed metadata. Returns (added_count, updated_count)."""
    wb, ws, headers = open_workbook()
    existing = load_existing_rows(ws)

    added = updated = 0
    for meta in file_metadata_list:
        key = meta["abs_path"]
        if key in existing:
            rownum, old_row = existing[key]
            for col, header in enumerate(headers, start=1):
                if header in ("description", "tags", "status", "notes"):
                    continue
                ws.cell(row=rownum, column=col, value=meta.get(header, ""))
            updated += 1
        else:
            ws.append([meta.get(h, "") for h in headers])
            added += 1

    wb.save(EXCEL_PATH)
    return added, updated

# ---------- GPT Integration ----------
def load_api_key() -> Optional[str]:
    # 1) config.json
    cfg = Path("config.json")
    if cfg.exists():
        try:
            with open(cfg) as f:
                data = json.load(f)
                if data.get("OPENAI_API_KEY"):
                    return data["OPENAI_API_KEY"]
        except Exception as e:
            log.warning(f"Failed to read config.json: {e}")
    # 2) env var
    return os.getenv("OPENAI_API_KEY")

def make_preview_b64(path: Path, max_side: int = 1024, jpeg_quality: int = 80) -> str:
    """Create a resized JPEG preview and return data URL (base64)."""
    with Image.open(path) as img:
        img = img.convert("RGB")
        img.thumbnail((max_side, max_side))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=jpeg_quality, optimize=True)
        b64 = base64.b64encode(buf.getvalue()).decode("ascii")
        return f"data:image/jpeg;base64,{b64}"

def prompt_for_shutterstock():
    system = (
        "You are a metadata generator for Shutterstock. "
        "Write a concise, factual, search-friendly photo description (<= 180 chars). "
        "Avoid opinions, branding, private info, and spam. "
        f"Return strict JSON with keys: description (string), tags (array, max {MAX_TAGS}, singular nouns, ordered by relevance)."
    )
    user = (
        "Look at the image and produce JSON. "
        "Description should mention main subject and context (e.g., location or action if clear). "
        f"Tags: 15–{MAX_TAGS} keywords, no duplicates, no brand names, no people names unless obviously public figures."
    )
    return system, user

def call_gpt_for_image(api_key: str, img_path: Path) -> Optional[Dict]:
    try:
        from openai import OpenAI
    except Exception as e:
        log.error(f"OpenAI SDK not installed: {e}")
        return None

    client = OpenAI(api_key=api_key)
    data_url = make_preview_b64(img_path)
    system, user = prompt_for_shutterstock()

    try:
        # Using Chat Completions with image + strict JSON ask
        resp = client.chat.completions.create(
            model=GPT_MODEL,
            temperature=0.2,
            messages=[
                {"role": "system", "content": system},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": user},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                },
            ],
            response_format={"type": "json_object"},
        )
        text = resp.choices[0].message.content.strip()
        data = json.loads(text)
        # Basic normalization
        desc = (data.get("description") or "").strip()
        tags = [t.strip() for t in (data.get("tags") or []) if isinstance(t, str) and t.strip()]
        tags = list(dict.fromkeys(tags))[:MAX_TAGS]  # de-dupe + cap
        return {"description": desc, "tags": tags}
    except Exception as e:
        log.error(f"GPT call failed for {img_path.name}: {e}")
        return None

def describe_needed_rows(limit: Optional[int] = None) -> int:
    """
    For rows where description/tags are empty AND status in {NEW, ANALYZED},
    call GPT and write back. Returns count updated.
    """
    api_key = load_api_key()
    if not api_key:
        log.error("Missing OPENAI_API_KEY (config.json or env). Cannot describe.")
        return 0

    wb, ws, headers = open_workbook()
    col_index = {h: i+1 for i, h in enumerate(headers)}

    updated = 0
    start_row = 2
    end_row = ws.max_row
    for r in range(start_row, end_row + 1):
        status = ws.cell(row=r, column=col_index["status"]).value or ""
        desc = (ws.cell(row=r, column=col_index["description"]).value or "").strip()
        tags_cell = (ws.cell(row=r, column=col_index["tags"]).value or "").strip()
        abs_path = ws.cell(row=r, column=col_index["abs_path"]).value or ""
        file_name = ws.cell(row=r, column=col_index["file_name"]).value or ""

        if status not in ("NEW", "ANALYZED"):
            continue
        if desc and tags_cell:
            continue
        if not abs_path:
            continue

        img_path = Path(abs_path)
        if not img_path.exists():
            log.warning(f"File missing on disk for description: {abs_path}")
            continue

        log.info(f"Describing: {file_name}")
        result = call_gpt_for_image(api_key, img_path)
        if not result:
            continue

        # Write results
        ws.cell(row=r, column=col_index["description"], value=result["description"])
        ws.cell(row=r, column=col_index["tags"], value=", ".join(result["tags"]))
        ws.cell(row=r, column=col_index["status"], value="DESCRIBED")
        ws.cell(row=r, column=col_index["last_seen"], value=datetime.utcnow().isoformat())
        updated += 1

        # Soft pacing to reduce rate-limit risk
        time.sleep(PER_IMAGE_SLEEP)

        if limit and updated >= limit:
            break

    if updated:
        wb.save(EXCEL_PATH)
    log.info(f"Describe step updated {updated} row(s)")
    return updated

# ---------- CLI ----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scan inbox/ and sync data/photos.xlsx")
    parser.add_argument("--reset", action="store_true", help="Reset Excel (keep headers only)")
    parser.add_argument("--describe", action="store_true", help="Generate description & tags with GPT for rows needing it")
    parser.add_argument("--describe-limit", type=int, default=None, help="Max rows to process in a single describe run")
    args = parser.parse_args()

    # Locking first
    acquire_lock()

    if args.reset:
        log.info("Starting reset...")
        reset_excel()
    else:
        log.info("Scan started")
        metas = scan_inbox()
        log.info(f"Discovered {len(metas)} candidate files")
        try:
            added, updated = update_excel(metas)
            log.info(f"Excel updated: added={added}, updated={updated}")
            print(f"✅ Updated {EXCEL_PATH} | added={added}, updated={updated}")
        except Exception as e:
            log.exception(f"Excel update failed: {e}")
            print("❌ Excel update failed (see logs/update.log)")

        if args.describe:
            try:
                n = describe_needed_rows(limit=args.describe_limit)
                print(f"📝 Describe step updated {n} row(s)")
            except Exception as e:
                log.exception(f"Describe step failed: {e}")
                print("❌ Describe step failed (see logs/update.log)")
