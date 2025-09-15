import os
import hashlib
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple

from PIL import Image
from openpyxl import load_workbook

import argparse

# ---------- Paths / Constants ----------
INBOX = Path("inbox")
EXCEL_PATH = Path("data/photos.xlsx")
SHEET_NAME = "Photos"
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "update.log"

SUPPORTED_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".tiff", ".heic", ".heif"}

# ---------- Logging Setup ----------
def setup_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("photo_manager")
    logger.setLevel(logging.INFO)

    # Avoid double handlers if script is reloaded
    if not logger.handlers:
        handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Also echo to console
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        logger.addHandler(console)

    return logger

log = setup_logging()

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

def update_excel(file_metadata_list: List[Dict]) -> Tuple[int, int]:
    """Update Excel with new/changed metadata. Returns (added_count, updated_count)."""
    if not EXCEL_PATH.exists():
        raise FileNotFoundError(f"{EXCEL_PATH} not found")

    wb = load_workbook(EXCEL_PATH)
    if SHEET_NAME not in wb.sheetnames:
        raise RuntimeError(f"Sheet '{SHEET_NAME}' not found in {EXCEL_PATH}")
    ws = wb[SHEET_NAME]

    headers = [cell.value for cell in ws[1]]
    existing = load_existing_rows(ws)

    added = updated = 0

    for meta in file_metadata_list:
        key = meta["abs_path"]
        if key in existing:
            rownum, old_row = existing[key]
            # Update only metadata + last_seen; preserve manual fields
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

# ---------- CLI ----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scan inbox/ and sync data/photos.xlsx")
    parser.add_argument("--reset", action="store_true", help="Reset Excel (keep headers only)")
    args = parser.parse_args()

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
