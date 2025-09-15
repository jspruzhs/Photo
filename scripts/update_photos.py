import os
import hashlib
from pathlib import Path
from PIL import Image

INBOX = Path("inbox")

def sha256sum(file_path):
    """Compute SHA256 hash of a file."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def get_metadata(file_path):
    """Extract metadata from image file."""
    stat = file_path.stat()
    size_kb = round(stat.st_size / 1024, 2)
    created_time = stat.st_ctime
    modified_time = stat.st_mtime

    try:
        with Image.open(file_path) as img:
            width, height = img.size
    except Exception as e:
        width = height = None

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
    }

def scan_inbox():
    """Scan inbox/ for images and print metadata."""
    if not INBOX.exists():
        print("❌ inbox/ folder not found.")
        return

    supported_exts = {".jpg", ".jpeg", ".png", ".webp", ".tiff", ".heic", ".heif"}
    for file_path in INBOX.rglob("*"):
        if file_path.suffix.lower() in supported_exts:
            meta = get_metadata(file_path)
            print(meta)

if __name__ == "__main__":
    scan_inbox()
