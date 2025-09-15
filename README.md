# Photo — Local stock workflow

Lightweight local pipeline to:
1) **Scan** an `inbox/` folder for images, extract metadata, and generate small **previews**  
2) Create & maintain an Excel catalog at `data/photos.xlsx`  
3) Generate **base** title/description/tags (platform-agnostic) with GPT  
4) Adapt the base metadata to **Shutterstock** (SS_*) — with a small, cheap text-only step

Everything runs through a Python 3.11 virtualenv via `make` (no manual activation).

---

## Project structure

├─ inbox/ # Drop images here (you manage this)
├─ previews/ # Auto-generated JPEG previews (gitignored)
├─ data/
│ └─ photos.xlsx # Main catalog (created by scripts or one-liner)
├─ logs/ # Rotating logs (gitignored)
├─ scripts/
│ ├─ update_photos.py # Scan, previews, Excel sync, base_* with GPT
│ └─ adapt_shutterstock.py # base_* -> SS_* adapter (title/desc/tags)
├─ requirements.txt
├─ Makefile
├─ config.json # { "OPENAI_API_KEY": "sk-..." } (gitignored)
└─ .gitignore



**.gitignore** should include at least: `data/`, `inbox/`, `previews/`, `logs/`, `config.json`.

---

## Prerequisites

- **Python 3.11**
- An OpenAI API key (put it into `config.json` as shown below)

{
  "OPENAI_API_KEY": "sk-REPLACE_ME"
}


Both scripts read config.json automatically. Alternatively, you can export OPENAI_API_KEY in your shell.

# 1) Install deps into ./.venv
make install

# 2) Put some images into ./inbox/

# 3) Scan + preview + sync Excel (also auto-enables all *_enabled columns by default)
make scan

# 4) Generate base title/description/tags with GPT (uses previews; cheap)
make describe            # add LIMIT=20 to cap the batch

# 5) Adapt to Shutterstock fields (text-only transformation)
make adapt-ss            # add LIMIT=50, or FORCE=1, or DRY_RUN=1

Makefile targets

make venv — Create .venv with Python 3.11

make install — Install requirements into .venv

make scan — Run scripts/update_photos.py

Scans inbox/, extracts metadata, generates previews, and writes/updates data/photos.xlsx

Enables all *_enabled columns by default (new rows and backfills blanks in existing rows)

make describe [LIMIT=N] — Run update_photos.py --describe

Generates base_title, base_description, base_tags from previews

Sets status_global=DESCRIBED for completed rows

make reset — Clears all rows from Photos sheet (keeps headers)

make adapt-ss [LIMIT=N] [FORCE=1] [DRY_RUN=1] — Run Shutterstock adapter

Reads base_*, produces SS_title, SS_description, SS_tags

Writes SS_status=READY_TO_UPLOAD when QA passes

FORCE=1 overwrites existing SS_*; DRY_RUN=1 computes without writing

make all — scan → describe → adapt-ss

make clean — Remove Python caches (keeps data and venv)

make help — Show help & examples

Examples:

make describe LIMIT=25

make adapt-ss LIMIT=100

make adapt-ss FORCE=1

make reset

Excel schema (summary)

Photos sheet (core columns used by scripts):

Core metadata: file_name, rel_path, abs_path, size_kb, width, height, created_time, modified_time, sha256, phash

Previews: preview_path, preview_width, preview_height, preview_size_kb

Pipeline: status_global, notes, last_seen

Base metadata (AI-generated once): base_title, base_description, base_tags

Legacy (compat): description, tags

Per-platform columns (example: Shutterstock):
SS_enabled, SS_status, SS_title, SS_description, SS_tags, SS_asset_id, SS_last_submitted, SS_url
(Similar pattern for other platforms: AS_*, IS_*, AL_*, etc.)

Platforms sheet (optional, but supported by adapter):
code | name | max_keywords | description_max_chars | title_required | notes
The SS adapter reads the SS row (defaults: 50 keywords, ~200 chars).

Statuses (quick reference)

Global (status_global): NEW → ANALYZED → DESCRIBED → READY_TO_SUBMIT (if you use it later)

DUPLICATE, ERROR, ARCHIVED as needed

Shutterstock (SS_status):
NOT_ENABLED, PENDING_PREP, READY_TO_UPLOAD, SUBMITTED, UNDER_REVIEW, PUBLISHED, REJECTED, NEEDS_RELEASE, ERROR, ON_HOLD

Logging & locking

Logs:

Scanner: logs/update.log

SS adapter: logs/adapt_ss.log

Locks:

Scanner: .update.lock

SS adapter: .adapt_ss.lock
(Locks prevent concurrent runs; they auto-clear on exit. Stale locks are overridden after ~2 hours.)

Cost & performance tips

Previews are 768px JPEG by default → cheaper & faster vision calls

--describe-limit N lets you batch to control spend

The SS adapter is text-only (no image), so it’s very cheap

Avoid re-describing images: the scanner uses sha256 and last_seen to keep things tidy

Troubleshooting

No Excel file?

Create one with our setup command (or re-run the earlier initializer you used).

Missing API key?

Put it in config.json or export OPENAI_API_KEY.

Still in review / rejected?

Update SS_status accordingly and add a note; iterate fields and re-run make adapt-ss with FORCE=1 if needed.

Stale lock?

If the process crashed, the next run will auto-override stale locks after 2 hours. Remove manually if needed.

Roadmap (optional next steps)

Add adapters for Adobe/iStock/Alamy, using the same pattern as SS (AS_*, IS_*, AL_*)

Duplicate detection via phash

Model/property release tracking

CLI “upload markers” to flip status to SUBMITTED with a timestamp automatically


