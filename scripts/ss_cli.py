#!/usr/bin/env python3
"""
Shutterstock Contributor CLI

Commands:
  - ss:login    → interactive login that persists session cookies/state
  - ss:quota    → show uploads used in the last 7 days (rolling, cap=500)
  - ss:status   → list recent submissions (read-only snapshot)

Credentials priority:
1) env: SS_USER / SS_PASS
2) config.json → SHUTTERSTOCK.USERNAME / SHUTTERSTOCK.PASSWORD
3) OS keyring (service 'ssku')

Prereqs:
  pip install -r requirements.txt
  playwright install

Notes:
- Timezone is Europe/Madrid for rolling window calculations.
- This is a conservative scraper: Shutterstock may change DOM/XHR; if parsing fails,
  commands will still exit gracefully with partial/no data rather than crash.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

import pytz
from rich.console import Console
from rich.table import Table
from rich import box

try:
    import keyring  # type: ignore
except Exception:
    keyring = None

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ────────────────────────────────────────────────────────────────────────────────
# Constants & Paths
# ────────────────────────────────────────────────────────────────────────────────
EUROPE_MADRID = pytz.timezone("Europe/Madrid")
DEFAULT_QUOTA = 500  # images per 7 days
ROLLING_DAYS = 7

HOME = Path.home()
SESSION_DIR = HOME / ".ssku" / "session"
SESSION_DIR.mkdir(parents=True, exist_ok=True)
STORAGE_JSON = SESSION_DIR / "storage.json"

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
LEDGER_PATH = DATA_DIR / "ss_quota_ledger.jsonl"  # will be used once uploads exist

# Config: look for ./config.json by default; allow override via ENV
DEFAULT_CONFIG_PATH = Path(os.getenv("SSKU_CONFIG", "config.json"))

# Known pages
CONTRIB_ROOT = "https://submit.shutterstock.com/"
PORTFOLIO_URL = "https://submit.shutterstock.com/dashboard"
LOGIN_URL = "https://accounts.shutterstock.com/login"

console = Console()


# ────────────────────────────────────────────────────────────────────────────────
# Utilities
# ────────────────────────────────────────────────────────────────────────────────
def load_config(path: Optional[Path] = None) -> Dict[str, Any]:
    cfg_path = path or DEFAULT_CONFIG_PATH
    if cfg_path.exists():
        try:
            with cfg_path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def now_madrid() -> datetime:
    return datetime.now(EUROPE_MADRID)


def parse_iso(dt: str) -> datetime:
    try:
        # Accept both Zulu and offset forms
        return datetime.fromisoformat(dt.replace("Z", "+00:00")).astimezone(EUROPE_MADRID)
    except Exception:
        return now_madrid()


def load_storage_state() -> Optional[str]:
    if STORAGE_JSON.exists():
        return str(STORAGE_JSON)
    return None


def save_storage_state(context) -> None:
    context.storage_state(path=str(STORAGE_JSON))


def read_credentials() -> tuple[Optional[str], Optional[str]]:
    # Priority: ENV > config.json > keyring
    user = os.getenv("SS_USER")
    pwd = os.getenv("SS_PASS")

    if not user or not pwd:
        cfg = load_config()
        ss_cfg = cfg.get("SHUTTERSTOCK") or cfg.get("shutterstock") or {}
        user = user or ss_cfg.get("USERNAME") or ss_cfg.get("username")
        pwd = pwd or ss_cfg.get("PASSWORD") or ss_cfg.get("password")

    if (not user or not pwd) and keyring is not None:
        service = "ssku"
        if not user:
            user = keyring.get_password(service, "username")  # may be None
        if user and not pwd:
            pwd = keyring.get_password(service, user)

    return user, pwd


# ────────────────────────────────────────────────────────────────────────────────
# Ledger helpers (used once uploads are implemented)
# ────────────────────────────────────────────────────────────────────────────────
def ledger_entries_within(days: int = ROLLING_DAYS) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not LEDGER_PATH.exists():
        return out
    cutoff = now_madrid() - timedelta(days=days)
    with LEDGER_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                ts_raw = obj.get("timestamp", "")
                ts = parse_iso(str(ts_raw)) if ts_raw else None
                if ts and ts >= cutoff:
                    out.append(obj)
            except Exception:
                continue
    return out


def tally_ledger(days: int = ROLLING_DAYS) -> int:
    return len(ledger_entries_within(days))


# ────────────────────────────────────────────────────────────────────────────────
# Portal snapshot helpers
# ────────────────────────────────────────────────────────────────────────────────
@dataclass
class PortalSnapshot:
    count_last_7d: int
    sample: List[Dict[str, Any]]


def snapshot_portal_uploads(verbose: bool = False) -> PortalSnapshot:
    """
    Best-effort count of items visible on the dashboard that look like
    submissions within the last 7 days.
    """
    storage = load_storage_state()
    result = PortalSnapshot(count_last_7d=0, sample=[])
    if storage is None:
        return result

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(storage_state=storage)
        page = context.new_page()
        try:
            page.goto(PORTFOLIO_URL, wait_until="domcontentloaded", timeout=30000)
            # Try timestamps via <time datetime="...">
            timestamps: List[datetime] = []
            for sel in ("time[datetime]", "[data-testid*=item] time[datetime]", "[data-ss-test=date]"):
                for el in page.query_selector_all(sel):
                    dt_attr = el.get_attribute("datetime")
                    if dt_attr:
                        timestamps.append(parse_iso(dt_attr))
            # Fallback: simple text scrape to catch ISO-like tokens
            if not timestamps:
                text_content = page.content()
                for token in text_content.split():
                    if token.startswith("202"):
                        try:
                            timestamps.append(parse_iso(token.strip('"\'')))
                        except Exception:
                            pass
            cutoff = now_madrid() - timedelta(days=ROLLING_DAYS)
            recent = [t for t in timestamps if t >= cutoff]
            result.count_last_7d = len(recent)
            result.sample = [{"timestamp": t.isoformat()} for t in sorted(recent, reverse=True)[:5]]
        except PWTimeoutError:
            pass
        except Exception:
            pass
        finally:
            context.storage_state(path=str(STORAGE_JSON))  # refresh cookies
            browser.close()
    return result


# ────────────────────────────────────────────────────────────────────────────────
# Submissions listing (read-only)
# ────────────────────────────────────────────────────────────────────────────────
@dataclass
class SubmissionItem:
    asset_id: str
    title: str
    status: str
    submitted_at: Optional[datetime] = None
    reviewed_at: Optional[datetime] = None
    published_at: Optional[datetime] = None
    reason: Optional[str] = None
    url: Optional[str] = None


def list_recent_submissions(limit: int = 50, days: int = 30) -> List[SubmissionItem]:
    """
    Best-effort scrape of recent submissions from the contributor dashboard.

    Implementation notes:
    - Uses existing session. If not logged in, returns [].
    - Strategy A: parse dashboard DOM (cards/rows).
    - Strategy B: listen to JSON XHR payloads and extract items.
    - Returns up to `limit` items within the last `days`.
    """
    storage = load_storage_state()
    if storage is None:
        return []

    items: List[SubmissionItem] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(storage_state=storage)
        page = context.new_page()

        # Collect JSON responses for Strategy B
        captured: List[Dict[str, Any]] = []

        def handle_response(resp):
            try:
                ct = resp.headers.get("content-type", "")
                url = resp.url
                if "application/json" in ct and (
                    "submit" in url or "dashboard" in url or "content" in url or "contributor" in url
                ):
                    data = resp.json()
                    captured.append({"url": url, "data": data})
            except Exception:
                pass

        page.on("response", handle_response)

        try:
            page.goto(PORTFOLIO_URL, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(2000)

            # Strategy A: parse DOM for obvious cards/rows
            candidate_rows = page.query_selector_all('[data-testid*="item"], [role="row"], article, li')
            for el in candidate_rows[:500]:
                try:
                    status = (el.get_attribute("data-status") or el.get_attribute("aria-label") or "").strip()
                    title_el = (el.query_selector("[data-testid=title]") or el.query_selector("h3, h4, .title"))
                    title = title_el.inner_text().strip() if title_el else ""
                    link_el = el.query_selector("a[href]")
                    href = link_el.get_attribute("href") if link_el else None
                    time_el = el.query_selector("time[datetime]")
                    submitted = parse_iso(time_el.get_attribute("datetime")) if time_el else None
                    asset_id = href.split("/")[-1] if href else ""
                    if title or asset_id:
                        items.append(
                            SubmissionItem(
                                asset_id=asset_id or "",
                                title=title or "",
                                status=status or "",
                                submitted_at=submitted,
                                url=(href if href and href.startswith("http") else ("https://submit.shutterstock.com" + href) if href else None),
                            )
                        )
                except Exception:
                    continue

            # Strategy B: parse captured JSON payloads
            cutoff = now_madrid() - timedelta(days=days)

            def maybe_add_from_json(obj: Dict[str, Any]):
                try:
                    aid = str(obj.get("id") or obj.get("asset_id") or obj.get("media_id") or "")
                    title = str(obj.get("title") or obj.get("name") or "")
                    status = str(obj.get("status") or obj.get("review_status") or "")
                    ts_fields = ["submitted_at", "created_at", "created", "submit_time", "uploaded_at"]
                    submitted = None
                    for k in ts_fields:
                        if k in obj and obj[k]:
                            submitted = parse_iso(str(obj[k]))
                            break
                    reviewed = parse_iso(str(obj.get("reviewed_at"))) if obj.get("reviewed_at") else None
                    published = parse_iso(str(obj.get("published_at"))) if obj.get("published_at") else None
                    reason = obj.get("rejection_reason") or obj.get("reason")
                    if submitted and submitted < cutoff:
                        return
                    if aid or title or status:
                        items.append(
                            SubmissionItem(
                                asset_id=aid,
                                title=title,
                                status=status,
                                submitted_at=submitted,
                                reviewed_at=reviewed,
                                published_at=published,
                                reason=str(reason) if reason else None,
                            )
                        )
                except Exception:
                    return

            for cap in captured:
                data = cap.get("data")
                if isinstance(data, dict):
                    for v in data.values():
                        if isinstance(v, list):
                            for obj in v:
                                if isinstance(obj, dict):
                                    maybe_add_from_json(obj)
                if isinstance(data, list):
                    for obj in data:
                        if isinstance(obj, dict):
                            maybe_add_from_json(obj)

        except Exception:
            pass
        finally:
            context.storage_state(path=str(STORAGE_JSON))
            browser.close()

    # De-duplicate by asset_id + title
    dedup: Dict[str, SubmissionItem] = {}
    for it in items:
        key = (it.asset_id or it.title or "").strip()
        if key and key not in dedup:
            dedup[key] = it
    out = list(dedup.values())

    # Sort by submitted_at desc, fallback to title
    out.sort(key=lambda x: x.submitted_at or datetime.min.replace(tzinfo=EUROPE_MADRID), reverse=True)
    return out[:limit]


# ────────────────────────────────────────────────────────────────────────────────
# Commands
# ────────────────────────────────────────────────────────────────────────────────
def cmd_login(args: argparse.Namespace) -> int:
    # Try credential-assisted login (optional), else let user login manually
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False, slow_mo=50)
        context = browser.new_context(storage_state=load_storage_state())
        page = context.new_page()
        console.rule("[bold]Shutterstock Contributor Login")
        page.goto(CONTRIB_ROOT, wait_until="domcontentloaded")

        # If already authenticated, a contributor dashboard element should exist
        try:
            page.wait_for_selector("a[href*='dashboard']", timeout=4000)
            console.print("[green]Already logged in. Session refreshed.")
            save_storage_state(context)
            browser.close()
            return 0
        except PWTimeoutError:
            pass

        # Navigate to the login page
        page.goto(LOGIN_URL, wait_until="domcontentloaded")

        user, pwd = read_credentials()
        if user and pwd:
            try:
                page.fill("input[type=email], input[name=email]", user)
                page.fill("input[type=password]", pwd)
                page.click("button[type=submit], button[data-testid='login-submit']")
            except Exception:
                pass

        console.print("If prompted, complete any MFA challenge in the opened window…")
        try:
            page.wait_for_url(lambda url: "submit.shutterstock.com" in url or "account" not in url, timeout=180000)
        except PWTimeoutError:
            console.print("[red]Login did not complete within 3 minutes.")
            save_storage_state(context)
            browser.close()
            return 2

        # Verify by checking a contributor-only element
        try:
            page.goto(CONTRIB_ROOT, wait_until="domcontentloaded")
            page.wait_for_selector("a[href*='dashboard']", timeout=15000)
            console.print("[green]Login successful. Session stored.")
            save_storage_state(context)
            browser.close()
            return 0
        except PWTimeoutError:
            console.print("[yellow]Unable to verify dashboard element. Session saved anyway.")
            save_storage_state(context)
            browser.close()
            return 1


def cmd_quota(args: argparse.Namespace) -> int:
    ledger_count = tally_ledger(ROLLING_DAYS)
    snap = snapshot_portal_uploads(verbose=args.verbose)
    used = min(ledger_count, snap.count_last_7d) if snap.count_last_7d else ledger_count
    remaining = max(0, DEFAULT_QUOTA - used)

    console.rule("[bold]Shutterstock 7-day Upload Quota")
    table = Table(box=box.SIMPLE_HEAVY)
    table.add_column("Window", justify="left")
    table.add_column("Used", justify="right")
    table.add_column("Remaining", justify="right")
    start = (now_madrid() - timedelta(days=ROLLING_DAYS)).strftime("%Y-%m-%d %H:%M %Z")
    end = now_madrid().strftime("%Y-%m-%d %H:%M %Z")
    table.add_row(f"{start} → {end}", str(used), str(remaining))
    console.print(table)

    if args.verbose:
        vt = Table(title="Details", box=box.MINIMAL_DOUBLE_HEAD)
        vt.add_column("Source")
        vt.add_column("Count")
        vt.add_row("Local ledger (last 7d)", str(ledger_count))
        vt.add_row("Portal snapshot (last 7d)", str(snap.count_last_7d))
        console.print(vt)
        if snap.sample:
            st = Table(title="Portal recent sample", box=box.SIMPLE)
            st.add_column("timestamp (Europe/Madrid)")
            for item in snap.sample:
                st.add_row(item.get("timestamp", ""))
            console.print(st)

    if remaining <= 0:
        console.print("[red]Quota appears exhausted. Delay new uploads to avoid rejection.")
    elif remaining < 25:
        console.print("[yellow]Quota nearly exhausted—consider pausing bulk jobs.")
    else:
        console.print("[green]Quota headroom looks OK.")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    data = list_recent_submissions(limit=args.limit, days=args.days)
    table = Table(title="Shutterstock — Recent Submissions", box=box.SIMPLE_HEAVY)
    table.add_column("ID", justify="left", no_wrap=True)
    table.add_column("Status", justify="left")
    table.add_column("Submitted", justify="left")
    table.add_column("Reviewed", justify="left")
    table.add_column("Published", justify="left")
    table.add_column("Title", justify="left")

    def fmt(dt: Optional[datetime]):
        return dt.astimezone(EUROPE_MADRID).strftime("%Y-%m-%d %H:%M") if dt else ""

    for it in data:
        short_id = (it.asset_id[:10] + ("…" if len(it.asset_id) > 10 else "")) if it.asset_id else ""
        title = (it.title[:60] + "…") if len(it.title) > 60 else it.title
        table.add_row(short_id, it.status or "", fmt(it.submitted_at), fmt(it.reviewed_at), fmt(it.published_at), title)

    console.print(table)

    if args.verbose:
        for it in data:
            if it.reason:
                console.print(f"[yellow]Note[/]: {it.asset_id or it.title}: {it.reason}")
            if it.url:
                console.print(f"[blue]URL[/]: {it.url}")

    return 0


# ────────────────────────────────────────────────────────────────────────────────
# Main CLI
# ────────────────────────────────────────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Shutterstock Contributor CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp_login = sub.add_parser("ss:login", help="Interactive login; persist session")
    sp_login.set_defaults(func=cmd_login)

    sp_quota = sub.add_parser("ss:quota", help="Show last-7d uploads vs 500 limit")
    sp_quota.add_argument("--verbose", action="store_true", help="Show details")
    sp_quota.set_defaults(func=cmd_quota)

    sp_status = sub.add_parser("ss:status", help="List recent submissions")
    sp_status.add_argument("--limit", type=int, default=50, help="Max items to show")
    sp_status.add_argument("--days", type=int, default=30, help="Look back this many days")
    sp_status.add_argument("--verbose", action="store_true", help="Show reasons/URLs when available")
    sp_status.set_defaults(func=cmd_status)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
