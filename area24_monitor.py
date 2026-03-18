#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monitors https://aim.eans.ee/web/notampib/area24.json for changes,
but ONLY compares the subsection: dynamicData.notams

Enhancements:
- Builds NOTAM AFTN text from provided JSON fields.
- On removals, classifies as:
  * EXPIRED: "NOTAM A####/YY expired and was removed from the PIB."
  * REPLACED: "NOTAM A####/YY was replaced with NOTAM B####/YY." + full AFTN of the replacing NOTAM
  * OTHER REMOVALS: fallback message if cause unknown
- On additions, prints full AFTN of new NOTAMs.
- Sends email notifications when changes are detected (if SMTP env vars are set).

Author: Mikk Maasik — with Copilot assist
"""

import json
import os
import sys
import re
import hashlib
import tempfile
import argparse
import smtplib
from email.message import EmailMessage
import urllib.request
from urllib.error import URLError, HTTPError
from datetime import datetime, timezone
from typing import Any, List, Dict, Tuple, Optional

# -------------------------
# Configuration
# -------------------------
URL = "https://aim.eans.ee/web/notampib/area24.json"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "area24_cache")
LAST_JSON_PATH = os.path.join(DATA_DIR, "area24_last.json")
DIFF_LOG_PATH = os.path.join(DATA_DIR, "area24_changes.log")
TIMEOUT_SECS = 30

# Regex to detect "REPLACES A1234/25" patterns in E-line text (case-insensitive).
RE_REPLACES = re.compile(r"\bREPLACES\s+([A-Z]\d{4}/\d{2})\b", re.IGNORECASE)

# -------------------------
# Utilities
# -------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)


def http_get_json(url: str, timeout: int = TIMEOUT_SECS) -> Tuple[dict, str]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "area24-monitor/1.3 (+local)",
            "Accept": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        raw = resp.read().decode(charset, errors="replace")
        data = json.loads(raw)
        return data, raw


def stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2)


def file_write_atomic(path: str, content: str):
    dirpath = os.path.dirname(path)
    os.makedirs(dirpath, exist_ok=True)
    fd, tmppath = tempfile.mkstemp(prefix=".tmp_", dir=dirpath)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmppath, path)
    finally:
        try:
            if os.path.exists(tmppath):
                os.remove(tmppath)
        except Exception:
            pass


def read_json_if_exists(path: str) -> Tuple[Any, str]:
    if not os.path.exists(path):
        return None, ""
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    try:
        obj = json.loads(text)
    except Exception:
        obj = None
    return obj, text


def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def append_log(line: str):
    try:
        with open(DIFF_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line.rstrip() + "\n")
    except Exception:
        pass


def send_email(subject: str, body: str):
    """
    Send an email using SMTP settings from environment variables.

    Required env vars:
    SMTP_HOST, SMTP_USER, SMTP_PASS, EMAIL_FROM, EMAIL_TO

    Optional:
    SMTP_PORT (default 587)
    """
    host = os.environ.get("SMTP_HOST")
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER")
    password = os.environ.get("SMTP_PASS")
    email_from = os.environ.get("EMAIL_FROM")
    email_to = os.environ.get("EMAIL_TO")

    if not all([host, user, password, email_from, email_to]):
        raise RuntimeError("Missing SMTP settings in environment variables")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = email_to
    msg.set_content(body)

    with smtplib.SMTP(host, port, timeout=30) as server:
        server.starttls()
        server.login(user, password)
        server.send_message(msg)


# -------------------------
# NOTAM helpers (domain-specific)
# -------------------------

def get_notams(obj: Any) -> List[dict]:
    """
    Extract dynamicData.notams as a list of dicts.
    """
    if not isinstance(obj, dict):
        return []
    dd = obj.get("dynamicData")
    if not isinstance(dd, dict):
        return []
    lst = dd.get("notams")
    return lst if isinstance(lst, list) else []


def notam_key(n: dict) -> Optional[str]:
    """
    Build 'A####/YY' from notamId {series, number, year}.
    """
    nid = n.get("notamId") if isinstance(n, dict) else None
    if not isinstance(nid, dict):
        return None
    series = str(nid.get("series", "")).strip().upper()
    number = nid.get("number")
    year = nid.get("year")
    if not series or not isinstance(number, int) or not isinstance(year, int):
        return None
    return f"{series}{number:04d}/{year % 100:02d}"


def parse_iso_utc(ts: Optional[str]) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts).astimezone(timezone.utc)
    except Exception:
        return None


def format_b_or_c(ts: Optional[str]) -> Optional[str]:
    """
    Format validity time to YYMMDDhhmm (UTC) as for B) and C) lines.
    """
    dt = parse_iso_utc(ts)
    if not dt:
        return None
    return dt.strftime("%y%m%d%H%M")


# NEW: helper to format PIB 'generated' timestamp for email subject

def pib_generated_utc_string(cur_obj: dict) -> str:
    """
    Returns PIB generated time as 'YYYY-MM-DD HH:MMZ'.
    Expects cur_obj['generated'] in ISO-8601 (e.g., '2026-03-18T12:45:00Z').
    Falls back to current UTC if not available/parseable.
    """
    gen = cur_obj.get("generated")
    try:
        if isinstance(gen, str):
            iso = gen
            if iso.endswith("Z"):
                iso = iso[:-1] + "+00:00"
            dt = datetime.fromisoformat(iso).astimezone(timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%MZ")
    except Exception:
        pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%MZ")


def build_q_line(n: dict) -> Optional[str]:
    """
    Q) FIR/SUBJCOND/TRAFFIC/PURPOSE/SCOPE/LLL/UUU/LATLONGRRR
    lower/upper from qualifiers.lower/upper (hundreds of feet).
    """
    q = n.get("qualifiers", {})
    if not isinstance(q, dict):
        return None
    fir = q.get("fir") or (n.get("locations") or [None])[0]
    subject = q.get("subject")
    condition = q.get("condition")
    traffic = q.get("traffic")
    purpose = q.get("purpose")
    scope = q.get("scope")
    lower = q.get("lower")
    upper = q.get("upper")
    coord = q.get("coordinate")
    radius = q.get("radius")
    if not (fir and subject and condition and traffic and purpose and scope and
            isinstance(lower, (int, float)) and isinstance(upper, (int, float)) and
            isinstance(coord, str) and isinstance(radius, (int, float))):
        return None
    lll = int(lower)
    uuu = int(upper)
    rrr = int(radius)
    return f"{fir}/{subject}{condition}/{traffic}/{purpose}/{scope}/{lll:03d}/{uuu:03d}/{coord}{rrr:03d}"


def build_aftn(n: dict, replaced_ref: Optional[str] = None) -> str:
    """
    Compose an AFTN NOTAM text from the JSON object.
    Lines: (A####/YY NOTAMX [replaced_ref]) Q) A) B) C) D) E) F) G) )
    """
    key = notam_key(n) or "UNKNOWN"
    typ = (n.get("type") or "N").upper()
    notamx = {"N": "NOTAMN", "R": "NOTAMR", "C": "NOTAMC"}.get(typ, f"NOTAM{typ}")

    # Q) line
    qline = build_q_line(n) or ""

    # A) B) C)
    locs = n.get("locations") if isinstance(n.get("locations"), list) else []
    a_loc = locs[0] if locs else (n.get("qualifiers", {}).get("fir") or "")
    b_val = format_b_or_c(n.get("validity", {}).get("start"))
    c_val = format_b_or_c(n.get("validity", {}).get("end"))

    # D)
    d_line = n.get("schedule")

    # E)
    e_text = n.get("text", "")
    e_text = re.sub(r"\r\n?", "\n", e_text).strip()

    # F)/G)
    levels = n.get("levels", {}) if isinstance(n.get("levels"), dict) else {}
    f_val = levels.get("lower")
    g_val = levels.get("upper")

    # Build lines
    lines: List[str] = []
    header = f"({key} {notamx}"
    if typ == "R" and replaced_ref:
        header += f" {replaced_ref}"
    lines.append(header)

    if qline:
        lines.append(f"Q){qline}")

    abc_parts = []
    if a_loc:
        abc_parts.append(f"A){a_loc}")
    if b_val:
        abc_parts.append(f"B){b_val}")
    if c_val:
        abc_parts.append(f"C){c_val}")
    if abc_parts:
        lines.append(" ".join(abc_parts))

    if d_line:
        lines.append(f"D){d_line}")

    if e_text:
        e_lines = e_text.split("\n")
        lines.append("E)" + (e_lines[0] if e_lines else ""))
        for cont in e_lines[1:]:
            lines.append(cont)
    else:
        lines.append("E)")

    if f_val:
        if g_val:
            lines.append(f"F){f_val} G){g_val}")
        else:
            lines.append(f"F){f_val}")
    elif g_val:
        lines.append(f"G){g_val}")

    lines.append(")")
    return "\n".join(lines)


def detect_replaced_target(n: dict) -> Optional[str]:
    """
    Determine which NOTAM this NOTAM replaces.
    - Look for common fields or nested notamId.
    - Fallback: regex in E-line text (REPLACES A1234/25).
    Returns 'A####/YY' or None.
    """
    for k in ["replaces", "replaced", "replacedNotam", "replacing", "replace", "parentNotam", "prevNotamId"]:
        v = n.get(k)
        if isinstance(v, str):
            m = re.search(r"[A-Z]\d{4}/\d{2}", v.upper())
            if m:
                return m.group(0)
    for k in ["replaces", "replaced", "replacedNotam", "parentNotam", "previous"]:
        v = n.get(k)
        if isinstance(v, dict):
            s = v.get("series"); num = v.get("number"); yr = v.get("year")
            if isinstance(s, str) and isinstance(num, int) and isinstance(yr, int):
                return f"{s.upper()}{num:04d}/{yr % 100:02d}"
            nid = v.get("notamId")
            if isinstance(nid, dict):
                s = nid.get("series"); num = nid.get("number"); yr = nid.get("year")
                if isinstance(s, str) and isinstance(num, int) and isinstance(yr, int):
                    return f"{s.upper()}{num:04d}/{yr % 100:02d}"
    txt = n.get("text") or ""
    m = RE_REPLACES.search(txt)
    if m:
        return m.group(1).upper()
    return None


def notam_end_time(n: dict) -> Optional[datetime]:
    v = n.get("validity", {})
    return parse_iso_utc(v.get("end"))


# -------------------------
# Main flow
# -------------------------

def main():
    parser = argparse.ArgumentParser(description="Monitor area24 NOTAMs (local test; no email).")
    parser.add_argument("--suppress-modified", action="store_true",
                        help="Do not include 'modified' notifications; only new/replaced/expired/removed.")
    args = parser.parse_args()

    ensure_dirs()
    ts = utc_now_iso()

    # Load previous full JSON
    prev_obj, _ = read_json_if_exists(LAST_JSON_PATH)

    # Fetch current full JSON
    try:
        cur_obj, _ = http_get_json(URL)
    except HTTPError as e:
        msg = f"[{ts}] ERROR: HTTP error {e.code} when fetching {URL}: {e.reason}"
        print(msg, file=sys.stderr); append_log(msg); sys.exit(2)
    except URLError as e:
        msg = f"[{ts}] ERROR: Network error when fetching {URL}: {e.reason}"
        print(msg, file=sys.stderr); append_log(msg); sys.exit(2)
    except json.JSONDecodeError as e:
        msg = f"[{ts}] ERROR: Invalid JSON from {URL}: {e}"
        print(msg, file=sys.stderr); append_log(msg); sys.exit(2)
    except Exception as e:
        msg = f"[{ts}] ERROR: Unexpected error: {repr(e)}"
        print(msg, file=sys.stderr); append_log(msg); sys.exit(2)

    # Extract NOTAM arrays
    prev_notams = get_notams(prev_obj) if prev_obj is not None else []
    cur_notams = get_notams(cur_obj)

    # Map by NOTAM key 'A####/YY'
    prev_map: Dict[str, dict] = {}
    for n in prev_notams:
        k = notam_key(n)
        if k:
            prev_map[k] = n

    cur_map: Dict[str, dict] = {}
    for n in cur_notams:
        k = notam_key(n)
        if k:
            cur_map[k] = n

    prev_keys = set(prev_map.keys())
    cur_keys = set(cur_map.keys())

    added_keys = cur_keys - prev_keys
    removed_keys = prev_keys - cur_keys
    common_keys = prev_keys & cur_keys

    now_utc = datetime.now(timezone.utc)

    # Build helper: replacement index (new NOTAMs of type R)
    replacements: Dict[str, str] = {}  # replaced_key -> replacer_key
    replacer_aftn: Dict[str, str] = {}  # replacer_key -> AFTN text

    for k in sorted(added_keys):
        n = cur_map[k]
        if str(n.get("type", "")).upper() == "R":
            target = detect_replaced_target(n)
            if target and target in prev_map:
                replacements[target] = k
                replacer_aftn[k] = build_aftn(n, replaced_ref=target)
            else:
                replacer_aftn[k] = build_aftn(n)

    expired_msgs: List[str] = []
    replaced_msgs: List[str] = []
    new_msgs: List[str] = []
    other_removed_msgs: List[str] = []
    modified_msgs: List[str] = []

    # Handle removals (expired / replaced / other)
    for k in sorted(removed_keys):
        old_n = prev_map[k]
        # Replaced?
        if k in replacements:
            repl_key = replacements[k]
            replaced_msgs.append(f"NOTAM {k} was replaced with NOTAM {repl_key}.\n\n{replacer_aftn.get(repl_key, '')}\n")
            continue
        # Expired?
        end = notam_end_time(old_n)
        if end and end < now_utc:
            expired_msgs.append(f"NOTAM {k} expired and was removed from the PIB.")
            continue
        # Other removal
        other_removed_msgs.append(f"NOTAM {k} was removed from the PIB.")

    # Handle additions (list AFTN). Skip replacers already printed above.
    for k in sorted(added_keys):
        if k in replacer_aftn and any(r == k for r in replacements.values()):
            continue
        n = cur_map[k]
        aftn = build_aftn(n)
        new_msgs.append(f"New NOTAM added:\n{aftn}\n")

    # Optional: detect modifications (brief)
    if not args.suppress_modified:
        for k in sorted(common_keys):
            prev_canon = stable_json(prev_map[k])
            cur_canon = stable_json(cur_map[k])
            if hash_text(prev_canon) != hash_text(cur_canon):
                modified_msgs.append(f"NOTAM {k} was modified.")

    # If nothing changed, log and exit
    if not (expired_msgs or replaced_msgs or new_msgs or other_removed_msgs or modified_msgs):
        msg = f"[{ts}] NO CHANGE in dynamicData.notams."
        print(msg); append_log(msg)
        # Update stored JSON to keep freshest copy
        file_write_atomic(LAST_JSON_PATH, stable_json(cur_obj))
        sys.exit(0)

    # Build console/log body (grouped)
    sections: List[str] = [f"[{ts}] CHANGE DETECTED in dynamicData.notams", f"URL: {URL}"]

    if expired_msgs:
        sections.append("\nExpired NOTAMs:")
        sections.extend(f"- {m}" for m in expired_msgs)

    if replaced_msgs:
        sections.append("\nReplaced NOTAMs:")
        sections.extend(replaced_msgs)

    if new_msgs:
        sections.append("\nNew NOTAMs:")
        sections.extend(new_msgs)

    if other_removed_msgs:
        sections.append("\nOther removals:")
        sections.extend(f"- {m}" for m in other_removed_msgs)

    if modified_msgs:
        sections.append("\nModified NOTAMs:")
        sections.extend(f"- {m}" for m in modified_msgs)

    body = "\n".join(sections)
    print(body)
    append_log(body)

    # --- SUBJECT CHANGE HERE ---
    try:
        pib_ts = pib_generated_utc_string(cur_obj)
        subject = f"AREA PIB: {pib_ts} muutunud NOTAMid"
        send_email(subject, body)
    except Exception as e:
        err = f"[{ts}] ERROR: Email sending failed: {repr(e)}"
        print(err, file=sys.stderr)
        append_log(err)

    # Store latest full JSON (discard previous)
    file_write_atomic(LAST_JSON_PATH, stable_json(cur_obj))


if __name__ == "__main__":
    main()
