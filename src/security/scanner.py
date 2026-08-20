"""Runtime security checks for incoming data (Security Layer G, runtime half).

Two distinct threats are handled here:

1. MALICIOUS FILE CONTENT - a file that arrives from outside could carry
   malware. `scan_file_for_malware` shells out to ClamAV when it is available.

2. MALICIOUS STRUCTURED CONTENT - far more relevant to a data platform. A JSON
   payload from a compromised API can carry SQL injection, script injection or
   path traversal inside ordinary-looking string fields. Those strings later
   reach SQL queries, dashboards and an LLM agent, so they must be inspected
   before they are trusted.

Everything degrades gracefully: if a scanner is missing, the function says so
instead of silently reporting "clean", because a silent false "clean" is worse
than no scan at all.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess  # nosec B404 - used only with a fixed, non-shell argument list
from dataclasses import dataclass, field
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Injection signatures. These are intentionally conservative: they look for
# patterns that have no legitimate reason to appear inside energy market data.
# ---------------------------------------------------------------------------
SUSPICIOUS_PATTERNS: list[tuple[str, str]] = [
    (r"(?i)\b(union\s+select|drop\s+table|truncate\s+table)\b", "SQL injection keyword"),
    (r"(?i);\s*(drop|delete|update|insert)\s+", "chained SQL statement"),
    (r"(?i)--\s*$", "SQL comment terminator"),
    (r"(?i)<script[^>]*>", "HTML/JS script tag"),
    (r"(?i)javascript:", "javascript: URI"),
    (r"(?i)\bon(error|load|click)\s*=", "inline event handler"),
    (r"\.\./|\.\.\\", "path traversal sequence"),
    (r"(?i)\$\{.*\}", "template/expression injection"),
    (r"(?i)\b(eval|exec|__import__)\s*\(", "code execution call"),
    # Prompt injection: this data is later shown to an AI agent, so an
    # instruction hidden in a data field is a genuine attack vector.
    (r"(?i)ignore\s+(all\s+)?(previous|prior|above)\s+instructions", "prompt injection"),
    (r"(?i)\b(system\s+prompt|you\s+are\s+now)\b", "prompt injection"),
]

# Fields that should only ever contain simple identifiers.
IDENTIFIER_FIELDS = {"trade_id", "trader_id", "counterparty", "commodity", "buy_sell"}
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_\-. ]{1,64}$")


@dataclass
class ScanResult:
    """Outcome of a security scan."""

    target: str
    clean: bool
    scanner_available: bool = True
    threats: list[str] = field(default_factory=list)
    details: dict = field(default_factory=dict)

    def summary(self) -> str:
        if not self.scanner_available:
            return f"[SKIP] {self.target}: scanner unavailable, NOT verified"
        status = "CLEAN" if self.clean else "THREAT"
        return f"[{status}] {self.target}: {len(self.threats)} issue(s)"


def sha256_file(path: str) -> str:
    """Checksum for data provenance: proves a batch was not altered later."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def scan_file_for_malware(path: str) -> ScanResult:
    """Scan a file with ClamAV, if the clamscan binary is installed."""
    if not os.path.exists(path):
        return ScanResult(path, clean=False, threats=[f"file not found: {path}"])

    binary = shutil.which("clamscan")
    if binary is None:
        return ScanResult(
            path,
            clean=True,
            scanner_available=False,
            details={"note": "clamscan not installed; install clamav to enable this check"},
        )

    try:
        # No shell is used and the argument list is fixed, so the input path
        # cannot be turned into a shell command.
        proc = subprocess.run(  # nosec B603
            [binary, "--no-summary", path],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return ScanResult(path, clean=False, threats=["clamscan timed out"])

    # clamscan: 0 = clean, 1 = virus found, 2 = error.
    if proc.returncode == 0:
        return ScanResult(path, clean=True)
    if proc.returncode == 1:
        return ScanResult(path, clean=False, threats=[proc.stdout.strip() or "malware detected"])
    return ScanResult(
        path, clean=False, scanner_available=False,
        threats=[f"clamscan error: {proc.stderr.strip()}"],
    )


def scan_text(value: str, location: str = "") -> list[str]:
    """Return the list of suspicious patterns found in a single string."""
    found = []
    for pattern, label in SUSPICIOUS_PATTERNS:
        if re.search(pattern, value):
            snippet = value[:80].replace("\n", " ")
            found.append(f"{label} at {location or 'value'}: '{snippet}'")
    return found


def scan_payload(payload, path: str = "$", max_depth: int = 20) -> ScanResult:
    """Walk a nested JSON-like structure and inspect every string it contains."""
    threats: list[str] = []
    strings_checked = 0

    def walk(node, location: str, depth: int) -> None:
        nonlocal strings_checked
        if depth > max_depth:
            threats.append(f"structure deeper than {max_depth} levels at {location} "
                           f"(possible denial-of-service payload)")
            return
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(key, str):
                    strings_checked += 1
                    threats.extend(scan_text(key, f"{location}.<key>"))
                walk(value, f"{location}.{key}", depth + 1)
        elif isinstance(node, (list, tuple)):
            for i, value in enumerate(node):
                walk(value, f"{location}[{i}]", depth + 1)
        elif isinstance(node, str):
            strings_checked += 1
            threats.extend(scan_text(node, location))

    walk(payload, path, 0)
    return ScanResult(
        target=path,
        clean=not threats,
        threats=threats,
        details={"strings_checked": strings_checked},
    )


def scan_json_file(path: str) -> ScanResult:
    """Full inspection of a Bronze JSON file: malware, then content injection."""
    malware = scan_file_for_malware(path)
    if not malware.clean and malware.scanner_available:
        return malware

    try:
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
    except json.JSONDecodeError as exc:
        return ScanResult(path, clean=False, threats=[f"malformed JSON: {exc}"])

    result = scan_payload(payload, path=os.path.basename(path))
    result.details["sha256"] = sha256_file(path)
    result.details["malware_scanner_available"] = malware.scanner_available
    result.details["scanned_at"] = datetime.now(timezone.utc).isoformat()
    return result


def _is_texty(series) -> bool:
    """True if a column can hold text.

    Do NOT test `dtype == object`. pandas 2/3 store text as StringDtype, so an
    object-only check silently skips every string column and the scanner would
    report "clean" on poisoned data - a false clean, the worst possible outcome
    for a security control.
    """
    import pandas as pd

    dtype = series.dtype
    if pd.api.types.is_string_dtype(dtype) or dtype == object:
        return True
    # Categorical columns of strings must be inspected too.
    return isinstance(dtype, pd.CategoricalDtype)


def scan_dataframe(df, columns: set | None = None) -> ScanResult:
    """Inspect the string columns of a DataFrame before they reach SQL or an LLM."""
    threats: list[str] = []
    columns = columns or IDENTIFIER_FIELDS
    checked = 0
    inspected_columns: list[str] = []

    for col in df.columns:
        if not _is_texty(df[col]):
            continue
        inspected_columns.append(str(col))
        values = df[col].dropna().astype(str)
        for idx, value in values.items():
            checked += 1
            threats.extend(scan_text(value, f"row {idx}, column '{col}'"))
            # Identifier fields must match a strict whitelist. Anything else is
            # unexpected structure, which is how injection usually arrives.
            if col in columns and not IDENTIFIER_PATTERN.match(value):
                threats.append(
                    f"unexpected characters in identifier column '{col}' at row {idx}: '{value[:40]}'"
                )

    return ScanResult(
        target="dataframe",
        clean=not threats,
        threats=threats,
        details={
            "values_checked": checked,
            "columns_inspected": inspected_columns,
        },
    )


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m security.scanner <file.json>")
        raise SystemExit(1)

    result = scan_json_file(sys.argv[1])
    print(result.summary())
    for threat in result.threats:
        print("  ->", threat)
    print("  details:", json.dumps(result.details, indent=2))
    raise SystemExit(0 if result.clean else 2)
