#!/usr/bin/env python3
"""
scripts/monitor_bundle_diffs.py

Automated Monitor & Audit Diff Tool for exchange.papertrade.xyz:
1. Fetches index.html from https://exchange.papertrade.xyz/ (with fallback/cross-check to https://papertrade.xyz/)
2. Discovers all referenced JS chunks, CSS files, and build metadata (X-Papertrade-Build, build-*.js)
3. Recursively discovers dynamic imports and secondary chunks
4. Checks for exposed sourcemap (.map) files
5. Scans for API routes, staging/canary/test environments, and query parameters
6. Compares hashes against stored baseline (data/bundle_state.json) and alerts on changes/regressions
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.request
from typing import Any, Dict, List, Set, Tuple

DEFAULT_ORIGIN = "https://exchange.papertrade.xyz"
STATE_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "bundle_state.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def fetch_url(url: str, custom_headers: dict | None = None) -> Tuple[int, dict, bytes]:
    """Fetch URL and return (status_code, headers_dict, body_bytes)."""
    headers = dict(HEADERS)
    if custom_headers:
        headers.update(custom_headers)
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            status = resp.status
            resp_headers = dict(resp.headers)
            body = resp.read()
            return status, resp_headers, body
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()
    except Exception as e:
        print(f"[!] Error fetching {url}: {e}", file=sys.stderr)
        return 0, {}, b""


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def extract_assets_from_html(html_text: str) -> List[str]:
    """Find all script src, modulepreload href, and stylesheet href tags."""
    pattern = r'(?:src|href)=["\'](/assets/[^"\']+)["\']'
    matches = re.findall(pattern, html_text)
    return sorted(list(set(matches)))


def extract_dynamic_chunks_from_js(js_code: str) -> List[str]:
    """Extract secondary dynamic imports like assets/uwu-controls-CLAOVKkq.js."""
    pattern = r'assets/[a-zA-Z0-9_.-]+\.(?:js|css)'
    matches = re.findall(pattern, js_code)
    # Return formatted as /assets/...
    normalized = set()
    for m in matches:
        if not m.startswith("/"):
            m = "/" + m
        normalized.add(m)
    return sorted(list(normalized))


def extract_routes_and_endpoints(js_code: str) -> Set[str]:
    """Extract query, state, intent, and routing endpoints."""
    endpoints = set()
    # Query / state / intents patterns
    for m in re.finditer(r'[`\'"](/query/[a-zA-Z0-9_\-/${}]+|/state/[a-zA-Z0-9_\-/${}]+|/intents[a-zA-Z0-9_\-/${}]*|/telemetry)[`\'"]', js_code):
        endpoints.add(m.group(1))

    # Frontend routes pattern
    for m in re.finditer(r'path:\s*[`"\'](/[^`"\']*)', js_code):
        endpoints.add(m.group(1))
    return endpoints


def extract_hidden_params(js_code: str) -> Set[str]:
    """Extract query param validations or feature flags."""
    params = set()
    for m in re.finditer(r'validateSearch:\s*([a-zA-Z0-9_$]+)', js_code):
        params.add(f"searchSchema:{m.group(1)}")
    # Find specific query parameters
    for m in re.finditer(r'([a-zA-Z0-9_]+):\s*Nn\(|([a-zA-Z0-9_]+):\s*Gn\(|([a-zA-Z0-9_]+):\s*L\(', js_code):
        p = m.group(1) or m.group(2) or m.group(3)
        if p and len(p) > 1 and p not in ["function", "var", "let", "const"]:
            params.add(p)
    return params


def scan_environment(base_url: str = DEFAULT_ORIGIN, check_sourcemaps: bool = True) -> Dict[str, Any]:
    """Perform a complete scan of the frontend bundles, hashes, endpoints, and sourcemap status."""
    print(f"[*] Scanning frontend bundles on {base_url}...")
    status, headers, html_bytes = fetch_url(base_url)
    if status != 200 or not html_bytes:
        print(f"[-] Failed to fetch index page from {base_url} (HTTP {status})")
        return {}

    html_text = html_bytes.decode("utf-8", errors="ignore")
    initial_assets = extract_assets_from_html(html_text)

    # Check for meta tags indicating preview/staging/origins
    origins = {}
    for meta_origin in re.findall(r'<meta name="([^"]+)" content="([^"]+)">', html_text):
        origins[meta_origin[0]] = meta_origin[1]

    # Download all initial assets and discover dynamic chunks
    all_assets: Set[str] = set(initial_assets)
    asset_data: Dict[str, Dict[str, Any]] = {}
    endpoints: Set[str] = set()
    search_params: Set[str] = set()
    build_version = None

    queue = list(initial_assets)
    visited = set()

    while queue:
        asset_path = queue.pop(0)
        if asset_path in visited:
            continue
        visited.add(asset_path)

        full_url = base_url.rstrip("/") + asset_path
        astatus, aheaders, abody = fetch_url(full_url)
        if astatus != 200:
            continue

        h = sha256_hex(abody)
        size = len(abody)
        content_text = abody.decode("utf-8", errors="ignore")

        # Check for build hash chunk
        if "build-" in asset_path and not build_version:
            m = re.search(r'var\s+e=`([0-9a-f]+)`', content_text)
            if m:
                build_version = m.group(1)

        # Discover dynamic chunks
        new_chunks = extract_dynamic_chunks_from_js(content_text)
        for c in new_chunks:
            if c not in visited and c not in queue:
                queue.append(c)
                all_assets.add(c)

        # Discover endpoints & params
        endpoints.update(extract_routes_and_endpoints(content_text))
        search_params.update(extract_hidden_params(content_text))

        asset_data[asset_path] = {
            "sha256": h,
            "size": size,
            "etag": aheaders.get("etag", "").strip('"'),
            "last_modified": aheaders.get("last-modified", ""),
        }

    # Sourcemap vulnerability probe
    sourcemap_status = {}
    if check_sourcemaps:
        print(f"[*] Checking .map sourcemap availability for key bundles...")
        sample_assets = [p for p in initial_assets if p.endswith(".js")][:5]
        for p in sample_assets:
            map_url = base_url.rstrip("/") + p + ".map"
            mstatus, _, _ = fetch_url(map_url)
            sourcemap_status[p + ".map"] = {
                "url": map_url,
                "status": mstatus,
                "exposed": mstatus == 200,
            }

    # Check for staging/canary domains
    known_candidates = [
        "https://preview.papertrade.xyz",
        "https://staging.papertrade.xyz",
        "https://testnet.papertrade.xyz",
        "https://dev.papertrade.xyz",
    ]
    env_probes = {}
    for cand in known_candidates:
        cstatus, _, _ = fetch_url(cand)
        env_probes[cand] = {
            "status": cstatus,
            "reachable": cstatus in [200, 301, 302, 401, 403],
        }

    scan_result = {
        "target": base_url,
        "build_version": build_version,
        "meta_origins": origins,
        "total_assets": len(asset_data),
        "assets": asset_data,
        "endpoints": sorted(list(endpoints)),
        "search_params": sorted(list(search_params)),
        "sourcemap_exposure": sourcemap_status,
        "staging_probes": env_probes,
    }
    return scan_result


def diff_with_baseline(current: Dict[str, Any], baseline_path: str = STATE_FILE_PATH) -> Dict[str, Any]:
    """Compare current scan against baseline state and return differences."""
    if not os.path.exists(baseline_path):
        return {"status": "NO_BASELINE", "message": f"Baseline file {baseline_path} does not exist yet."}

    with open(baseline_path, "r", encoding="utf-8") as f:
        baseline = json.load(f)

    changes = {
        "status": "DIFF_DETECTED",
        "build_version_changed": False,
        "old_build": baseline.get("build_version"),
        "new_build": current.get("build_version"),
        "added_assets": [],
        "removed_assets": [],
        "modified_assets": [],
        "new_endpoints": [],
        "removed_endpoints": [],
    }

    if current.get("build_version") != baseline.get("build_version"):
        changes["build_version_changed"] = True

    base_assets = baseline.get("assets", {})
    curr_assets = current.get("assets", {})

    for p, meta in curr_assets.items():
        if p not in base_assets:
            changes["added_assets"].append(p)
        elif meta["sha256"] != base_assets[p]["sha256"]:
            changes["modified_assets"].append({
                "path": p,
                "old_sha": base_assets[p]["sha256"],
                "new_sha": meta["sha256"],
            })

    for p in base_assets:
        if p not in curr_assets:
            changes["removed_assets"].append(p)

    base_eps = set(baseline.get("endpoints", []))
    curr_eps = set(current.get("endpoints", []))

    changes["new_endpoints"] = sorted(list(curr_eps - base_eps))
    changes["removed_endpoints"] = sorted(list(base_eps - curr_eps))

    has_diff = any([
        changes["build_version_changed"],
        changes["added_assets"],
        changes["removed_assets"],
        changes["modified_assets"],
        changes["new_endpoints"],
        changes["removed_endpoints"],
    ])

    if not has_diff:
        return {"status": "UNCHANGED", "message": "All bundle hashes, builds, and endpoints match baseline."}

    return changes


def main():
    parser = argparse.ArgumentParser(description="PaperTrade Frontend Bundle Monitor & Diff Audit Tool")
    parser.add_argument("--url", default=DEFAULT_ORIGIN, help="Target origin URL (default: https://exchange.papertrade.xyz)")
    parser.add_argument("--save-baseline", action="store_true", help="Save current scan as baseline")
    parser.add_argument("--diff", action="store_true", help="Compare current scan against stored baseline")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    args = parser.parse_args()

    scan = scan_environment(base_url=args.url)

    if args.save_baseline:
        os.makedirs(os.path.dirname(STATE_FILE_PATH), exist_ok=True)
        with open(STATE_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(scan, f, indent=2)
        print(f"[+] Saved baseline state ({scan['total_assets']} assets, build={scan['build_version']}) to {STATE_FILE_PATH}")
        return

    if args.diff:
        diff_res = diff_with_baseline(scan, STATE_FILE_PATH)
        if args.json:
            print(json.dumps(diff_res, indent=2))
        else:
            print(f"\n=== Bundle Diff Status: {diff_res['status']} ===")
            if diff_res["status"] == "UNCHANGED":
                print("Everything is up-to-date with baseline.")
            elif diff_res["status"] == "NO_BASELINE":
                print(diff_res["message"])
            else:
                if diff_res.get("build_version_changed"):
                    print(f"[!] Build version changed: {diff_res['old_build']} -> {diff_res['new_build']}")
                if diff_res.get("added_assets"):
                    print(f"[+] Added assets ({len(diff_res['added_assets'])}):")
                    for a in diff_res["added_assets"][:10]:
                        print(f"    + {a}")
                if diff_res.get("modified_assets"):
                    print(f"[*] Modified assets ({len(diff_res['modified_assets'])}):")
                    for m in diff_res["modified_assets"][:10]:
                        print(f"    * {m['path']}")
                if diff_res.get("new_endpoints"):
                    print(f"[!] NEW ENDPOINTS DETECTED ({len(diff_res['new_endpoints'])}):")
                    for ep in diff_res["new_endpoints"]:
                        print(f"    >>> {ep}")
        return

    if args.json:
        print(json.dumps(scan, indent=2))
    else:
        print("\n=== Scan Summary ===")
        print(f"Target: {scan['target']}")
        print(f"Build Version: {scan['build_version']}")
        print(f"Total Discovered Assets: {scan['total_assets']}")
        print(f"Endpoints Count: {len(scan['endpoints'])}")
        print("\nTop Endpoints:")
        for ep in scan["endpoints"][:10]:
            print(f"  {ep}")
        print("\nSourcemap Status:")
        for sm, data in scan["sourcemap_exposure"].items():
            print(f"  {sm}: {'EXPOSED (200)' if data['exposed'] else f'Protected ({data['status']})'}")
        print("\nStaging Probes:")
        for url, data in scan["staging_probes"].items():
            print(f"  {url}: {'Reachable' if data['reachable'] else 'Unreachable / NXDOMAIN'}")


if __name__ == "__main__":
    main()
