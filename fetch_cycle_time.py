#!/usr/bin/env python3
"""
Fetch Cycle Time Metrics from LinearB API.

This script fetches:
- Cycle Time (full cycle: coding + pickup + review + deploy)
- Coding Time (time from first commit to PR creation)
- Pickup Time (time from PR creation to first review)
- Review Time (time from first review to merge)
- Deploy Time (time from merge to production)

Data can be grouped by:
- Teams (default) - for teams under 'Paypay India' group
- Repositories (--by-repo) - for all repositories in the organization

Usage:
    1. Set the LINEARB_API_KEY environment variable
    2. Run: python fetch_cycle_time.py

Commands:
    python fetch_cycle_time.py              # Group by team (default)
    python fetch_cycle_time.py --by-repo    # Group by repository
    python fetch_cycle_time.py --list-repos # List all repositories

Optional environment variables:
    - DATE_AFTER: Start date in YYYY-MM-DD format (default: 30 days ago)
    - DATE_BEFORE: End date in YYYY-MM-DD format (default: today)
"""

import os
import sys
import time
import csv
import json
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from collections import deque

# For chart generation
try:
    import pandas as pd
    import matplotlib.pyplot as plt
    CHARTS_AVAILABLE = True
except ImportError:
    CHARTS_AVAILABLE = False

# =============================================================================
# CONFIGURATION
# =============================================================================

# LinearB API Configuration
LINEARB_BASE = "https://public-api.linearb.io"
TEAMS_ENDPOINT = f"{LINEARB_BASE}/api/v2/teams"
EXPORT_ENDPOINT = f"{LINEARB_BASE}/api/v2/measurements/export"
MEASUREMENTS_ENDPOINT = f"{LINEARB_BASE}/api/v2/measurements"
SERVICES_ENDPOINT = f"{LINEARB_BASE}/api/v1/services"

# Target Organization
TARGET_PARENT_NAME = "Paypay India"

# Default date range (last 30 days if not specified via env vars)
DEFAULT_DAYS_BACK = 30

# Teams to exclude (case-insensitive)
EXCLUDED_TEAM_NAMES = {"payments qa", "paypay india merchant & finance qa"}
EXCLUDED_IDS_FILE = "excluded_team_ids.json"

# Output files
OUTPUT_CSV = "cycle_time_by_team.csv"
OUTPUT_JSON = "cycle_time_by_team.json"
DAILY_OUTPUT_CSV = "cycle_time_by_team_daily.csv"


class LinearBError(Exception):
    """Custom exception for LinearB API errors."""
    pass


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _req(method: str, url: str, headers: dict, **kwargs) -> requests.Response:
    """
    Make an HTTP request with retry logic for transient errors.
    """
    backoff = 1.0
    timeout = kwargs.pop("timeout", 30)
    
    for attempt in range(5):
        try:
            response = requests.request(method, url, headers=headers, timeout=timeout, **kwargs)
            
            if response.status_code < 400:
                return response
            
            # Retry on transient errors
            if response.status_code in (429, 500, 502, 503, 504):
                print(f"  ⚠️ Got {response.status_code}, retrying in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 16)
                continue
            
            raise LinearBError(f"{response.status_code}: {response.text}")
        
        except requests.exceptions.RequestException as e:
            if attempt < 4:
                print(f"  ⚠️ Request failed: {e}, retrying in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 16)
                continue
            raise LinearBError(f"Request failed after retries: {e}")
    
    raise LinearBError("Request failed after maximum retries")


def minutes_to_dhm(minutes: Optional[float]) -> str:
    """Convert minutes to human-readable days/hours/minutes format."""
    try:
        total = int(round(float(minutes)))
    except (TypeError, ValueError):
        return ""
    
    d, rem = divmod(total, 1440)  # 1440 minutes in a day
    h, m = divmod(rem, 60)
    
    parts = []
    if d:
        parts.append(f"{d}d")
    if h or d:
        parts.append(f"{h}h")
    parts.append(f"{m}m")
    
    return " ".join(parts)


def get_date_range() -> tuple:
    """Get date range from environment variables or use defaults."""
    date_before = os.getenv("DATE_BEFORE")
    date_after = os.getenv("DATE_AFTER")
    
    if not date_before:
        date_before = datetime.now().strftime("%Y-%m-%d")
    
    if not date_after:
        date_after = (datetime.now() - timedelta(days=DEFAULT_DAYS_BACK)).strftime("%Y-%m-%d")
    
    return date_after, date_before


def inclusive_end_date_str(before_iso: str) -> str:
    """Convert exclusive end date to inclusive end date string."""
    return (datetime.strptime(before_iso, "%Y-%m-%d").date() - timedelta(days=1)).isoformat()


# =============================================================================
# TEAMS MANAGEMENT
# =============================================================================

def fetch_all_teams(api_key: str) -> List[Dict]:
    """Fetch all teams from LinearB with pagination support."""
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    all_teams = []
    offset = 0
    page_size = 50
    
    print("🔍 Fetching teams from LinearB...")
    
    while True:
        params = {"offset": offset, "page_size": page_size}
        response = _req("GET", TEAMS_ENDPOINT, headers, params=params)
        data = response.json()
        items = data.get("items", [])
        all_teams.extend(items)
        
        total = data.get("total", len(all_teams))
        print(f"  📥 Fetched {len(all_teams)}/{total} teams...")
        
        if offset + page_size >= total or not items:
            break
        
        offset += page_size
    
    print(f"✅ Total teams fetched: {len(all_teams)}")
    return all_teams


def _build_indexes(teams: List[Dict]) -> tuple:
    """Build ID and name indexes for teams."""
    id_index = {}
    name_index = {}
    
    for team in teams:
        tid = str(team.get("id") or team.get("_id") or "")
        name = team.get("name")
        
        if tid:
            id_index[tid] = team
        if name:
            name_index[name] = team
    
    return id_index, name_index


def _get_parent_id(team: Dict) -> Optional[str]:
    """Extract parent team ID from team data."""
    parent = team.get("parent") or team.get("parentTeam") or team.get("parent_team")
    
    if isinstance(parent, dict):
        return str(parent.get("id") or parent.get("_id") or "")
    
    for key in ("parent_team_id", "parentTeamId", "parent_id", "parentId"):
        if team.get(key):
            return str(team[key])
    
    return None


def compute_depths(teams: List[Dict], parent_name: str) -> List[Dict]:
    """Compute depth levels for all teams relative to parent."""
    id_index, name_index = _build_indexes(teams)
    
    root = name_index.get(parent_name)
    if not root:
        raise LinearBError(f"Parent team '{parent_name}' not found.")
    
    root_id = str(root.get("id") or root.get("_id"))
    
    # Build children map
    children = {tid: [] for tid in id_index}
    for team in teams:
        tid = str(team.get("id") or team.get("_id") or "")
        pid = _get_parent_id(team)
        if pid and pid in children:
            children[pid].append(tid)
    
    # BFS to compute depths
    depth = {root_id: 0}
    path = {root_id: [parent_name]}
    queue = deque([root_id])
    
    while queue:
        current = queue.popleft()
        for child_id in children.get(current, []):
            depth[child_id] = depth[current] + 1
            child_name = id_index[child_id].get("name")
            path[child_id] = path[current] + ([child_name] if child_name else [])
            queue.append(child_id)
    
    # Attach depth info to teams
    for team in teams:
        tid = str(team.get("id") or team.get("_id") or "")
        team["depth_level_under_parent"] = depth.get(tid)
        team["path_from_parent"] = path.get(tid, [])
    
    return teams


def get_teams_under_parent(api_key: str, parent_name: str, depth: int = 2) -> List[Dict]:
    """
    Get teams at specified depth under the parent organization.
    
    Args:
        api_key: LinearB API key
        parent_name: Name of the parent team/organization
        depth: Depth level to filter (default: 2 for grandchildren)
    
    Returns:
        List of teams at the specified depth
    """
    teams = fetch_all_teams(api_key)
    teams = compute_depths(teams, parent_name)
    
    # Load persisted exclusions
    excluded_ids = set()
    if os.path.exists(EXCLUDED_IDS_FILE):
        try:
            with open(EXCLUDED_IDS_FILE) as f:
                loaded = json.load(f)
                if isinstance(loaded, list):
                    excluded_ids = set(str(x) for x in loaded)
        except Exception as e:
            print(f"  ⚠️ Warning loading excluded IDs: {e}")
    
    # Discover IDs by names if not already persisted
    if not excluded_ids:
        found = {}
        for team in teams:
            name = (team.get("name") or "").strip().lower()
            if name in EXCLUDED_TEAM_NAMES:
                tid = str(team.get("id") or team.get("_id"))
                if tid:
                    excluded_ids.add(tid)
                    found[name] = tid
        
        if found:
            try:
                with open(EXCLUDED_IDS_FILE, "w") as f:
                    json.dump(sorted(list(excluded_ids)), f)
            except Exception as e:
                print(f"  ⚠️ Warning persisting excluded IDs: {e}")
    
    # Filter teams at specified depth, excluding certain teams
    def normalize(name):
        return (name or "").strip().lower()
    
    filtered = [
        t for t in teams
        if t.get("depth_level_under_parent") == depth
        and str(t.get("id") or t.get("_id")) not in excluded_ids
        and normalize(t.get("name")) not in EXCLUDED_TEAM_NAMES
    ]
    
    # Sort alphabetically
    filtered = sorted(filtered, key=lambda t: (t.get("name") or "").lower())
    
    print(f"✅ Found {len(filtered)} teams at depth {depth} under '{parent_name}'")
    return filtered


# =============================================================================
# REPOSITORIES MANAGEMENT
# =============================================================================

def fetch_all_repositories(api_key: str) -> List[Dict]:
    """
    Fetch all repositories from LinearB via the Services endpoint.
    
    The Services endpoint returns services which contain repository information.
    Each service can have multiple repositories (paths).
    
    Returns:
        List of dictionaries with repository info: {id, name, service_id, service_name}
    """
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    all_repos = []
    offset = 0
    page_size = 50
    
    print("🔍 Fetching repositories from LinearB (via Services endpoint)...")
    
    while True:
        params = {"offset": offset, "page_size": page_size}
        response = _req("GET", SERVICES_ENDPOINT, headers, params=params)
        data = response.json()
        
        items = data.get("items", [])
        
        # Extract repositories from services
        for service in items:
            service_id = service.get("id")
            service_name = service.get("name", "")
            
            # Repositories are in the "paths" or "repositories" field
            repos = service.get("paths", []) or service.get("repositories", [])
            
            for repo in repos:
                if isinstance(repo, dict):
                    repo_info = {
                        "id": repo.get("id"),
                        "name": repo.get("name", ""),
                        "service_id": service_id,
                        "service_name": service_name,
                    }
                    all_repos.append(repo_info)
        
        total = data.get("total", len(items))
        print(f"  📥 Processed {min(offset + page_size, total)}/{total} services...")
        
        if offset + page_size >= total or not items:
            break
        
        offset += page_size
    
    # Remove duplicates by repo ID
    seen_ids = set()
    unique_repos = []
    for repo in all_repos:
        repo_id = repo.get("id")
        if repo_id and repo_id not in seen_ids:
            seen_ids.add(repo_id)
            unique_repos.append(repo)
    
    print(f"✅ Total unique repositories: {len(unique_repos)}")
    return unique_repos


def display_repositories(repos: List[Dict]) -> None:
    """Display repository information in a formatted table."""
    if not repos:
        print("No repositories found.")
        return
    
    print("\n" + "=" * 100)
    print(f"{'ID':<12} | {'Repository Name':<50} | {'Service':<30}")
    print("=" * 100)
    
    for repo in sorted(repos, key=lambda r: (r.get("name") or "").lower()):
        repo_id = str(repo.get("id") or "N/A")[:10]
        name = (repo.get("name") or "N/A")[:48]
        service = (repo.get("service_name") or "N/A")[:28]
        
        print(f"{repo_id:<12} | {name:<50} | {service:<30}")
    
    print("=" * 100)


def save_repositories_to_csv(repos: List[Dict], filename: str = "linearb_repositories.csv") -> str:
    """Save repositories to a CSV file."""
    if not repos:
        print("No repositories to save.")
        return filename
    
    fieldnames = ["id", "name", "service_id", "service_name"]
    
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for repo in repos:
            writer.writerow({
                "id": repo.get("id", ""),
                "name": repo.get("name", ""),
                "service_id": repo.get("service_id", ""),
                "service_name": repo.get("service_name", ""),
            })
    
    print(f"📄 Saved repositories to: {filename}")
    return filename


def save_repositories_to_json(repos: List[Dict], filename: str = "linearb_repositories.json") -> str:
    """Save repositories to a JSON file."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(repos, f, indent=2, ensure_ascii=False)
    
    print(f"💾 Saved repositories to: {filename}")
    return filename


# =============================================================================
# METRICS EXPORT - BY REPOSITORY
# =============================================================================

def export_metrics_by_repo(
    api_key: str,
    repo_ids: List[int],
    date_after: str,
    date_before: str,
    output_path: str,
    roll_up: str = "custom"
) -> str:
    """
    Export cycle time metrics grouped by repository.
    
    Metrics (P50):
    - branch.computed.cycle_time: Full cycle time
    - branch.time_to_pr: Coding time (first commit to PR)
    - branch.time_to_review: Pickup time (PR to first review)
    - branch.review_time: Review time (first review to merge)
    - branch.time_to_prod: Deploy time (merge to production)
    
    Note: API limits to 50 repositories per request, so we batch requests.
    
    Args:
        api_key: LinearB API key
        repo_ids: List of repository IDs to include
        date_after: Start date (inclusive) in YYYY-MM-DD format
        date_before: End date (exclusive) in YYYY-MM-DD format
        output_path: Path to save the CSV output
        roll_up: Aggregation period ("custom", "1d", "1w", "1m")
    
    Returns:
        Path to the saved CSV file
    """
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    
    print(f"📊 Fetching cycle time metrics by repository...")
    print(f"   Date range: {date_after} to {inclusive_end_date_str(date_before)}")
    print(f"   Repositories: {len(repo_ids)}")
    print(f"   Roll-up: {roll_up}")
    
    # API limits to 10 repositories per request when grouping by repository
    BATCH_SIZE = 10
    all_csv_rows = []
    header_row = None
    
    for i in range(0, len(repo_ids), BATCH_SIZE):
        batch = repo_ids[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(repo_ids) + BATCH_SIZE - 1) // BATCH_SIZE
        
        print(f"   📦 Batch {batch_num}/{total_batches}: {len(batch)} repositories...")
        
        payload = {
            "group_by": "repository",
            "roll_up": roll_up,
            "repository_ids": [int(rid) for rid in batch],
            "requested_metrics": [
                {"name": "branch.computed.cycle_time", "agg": "p50"},
                {"name": "branch.computed.cycle_time", "agg": "p75"},
                {"name": "branch.time_to_pr", "agg": "p50"},
                {"name": "branch.time_to_review", "agg": "p50"},
                {"name": "branch.review_time", "agg": "p50"},
                {"name": "branch.time_to_prod", "agg": "p50"},
            ],
            "time_ranges": [{"after": date_after, "before": date_before}],
            "limit": len(batch)
        }
        
        response = _req("POST", EXPORT_ENDPOINT, headers, params={"file_format": "csv"}, json=payload)
        
        if response.status_code == 204:
            print(f"      ⚠️ No data for this batch")
            continue
        
        report_url = response.json().get("report_url")
        if not report_url:
            print(f"      ⚠️ No report URL for this batch")
            continue
        
        # Download and parse the CSV
        csv_data = _req("GET", report_url, headers={}).content.decode("utf-8")
        lines = csv_data.strip().split("\n")
        
        if not lines:
            continue
        
        # First batch: capture header
        if header_row is None:
            header_row = lines[0]
            all_csv_rows.append(header_row)
        
        # Add data rows (skip header for subsequent batches)
        for line in lines[1:]:
            if line.strip():
                all_csv_rows.append(line)
    
    if not all_csv_rows:
        raise LinearBError("No data available for the specified date range")
    
    # Write combined CSV
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(all_csv_rows))
    
    print(f"✅ Saved raw CSV: {output_path} ({len(all_csv_rows) - 1} rows)")
    return output_path


# =============================================================================
# METRICS EXPORT - BY TEAM
# =============================================================================

def export_metrics_by_team(
    api_key: str,
    team_ids: List[str],
    date_after: str,
    date_before: str,
    output_path: str,
    roll_up: str = "custom"
) -> str:
    """
    Export cycle time metrics grouped by team.
    
    Metrics (P50):
    - branch.computed.cycle_time: Full cycle time
    - branch.time_to_pr: Coding time (first commit to PR)
    - branch.time_to_review: Pickup time (PR to first review)
    - branch.review_time: Review time (first review to merge)
    - branch.time_to_prod: Deploy time (merge to production)
    
    Note: API limits to 50 teams per request, so we batch requests.
    
    Args:
        api_key: LinearB API key
        team_ids: List of team IDs to include
        date_after: Start date (inclusive) in YYYY-MM-DD format
        date_before: End date (exclusive) in YYYY-MM-DD format
        output_path: Path to save the CSV output
        roll_up: Aggregation period ("custom", "1d", "1w", "1m")
    
    Returns:
        Path to the saved CSV file
    """
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    
    print(f"📊 Fetching cycle time metrics by team...")
    print(f"   Date range: {date_after} to {inclusive_end_date_str(date_before)}")
    print(f"   Teams: {len(team_ids)}")
    print(f"   Roll-up: {roll_up}")
    
    # API limits to 50 teams per request - batch them
    BATCH_SIZE = 50
    all_csv_rows = []
    header_row = None
    
    for i in range(0, len(team_ids), BATCH_SIZE):
        batch = team_ids[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(team_ids) + BATCH_SIZE - 1) // BATCH_SIZE
        
        if total_batches > 1:
            print(f"   📦 Batch {batch_num}/{total_batches}: {len(batch)} teams...")
        
        payload = {
            "group_by": "team",
            "roll_up": roll_up,
            "team_ids": [int(tid) for tid in batch],
            "requested_metrics": [
                {"name": "branch.computed.cycle_time", "agg": "p50"},
                {"name": "branch.time_to_pr", "agg": "p50"},
                {"name": "branch.time_to_review", "agg": "p50"},
                {"name": "branch.review_time", "agg": "p50"},
                {"name": "branch.time_to_prod", "agg": "p50"},
            ],
            "time_ranges": [{"after": date_after, "before": date_before}],
            "limit": len(batch)
        }
        
        response = _req("POST", EXPORT_ENDPOINT, headers, params={"file_format": "csv"}, json=payload)
        
        if response.status_code == 204:
            print(f"      ⚠️ No data for this batch")
            continue
        
        report_url = response.json().get("report_url")
        if not report_url:
            print(f"      ⚠️ No report URL for this batch")
            continue
        
        # Download and parse the CSV
        csv_data = _req("GET", report_url, headers={}).content.decode("utf-8")
        lines = csv_data.strip().split("\n")
        
        if not lines:
            continue
        
        # First batch: capture header
        if header_row is None:
            header_row = lines[0]
            all_csv_rows.append(header_row)
        
        # Add data rows (skip header for subsequent batches)
        for line in lines[1:]:
            if line.strip():
                all_csv_rows.append(line)
    
    if not all_csv_rows:
        raise LinearBError("No data available for the specified date range")
    
    # Write combined CSV
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(all_csv_rows))
    
    print(f"✅ Saved raw CSV: {output_path} ({len(all_csv_rows) - 1} rows)")
    return output_path


def augment_csv_with_dhm(input_path: str, output_path: str, repo_name_map: Optional[Dict[int, str]] = None) -> str:
    """
    Add human-readable time columns (days/hours/minutes) to CSV.
    Also rename columns to be more readable.
    
    Args:
        input_path: Path to input CSV
        output_path: Path to output CSV
        repo_name_map: Optional dict mapping repo_id -> repo_name for enrichment
    """
    with open(input_path, encoding="utf-8") as f:
        rows = list(csv.reader(f))
    
    if not rows:
        raise LinearBError("CSV file is empty")
    
    header = rows[0]
    data = rows[1:]
    
    # Find metric columns
    def find_col(target):
        for i, h in enumerate(header):
            if h.strip() == target:
                return i
        for i, h in enumerate(header):
            if target in h:
                return i
        return None
    
    col_cycle = find_col("branch.computed.cycle_time:p50")
    col_coding = find_col("branch.time_to_pr:p50")
    col_pickup = find_col("branch.time_to_review:p50")
    col_review = find_col("branch.review_time:p50")
    col_deploy = find_col("branch.time_to_prod:p50")
    
    # Find repository ID column for name enrichment
    col_repo_id = find_col("repository_id")
    
    # Check if we need to add repo name column
    add_repo_name = repo_name_map and col_repo_id is not None and find_col("repository_name") is None
    
    # Add dhm columns (and repo_name if needed)
    new_header = header.copy()
    if add_repo_name:
        # Insert repository_name right after repository_id
        new_header.insert(col_repo_id + 1, "repository_name")
    new_header.extend([
        "cycle_time_dhm",
        "coding_time_dhm",
        "pickup_time_dhm",
        "review_time_dhm",
        "deploy_time_dhm",
    ])
    
    def get_val(row, idx):
        if idx is None or idx >= len(row) or row[idx] == "":
            return None
        try:
            return float(row[idx])
        except (ValueError, TypeError):
            return None
    
    new_rows = [new_header]
    for row in data:
        new_row = row.copy()
        
        # Add repository name if mapping provided
        if add_repo_name:
            try:
                repo_id = int(row[col_repo_id]) if row[col_repo_id] else None
                repo_name = repo_name_map.get(repo_id, "") if repo_id else ""
            except (ValueError, TypeError):
                repo_name = ""
            new_row.insert(col_repo_id + 1, repo_name)
        
        # Add dhm columns
        new_row.extend([
            minutes_to_dhm(get_val(row, col_cycle)),
            minutes_to_dhm(get_val(row, col_coding)),
            minutes_to_dhm(get_val(row, col_pickup)),
            minutes_to_dhm(get_val(row, col_review)),
            minutes_to_dhm(get_val(row, col_deploy)),
        ])
        new_rows.append(new_row)
    
    # Rename columns for readability (supports both team and repository grouping)
    column_renames = {
        "after": "Start Date",
        "before": "End Date",
        "team_id": "Team ID",
        "team_name": "Team Name",
        "repository_id": "Repository ID",
        "repository_name": "Repository Name",
        "repo_id": "Repository ID",
        "repo_name": "Repository Name",
        "branch.computed.cycle_time:p50": "Cycle Time (P50)",
        "branch.computed.cycle_time:p75": "Cycle Time (P75)",
        "branch.time_to_pr:p50": "Coding Time (P50)",
        "branch.time_to_review:p50": "Pickup Time (P50)",
        "branch.review_time:p50": "Review Time (P50)",
        "branch.time_to_prod:p50": "Deploy Time (P50)",
        "cycle_time_dhm": "Cycle Time (P50) - dhm",
        "coding_time_dhm": "Coding Time (P50) - dhm",
        "pickup_time_dhm": "Pickup Time (P50) - dhm",
        "review_time_dhm": "Review Time (P50) - dhm",
        "deploy_time_dhm": "Deploy Time (P50) - dhm",
    }
    
    new_rows[0] = [column_renames.get(h, h) for h in new_rows[0]]
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(new_rows)
    
    print(f"✅ Saved augmented CSV: {output_path}")
    return output_path


def csv_to_json(csv_path: str, json_path: str) -> str:
    """Convert CSV to JSON format."""
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        data = list(reader)
    
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Saved JSON: {json_path}")
    return json_path


def export_dashboard_json(
    api_key: str,
    team_ids: List[str],
    team_names: List[str],
    output_path: str = "dashboard/data/cycle_time_data.json"
) -> str:
    """
    Export monthly aggregated cycle time data for the dashboard.
    
    Fetches last 6 months of data with monthly roll-up and exports
    as JSON for the static dashboard.
    
    Args:
        api_key: LinearB API key
        team_ids: List of team IDs to include
        team_names: List of team names (for reference)
        output_path: Path to save the JSON output
    
    Returns:
        Path to the saved JSON file
    """
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    
    # Calculate date range: last 6 months
    today = datetime.now()
    # Start from the first day of the month, 6 months ago
    start_date = (today.replace(day=1) - timedelta(days=180)).replace(day=1)
    end_date = today
    
    date_after = start_date.strftime("%Y-%m-%d")
    date_before = end_date.strftime("%Y-%m-%d")
    
    print(f"📊 Fetching monthly cycle time data for dashboard...")
    print(f"   Date range: {date_after} to {date_before}")
    print(f"   Teams: {len(team_ids)}")
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    
    # Fetch monthly aggregated data
    payload = {
        "group_by": "team",
        "roll_up": "1m",  # Monthly roll-up
        "team_ids": [int(tid) for tid in team_ids],
        "requested_metrics": [
            {"name": "branch.computed.cycle_time", "agg": "p50"},
            {"name": "branch.time_to_pr", "agg": "p50"},
            {"name": "branch.time_to_review", "agg": "p50"},
            {"name": "branch.review_time", "agg": "p50"},
            {"name": "branch.time_to_prod", "agg": "p50"},
        ],
        "time_ranges": [{"after": date_after, "before": date_before}],
        "limit": len(team_ids) * 12  # Max 12 months per team
    }
    
    response = _req("POST", EXPORT_ENDPOINT, headers, params={"file_format": "csv"}, json=payload)
    
    if response.status_code == 204:
        print("⚠️ No data available for the specified date range")
        # Save empty result
        empty_data = {
            "generated_at": datetime.now().isoformat(),
            "date_range": {"start": date_after, "end": date_before},
            "teams": team_names,
            "monthly_data": [],
            "summary": {}
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(empty_data, f, indent=2, ensure_ascii=False)
        return output_path
    
    # Get CSV from report URL
    report_url = response.json().get("report_url")
    csv_data = _req("GET", report_url, headers={}).content.decode("utf-8")
    
    # Parse CSV data
    import io
    reader = csv.DictReader(io.StringIO(csv_data))
    rows = list(reader)
    
    print(f"   Retrieved {len(rows)} data points")
    
    # Process and structure the data
    monthly_data = []
    by_team = {}
    by_month = {}
    
    for row in rows:
        team_name = row.get("team_name", "")
        date_str = row.get("after", "")  # Month start date
        
        # Parse metrics (convert from minutes)
        cycle_time = float(row.get("branch.computed.cycle_time:p50", 0) or 0)
        coding_time = float(row.get("branch.time_to_pr:p50", 0) or 0)
        pickup_time = float(row.get("branch.time_to_review:p50", 0) or 0)
        review_time = float(row.get("branch.review_time:p50", 0) or 0)
        deploy_time = float(row.get("branch.time_to_prod:p50", 0) or 0)
        
        # Determine month name
        try:
            dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            month_name = dt.strftime("%Y-%m")
            month_display = dt.strftime("%b %Y")
        except:
            month_name = date_str[:7] if len(date_str) >= 7 else "Unknown"
            month_display = month_name
        
        entry = {
            "team_name": team_name,
            "month": month_name,
            "month_display": month_display,
            "cycle_time_minutes": cycle_time,
            "coding_time_minutes": coding_time,
            "pickup_time_minutes": pickup_time,
            "review_time_minutes": review_time,
            "deploy_time_minutes": deploy_time,
            "cycle_time_dhm": minutes_to_dhm(cycle_time),
            "coding_time_dhm": minutes_to_dhm(coding_time),
            "pickup_time_dhm": minutes_to_dhm(pickup_time),
            "review_time_dhm": minutes_to_dhm(review_time),
            "deploy_time_dhm": minutes_to_dhm(deploy_time),
        }
        
        monthly_data.append(entry)
        
        # Group by team
        if team_name not in by_team:
            by_team[team_name] = []
        by_team[team_name].append(entry)
        
        # Group by month
        if month_name not in by_month:
            by_month[month_name] = {"month": month_name, "month_display": month_display, "teams": {}}
        by_month[month_name]["teams"][team_name] = entry
    
    # Calculate summary statistics
    all_cycle_times = [e["cycle_time_minutes"] for e in monthly_data if e["cycle_time_minutes"] > 0]
    summary = {
        "total_data_points": len(monthly_data),
        "teams_count": len(by_team),
        "months_count": len(by_month),
        "avg_cycle_time_minutes": round(sum(all_cycle_times) / len(all_cycle_times), 1) if all_cycle_times else 0,
        "avg_cycle_time_dhm": minutes_to_dhm(sum(all_cycle_times) / len(all_cycle_times)) if all_cycle_times else "",
    }
    
    # Build output structure
    output_data = {
        "generated_at": datetime.now().isoformat(),
        "date_range": {
            "start": date_after,
            "end": date_before
        },
        "teams": sorted(list(by_team.keys())),
        "months": sorted(list(by_month.keys())),
        "summary": summary,
        "by_team": by_team,
        "by_month": dict(sorted(by_month.items())),
        "monthly_data": sorted(monthly_data, key=lambda x: (x["month"], x["team_name"])),
    }
    
    # Save JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Dashboard JSON saved to: {output_path}")
    return output_path


def display_summary(csv_path: str, group_by: str = "team") -> None:
    """Display a summary of the fetched metrics."""
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    if not rows:
        print("No data to display.")
        return
    
    # Determine if this is team or repository data
    is_repo = group_by == "repo" or "Repository Name" in rows[0] or "repository_name" in rows[0]
    
    if is_repo:
        print("\n" + "=" * 130)
        print(f"{'Repository':<30} | {'Cycle Time':<12} | {'Coding':<12} | {'Pickup':<12} | {'Review':<12} | {'Deploy':<12}")
        print("=" * 130)
        
        for row in rows:
            name = (row.get("Repository Name") or row.get("repository_name") or row.get("repo_name") or "N/A")[:28]
            cycle = row.get("Cycle Time (P50) - dhm") or row.get("cycle_time_dhm") or "-"
            coding = row.get("Coding Time (P50) - dhm") or row.get("coding_time_dhm") or "-"
            pickup = row.get("Pickup Time (P50) - dhm") or row.get("pickup_time_dhm") or "-"
            review = row.get("Review Time (P50) - dhm") or row.get("review_time_dhm") or "-"
            deploy = row.get("Deploy Time (P50) - dhm") or row.get("deploy_time_dhm") or "-"
            
            print(f"{name:<30} | {cycle:<12} | {coding:<12} | {pickup:<12} | {review:<12} | {deploy:<12}")
        
        print("=" * 130)
        print(f"Total repositories: {len(rows)}")
    else:
        print("\n" + "=" * 130)
        print(f"{'Team':<30} | {'Cycle Time':<12} | {'Coding':<12} | {'Pickup':<12} | {'Review':<12} | {'Deploy':<12}")
        print("=" * 130)
        
        for row in rows:
            team = (row.get("Team Name") or row.get("team_name") or "N/A")[:28]
            cycle = row.get("Cycle Time (P50) - dhm") or row.get("cycle_time_dhm") or "-"
            coding = row.get("Coding Time (P50) - dhm") or row.get("coding_time_dhm") or "-"
            pickup = row.get("Pickup Time (P50) - dhm") or row.get("pickup_time_dhm") or "-"
            review = row.get("Review Time (P50) - dhm") or row.get("review_time_dhm") or "-"
            deploy = row.get("Deploy Time (P50) - dhm") or row.get("deploy_time_dhm") or "-"
            
            print(f"{team:<30} | {cycle:<12} | {coding:<12} | {pickup:<12} | {review:<12} | {deploy:<12}")
        
        print("=" * 130)
        print(f"Total teams: {len(rows)}")


# =============================================================================
# CHART GENERATION
# =============================================================================

def is_k8s_related_repo(repo_name: str) -> bool:
    """
    Check if a repository is Kubernetes/k8s related based on its name.
    
    Matches: k8s, kubernetes, kube, helm, argocd, flux, istio, etc.
    """
    name_lower = repo_name.lower()
    k8s_patterns = [
        "k8s",
        "kubernetes", 
        "kube-",
        "-kube",
        "helm",
        "argocd",
        "argo-cd",
        "flux",
        "istio",
        "envoy",
        "ingress",
        "terraform",
        "infra",
        "gitops",
        "deploy",
        "manifest",
    ]
    return any(pattern in name_lower for pattern in k8s_patterns)


def calculate_cycle_time_stats(csv_path: str, exclude_k8s: bool = False) -> Dict[str, float]:
    """
    Calculate cycle time statistics from CSV data (both P50 and P75).
    
    Args:
        csv_path: Path to the CSV file
        exclude_k8s: If True, exclude repos with k8s-related names
    
    Returns:
        Dict with p50_avg, p75_avg, count stats
    """
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    if not rows:
        return {"p50_avg": 0, "p75_avg": 0, "count": 0}
    
    # Get cycle time values
    p50_values = []
    p75_values = []
    
    for row in rows:
        # Check if we should exclude this row
        repo_name = row.get("Repository Name") or row.get("repository_name") or row.get("repo_name") or ""
        if exclude_k8s and is_k8s_related_repo(repo_name):
            continue
        
        # Get P50 cycle time value (in minutes)
        p50_val = row.get("Cycle Time (P50)") or row.get("branch.computed.cycle_time:p50") or ""
        try:
            val = float(p50_val)
            if val > 0:
                p50_values.append(val)
        except (ValueError, TypeError):
            pass
        
        # Get P75 cycle time value (in minutes)
        p75_val = row.get("Cycle Time (P75)") or row.get("branch.computed.cycle_time:p75") or ""
        try:
            val = float(p75_val)
            if val > 0:
                p75_values.append(val)
        except (ValueError, TypeError):
            pass
    
    return {
        "p50_avg": sum(p50_values) / len(p50_values) if p50_values else 0,
        "p75_avg": sum(p75_values) / len(p75_values) if p75_values else 0,
        "count": len(p50_values)
    }


def generate_cycle_time_comparison_chart(
    india_csv: str,
    other_csv: str,
    output_path: str,
    date_range: str
) -> str:
    """
    Generate a comparison chart showing cycle times for India vs Other teams,
    with and without k8s repos.
    
    Args:
        india_csv: Path to India teams repo-level CSV
        other_csv: Path to Other teams repo-level CSV  
        output_path: Path to save the chart PNG
        date_range: Date range string for chart title
    
    Returns:
        Path to saved chart
    """
    if not CHARTS_AVAILABLE:
        print("⚠️ matplotlib/pandas not available. Install with: pip install matplotlib pandas")
        return ""
    
    # Calculate stats for each category
    india_all = calculate_cycle_time_stats(india_csv, exclude_k8s=False)
    india_no_k8s = calculate_cycle_time_stats(india_csv, exclude_k8s=True)
    other_all = calculate_cycle_time_stats(other_csv, exclude_k8s=False)
    other_no_k8s = calculate_cycle_time_stats(other_csv, exclude_k8s=True)
    
    # Convert minutes to hours for readability
    def mins_to_hours(mins):
        return mins / 60
    
    # Create the chart
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Colors
    color_all = "#2c5282"
    color_no_k8s = "#9b2c2c"
    
    # Chart 1: India Teams
    categories = ["All Repos", "Excluding k8s"]
    india_values = [mins_to_hours(india_all["avg"]), mins_to_hours(india_no_k8s["avg"])]
    india_counts = [india_all["count"], india_no_k8s["count"]]
    
    bars1 = ax1.bar(categories, india_values, color=[color_all, color_no_k8s], width=0.5)
    ax1.set_title("Paypay India Teams\nAverage Cycle Time (P50)", fontsize=14, fontweight="bold")
    ax1.set_ylabel("Hours", fontsize=12)
    ax1.set_ylim(0, max(india_values) * 1.3 if max(india_values) > 0 else 10)
    
    # Add value labels
    for bar, val, count in zip(bars1, india_values, india_counts):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{val:.1f}h\n({count} repos)',
                ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    # Chart 2: Other Teams
    other_values = [mins_to_hours(other_all["avg"]), mins_to_hours(other_no_k8s["avg"])]
    other_counts = [other_all["count"], other_no_k8s["count"]]
    
    bars2 = ax2.bar(categories, other_values, color=[color_all, color_no_k8s], width=0.5)
    ax2.set_title("Other Teams\nAverage Cycle Time (P50)", fontsize=14, fontweight="bold")
    ax2.set_ylabel("Hours", fontsize=12)
    ax2.set_ylim(0, max(other_values) * 1.3 if max(other_values) > 0 else 10)
    
    # Add value labels
    for bar, val, count in zip(bars2, other_values, other_counts):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{val:.1f}h\n({count} repos)',
                ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    # Add legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=color_all, label='All Repositories'),
        Patch(facecolor=color_no_k8s, label='Excluding k8s repos')
    ]
    fig.legend(handles=legend_elements, loc='upper center', ncol=2, 
               bbox_to_anchor=(0.5, 0.02), fontsize=11)
    
    # Main title
    fig.suptitle(f"Cycle Time Comparison: All Repos vs Excluding k8s\n{date_range}", 
                 fontsize=16, fontweight="bold", y=1.02)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"📊 Chart saved: {output_path}")
    return output_path


def generate_combined_cycle_time_chart(
    repo_csv: str,
    output_path: str,
    date_range: str,
    title_prefix: str = ""
) -> str:
    """
    Generate a single chart comparing all repos vs excluding k8s repos.
    Shows both P50 and P75 cycle times.
    
    Args:
        repo_csv: Path to repository-level CSV
        output_path: Path to save the chart PNG
        date_range: Date range string for chart title
        title_prefix: Optional prefix for title (e.g., "India Teams" or "Other Teams")
    
    Returns:
        Path to saved chart
    """
    if not CHARTS_AVAILABLE:
        print("⚠️ matplotlib/pandas not available. Install with: pip install matplotlib pandas")
        return ""
    
    # Calculate stats
    stats_all = calculate_cycle_time_stats(repo_csv, exclude_k8s=False)
    stats_no_k8s = calculate_cycle_time_stats(repo_csv, exclude_k8s=True)
    
    # Convert minutes to hours
    def mins_to_hours(mins):
        return mins / 60
    
    # Create chart with grouped bars
    fig, ax = plt.subplots(figsize=(10, 7))
    
    import numpy as np
    categories = ["All Repos", "App Repos Only\n(excl. k8s/infra)"]
    x = np.arange(len(categories))
    width = 0.35
    
    p50_values = [mins_to_hours(stats_all["p50_avg"]), mins_to_hours(stats_no_k8s["p50_avg"])]
    p75_values = [mins_to_hours(stats_all["p75_avg"]), mins_to_hours(stats_no_k8s["p75_avg"])]
    counts = [stats_all["count"], stats_no_k8s["count"]]
    
    # Colors
    color_p50 = "#2c5282"
    color_p75 = "#9b2c2c"
    
    bars1 = ax.bar(x - width/2, p50_values, width, label='P50', color=color_p50)
    bars2 = ax.bar(x + width/2, p75_values, width, label='P75', color=color_p75)
    
    title = f"{title_prefix}\nCycle Time Comparison" if title_prefix else "Cycle Time Comparison"
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_ylabel("Hours", fontsize=12)
    ax.set_xticks(x)
    ax.set_xticklabels([f"{cat}\n({count} repos)" for cat, count in zip(categories, counts)])
    max_val = max(max(p50_values), max(p75_values))
    ax.set_ylim(0, max_val * 1.3 if max_val > 0 else 10)
    ax.legend()
    ax.yaxis.grid(True, linestyle='--', alpha=0.7)
    
    # Add value labels on bars
    def add_labels(bars):
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}h',
                    ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    add_labels(bars1)
    add_labels(bars2)
    
    # Add date range
    fig.text(0.5, 0.01, f"Period: {date_range}", ha='center', fontsize=10, color='gray')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"📊 Chart saved: {output_path}")
    return output_path


def generate_full_comparison_chart(
    india_csv: str,
    other_csv: str,
    output_path: str,
    date_range: str
) -> str:
    """
    Generate a comprehensive comparison chart showing:
    - Paypay India Teams vs Other Teams
    - P50 and P75 cycle times
    - All repos vs Excluding k8s repos
    
    Args:
        india_csv: Path to India teams repo-level CSV
        other_csv: Path to Other teams repo-level CSV
        output_path: Path to save the chart PNG
        date_range: Date range string for chart title
    
    Returns:
        Path to saved chart
    """
    if not CHARTS_AVAILABLE:
        print("⚠️ matplotlib/pandas not available. Install with: pip install matplotlib pandas")
        return ""
    
    import numpy as np
    
    # Calculate stats for all categories
    india_all = calculate_cycle_time_stats(india_csv, exclude_k8s=False)
    india_no_k8s = calculate_cycle_time_stats(india_csv, exclude_k8s=True)
    other_all = calculate_cycle_time_stats(other_csv, exclude_k8s=False)
    other_no_k8s = calculate_cycle_time_stats(other_csv, exclude_k8s=True)
    
    # Convert minutes to hours
    def mins_to_hours(mins):
        return mins / 60
    
    # Create figure with 2 subplots (India and Other)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # Colors
    color_p50 = "#2c5282"
    color_p75 = "#9b2c2c"
    
    # Setup for grouped bars
    categories = ["All Repos", "Excluding k8s"]
    x = np.arange(len(categories))
    width = 0.35
    
    # Chart 1: Paypay India Teams
    india_p50 = [mins_to_hours(india_all["p50_avg"]), mins_to_hours(india_no_k8s["p50_avg"])]
    india_p75 = [mins_to_hours(india_all["p75_avg"]), mins_to_hours(india_no_k8s["p75_avg"])]
    india_counts = [india_all["count"], india_no_k8s["count"]]
    
    bars1_1 = ax1.bar(x - width/2, india_p50, width, label='P50', color=color_p50)
    bars1_2 = ax1.bar(x + width/2, india_p75, width, label='P75', color=color_p75)
    
    ax1.set_title("Paypay India Teams\nCycle Time", fontsize=14, fontweight="bold")
    ax1.set_ylabel("Hours", fontsize=12)
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"{cat}\n({count} repos)" for cat, count in zip(categories, india_counts)])
    india_max = max(max(india_p50), max(india_p75)) if india_p50 and india_p75 else 10
    ax1.set_ylim(0, india_max * 1.4 if india_max > 0 else 10)
    ax1.legend(loc='upper right')
    ax1.yaxis.grid(True, linestyle='--', alpha=0.7)
    
    # Add value labels
    for bar in bars1_1:
        ax1.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                f'{bar.get_height():.1f}h', ha='center', va='bottom', fontsize=10, fontweight='bold')
    for bar in bars1_2:
        ax1.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                f'{bar.get_height():.1f}h', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Chart 2: Other Teams
    other_p50 = [mins_to_hours(other_all["p50_avg"]), mins_to_hours(other_no_k8s["p50_avg"])]
    other_p75 = [mins_to_hours(other_all["p75_avg"]), mins_to_hours(other_no_k8s["p75_avg"])]
    other_counts = [other_all["count"], other_no_k8s["count"]]
    
    bars2_1 = ax2.bar(x - width/2, other_p50, width, label='P50', color=color_p50)
    bars2_2 = ax2.bar(x + width/2, other_p75, width, label='P75', color=color_p75)
    
    ax2.set_title("Other Teams\nCycle Time", fontsize=14, fontweight="bold")
    ax2.set_ylabel("Hours", fontsize=12)
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"{cat}\n({count} repos)" for cat, count in zip(categories, other_counts)])
    other_max = max(max(other_p50), max(other_p75)) if other_p50 and other_p75 else 10
    ax2.set_ylim(0, other_max * 1.4 if other_max > 0 else 10)
    ax2.legend(loc='upper right')
    ax2.yaxis.grid(True, linestyle='--', alpha=0.7)
    
    # Add value labels
    for bar in bars2_1:
        ax2.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                f'{bar.get_height():.1f}h', ha='center', va='bottom', fontsize=10, fontweight='bold')
    for bar in bars2_2:
        ax2.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                f'{bar.get_height():.1f}h', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Main title and date range
    fig.suptitle(f"Cycle Time Comparison: P50 vs P75, All Repos vs Excluding k8s\n{date_range}",
                 fontsize=16, fontweight="bold", y=1.02)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"📊 Chart saved: {output_path}")
    return output_path


# =============================================================================
# MAIN
# =============================================================================

def list_repos_command(api_key: str) -> None:
    """List all repositories and save to files."""
    repos = fetch_all_repositories(api_key)
    display_repositories(repos)
    save_repositories_to_csv(repos)
    save_repositories_to_json(repos)
    print(f"\n✅ Found {len(repos)} repositories")


def main():
    """Main function to fetch cycle time metrics."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fetch cycle time metrics from LinearB API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch cycle time metrics for Paypay India teams (default)
  python fetch_cycle_time.py
  
  # Fetch cycle time metrics for ALL OTHER teams (not under Paypay India)
  python fetch_cycle_time.py --other-teams
  
  # Fetch cycle time metrics grouped by repositories
  python fetch_cycle_time.py --by-repo
  
  # Generate comparison chart (All repos vs Excluding k8s)
  python fetch_cycle_time.py --chart
  
  # List all repositories
  python fetch_cycle_time.py --list-repos
  
  # Set date range via environment variables
  DATE_AFTER='2025-10-01' DATE_BEFORE='2025-12-01' python fetch_cycle_time.py
        """
    )
    parser.add_argument(
        "--list-repos", 
        action="store_true",
        help="List all repositories with their IDs and save to CSV/JSON"
    )
    parser.add_argument(
        "--by-repo",
        action="store_true", 
        help="Group metrics by repository instead of team"
    )
    parser.add_argument(
        "--other-teams",
        action="store_true",
        help="Fetch metrics for all teams NOT under Paypay India"
    )
    parser.add_argument(
        "--chart",
        action="store_true",
        help="Generate comparison charts (All repos vs Excluding k8s repos)"
    )
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Export monthly aggregated data for dashboard (dashboard/data/cycle_time_data.json)"
    )
    
    args = parser.parse_args()
    
    api_key = os.getenv("LINEARB_API_KEY")
    
    if not api_key:
        print("❌ Error: LINEARB_API_KEY environment variable is not set.")
        print("\nTo set it:")
        print("  export LINEARB_API_KEY='your_api_key_here'")
        print("\nOr pass it inline:")
        print("  LINEARB_API_KEY='your_api_key' python fetch_cycle_time.py")
        sys.exit(1)
    
    # Show masked API key
    masked = api_key[:6] + "..." + api_key[-4:] if len(api_key) > 10 else "***"
    print(f"🔑 Using API key: {masked}")
    
    try:
        # Handle --list-repos command
        if args.list_repos:
            list_repos_command(api_key)
            return
        
        # Get date range
        date_after, date_before = get_date_range()
        print(f"📅 Date range: {date_after} to {inclusive_end_date_str(date_before)}")
        date_range_str = f"{date_after} to {inclusive_end_date_str(date_before)}"
        
        # Handle --chart: Generate comparison charts
        if args.chart:
            if not CHARTS_AVAILABLE:
                print("❌ Chart generation requires matplotlib and pandas.")
                print("   Install with: pip install matplotlib pandas")
                sys.exit(1)
            
            print("\n📊 Generating Cycle Time Comparison Charts...")
            print("   This will show P50 and P75 cycle times")
            print("   For both All repos and Excluding k8s repos")
            
            # Fetch all repositories
            print("\n📦 Fetching all repositories...")
            repos = fetch_all_repositories(api_key)
            if not repos:
                print("❌ No repositories found.")
                sys.exit(1)
            
            repo_ids = [r.get("id") for r in repos if r.get("id")]
            repo_name_map = {r.get("id"): r.get("name", "") for r in repos if r.get("id")}
            
            # Export repo-level cycle times for ALL repos (org-wide)
            print(f"\n{'='*60}")
            print("FETCHING CYCLE TIME DATA (P50 & P75)")
            print(f"{'='*60}")
            
            all_repos_csv = "cycle_time_all_repos.csv"
            raw_csv = export_metrics_by_repo(
                api_key, repo_ids, date_after, date_before,
                all_repos_csv.replace(".csv", "_raw.csv"),
                roll_up="custom"
            )
            final_csv = augment_csv_with_dhm(raw_csv, all_repos_csv, repo_name_map=repo_name_map)
            
            # Analyze k8s-related repos
            with open(final_csv, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            k8s_repos = [r for r in rows if is_k8s_related_repo(r.get("Repository Name") or r.get("repository_name") or "")]
            non_k8s_repos = [r for r in rows if not is_k8s_related_repo(r.get("Repository Name") or r.get("repository_name") or "")]
            
            print(f"\n{'='*60}")
            print("K8S/INFRA REPOS ANALYSIS")
            print(f"{'='*60}")
            print(f"   Patterns matched: k8s, kubernetes, kube, helm, argocd,")
            print(f"                     flux, istio, terraform, infra, gitops,")
            print(f"                     deploy, manifest, ingress, envoy")
            print(f"\n   Total repos: {len(rows)}")
            print(f"   K8s/Infra repos: {len(k8s_repos)}")
            print(f"   Application repos: {len(non_k8s_repos)}")
            
            if k8s_repos:
                print(f"\n   K8s/Infra repositories excluded:")
                for r in sorted(k8s_repos, key=lambda x: (x.get("Repository Name") or x.get("repository_name") or "").lower())[:25]:
                    name = r.get("Repository Name") or r.get("repository_name") or "N/A"
                    print(f"      • {name}")
                if len(k8s_repos) > 25:
                    print(f"      ... and {len(k8s_repos) - 25} more")
            
            # Generate the chart
            print(f"\n{'='*60}")
            print("GENERATING CHART")
            print(f"{'='*60}")
            
            chart_path = "cycle_time_comparison_chart.png"
            generate_combined_cycle_time_chart(
                final_csv, 
                chart_path, 
                date_range_str,
                title_prefix="All Organization Repositories"
            )
            
            # Calculate and print summary stats
            stats_all = calculate_cycle_time_stats(final_csv, exclude_k8s=False)
            stats_no_k8s = calculate_cycle_time_stats(final_csv, exclude_k8s=True)
            
            print(f"\n{'='*60}")
            print("CYCLE TIME SUMMARY")
            print(f"{'='*60}")
            print(f"\n   ALL REPOS ({stats_all['count']} repos):")
            print(f"      P50: {stats_all['p50_avg']/60:.1f} hours ({stats_all['p50_avg']:.0f} minutes)")
            print(f"      P75: {stats_all['p75_avg']/60:.1f} hours ({stats_all['p75_avg']:.0f} minutes)")
            print(f"\n   APPLICATION REPOS ONLY ({stats_no_k8s['count']} repos, excluding k8s/infra):")
            print(f"      P50: {stats_no_k8s['p50_avg']/60:.1f} hours ({stats_no_k8s['p50_avg']:.0f} minutes)")
            print(f"      P75: {stats_no_k8s['p75_avg']/60:.1f} hours ({stats_no_k8s['p75_avg']:.0f} minutes)")
            
            # Summary
            print(f"\n{'='*60}")
            print("OUTPUT FILES")
            print(f"{'='*60}")
            print(f"   📊 Chart: {chart_path}")
            print(f"   📄 Data: {final_csv}")
            print(f"   📅 Period: {date_range_str}")
            
            print("\n⚠️  NOTE: Repository data is organization-wide.")
            print("   LinearB API doesn't support filtering repos by team.")
            print("   Use team-level metrics (default command) for team separation.")
            return
        
        # Handle --dashboard: Export monthly aggregated data for dashboard
        if args.dashboard:
            print("\n📊 Exporting monthly cycle time data for dashboard...")
            
            # Get teams under Paypay India at depth 2
            teams = get_teams_under_parent(api_key, TARGET_PARENT_NAME, depth=2)
            
            if not teams:
                print("❌ No teams found under the parent organization.")
                sys.exit(1)
            
            team_names = [t.get("name") for t in teams]
            team_ids = [str(t.get("id") or t.get("_id")) for t in teams]
            
            print(f"   Teams: {len(team_ids)} ({', '.join(team_names[:5])}{'...' if len(team_names) > 5 else ''})")
            
            dashboard_json = export_dashboard_json(
                api_key, team_ids, team_names,
                output_path="dashboard/data/cycle_time_data.json"
            )
            
            # Summary
            print(f"\n{'='*60}")
            print("DASHBOARD EXPORT COMPLETE")
            print(f"{'='*60}")
            print(f"📊 Output: {dashboard_json}")
            print(f"👥 Teams: {len(team_ids)}")
            print(f"\n💡 This data is used by the static dashboard hosted on GitHub Pages")
            return
        
        # Handle --by-repo: Group metrics by repository
        if args.by_repo:
            print("\n📦 Fetching repositories...")
            repos = fetch_all_repositories(api_key)
            
            if not repos:
                print("❌ No repositories found.")
                sys.exit(1)
            
            repo_ids = [r.get("id") for r in repos if r.get("id")]
            # Create mapping of repo_id -> repo_name for CSV enrichment
            repo_name_map = {r.get("id"): r.get("name", "") for r in repos if r.get("id")}
            print(f"✅ Found {len(repo_ids)} repositories")
            
            # Output files for repository grouping
            repo_csv = "cycle_time_by_repo.csv"
            repo_json = "cycle_time_by_repo.json"
            repo_daily_csv = "cycle_time_by_repo_daily.csv"
            
            # Export aggregated metrics by repository
            print(f"\n{'='*60}")
            print("AGGREGATED CYCLE TIME METRICS BY REPOSITORY")
            print(f"{'='*60}")
            
            raw_csv = export_metrics_by_repo(
                api_key, repo_ids, date_after, date_before,
                repo_csv.replace(".csv", "_raw.csv"),
                roll_up="custom"
            )
            
            final_csv = augment_csv_with_dhm(raw_csv, repo_csv, repo_name_map=repo_name_map)
            csv_to_json(final_csv, repo_json)
            display_summary(final_csv, group_by="repo")
            
            # Export daily metrics by repository
            print(f"\n{'='*60}")
            print("DAILY CYCLE TIME METRICS BY REPOSITORY")
            print(f"{'='*60}")
            
            daily_raw_csv = export_metrics_by_repo(
                api_key, repo_ids, date_after, date_before,
                repo_daily_csv.replace(".csv", "_raw.csv"),
                roll_up="1d"
            )
            
            daily_final_csv = augment_csv_with_dhm(daily_raw_csv, repo_daily_csv, repo_name_map=repo_name_map)
            print(f"\n✅ Daily metrics saved to: {repo_daily_csv}")
            
            # Summary
            print(f"\n{'='*60}")
            print("SUMMARY")
            print(f"{'='*60}")
            print(f"📊 Aggregated metrics: {repo_csv}")
            print(f"📊 Daily metrics: {repo_daily_csv}")
            print(f"📊 JSON output: {repo_json}")
            print(f"📅 Date range: {date_after} to {inclusive_end_date_str(date_before)}")
            print(f"📦 Repositories included: {len(repo_ids)}")
            return
        
        # Handle --other-teams: Get teams NOT under Paypay India
        if args.other_teams:
            print("\n📋 Fetching all teams...")
            all_teams = fetch_all_teams(api_key)
            all_teams = compute_depths(all_teams, TARGET_PARENT_NAME)
            
            # Get IDs of teams under Paypay India (to exclude them)
            paypay_india_team_ids = set()
            for t in all_teams:
                if t.get("depth_level_under_parent") is not None:
                    tid = str(t.get("id") or t.get("_id"))
                    paypay_india_team_ids.add(tid)
            
            # Get all other teams (those NOT under Paypay India and have no parent or different parent)
            other_teams = [
                t for t in all_teams
                if str(t.get("id") or t.get("_id")) not in paypay_india_team_ids
            ]
            
            if not other_teams:
                print("❌ No other teams found.")
                sys.exit(1)
            
            team_names = [t.get("name") for t in other_teams]
            team_ids = [str(t.get("id") or t.get("_id")) for t in other_teams]
            
            print(f"✅ Found {len(other_teams)} teams (excluding Paypay India)")
            print(f"\n📋 Teams included:")
            for name in sorted(team_names):
                print(f"   • {name}")
            
            # Output files for other teams
            other_csv = "cycle_time_other_teams.csv"
            other_json = "cycle_time_other_teams.json"
            other_daily_csv = "cycle_time_other_teams_daily.csv"
            
            # Export aggregated metrics
            print(f"\n{'='*60}")
            print("AGGREGATED CYCLE TIME METRICS (Other Teams)")
            print(f"{'='*60}")
            
            raw_csv = export_metrics_by_team(
                api_key, team_ids, date_after, date_before,
                other_csv.replace(".csv", "_raw.csv"),
                roll_up="custom"
            )
            
            final_csv = augment_csv_with_dhm(raw_csv, other_csv)
            csv_to_json(final_csv, other_json)
            display_summary(final_csv, group_by="team")
            
            # Export daily metrics
            print(f"\n{'='*60}")
            print("DAILY CYCLE TIME METRICS (Other Teams)")
            print(f"{'='*60}")
            
            daily_raw_csv = export_metrics_by_team(
                api_key, team_ids, date_after, date_before,
                other_daily_csv.replace(".csv", "_raw.csv"),
                roll_up="1d"
            )
            
            daily_final_csv = augment_csv_with_dhm(daily_raw_csv, other_daily_csv)
            print(f"\n✅ Daily metrics saved to: {other_daily_csv}")
            
            # Summary
            print(f"\n{'='*60}")
            print("SUMMARY")
            print(f"{'='*60}")
            print(f"📊 Aggregated metrics: {other_csv}")
            print(f"📊 Daily metrics: {other_daily_csv}")
            print(f"📊 JSON output: {other_json}")
            print(f"📅 Date range: {date_after} to {inclusive_end_date_str(date_before)}")
            print(f"👥 Teams included: {len(other_teams)} (excluding Paypay India)")
            return
        
        # Default: Group metrics by Paypay India teams
        # Get teams under Paypay India at depth 2
        teams = get_teams_under_parent(api_key, TARGET_PARENT_NAME, depth=2)
        
        if not teams:
            print("❌ No teams found under the parent organization.")
            sys.exit(1)
        
        team_names = [t.get("name") for t in teams]
        team_ids = [str(t.get("id") or t.get("_id")) for t in teams]
        
        print(f"\n📋 Paypay India Teams included:")
        for name in team_names:
            print(f"   • {name}")
        
        # Export aggregated metrics by team
        print(f"\n{'='*60}")
        print("AGGREGATED CYCLE TIME METRICS (Paypay India Teams)")
        print(f"{'='*60}")
        
        raw_csv = export_metrics_by_team(
            api_key, team_ids, date_after, date_before,
            OUTPUT_CSV.replace(".csv", "_raw.csv"),
            roll_up="custom"
        )
        
        final_csv = augment_csv_with_dhm(raw_csv, OUTPUT_CSV)
        csv_to_json(final_csv, OUTPUT_JSON)
        display_summary(final_csv, group_by="team")
        
        # Export daily metrics by team
        print(f"\n{'='*60}")
        print("DAILY CYCLE TIME METRICS (Paypay India Teams)")
        print(f"{'='*60}")
        
        daily_raw_csv = export_metrics_by_team(
            api_key, team_ids, date_after, date_before,
            DAILY_OUTPUT_CSV.replace(".csv", "_raw.csv"),
            roll_up="1d"
        )
        
        daily_final_csv = augment_csv_with_dhm(daily_raw_csv, DAILY_OUTPUT_CSV)
        print(f"\n✅ Daily metrics saved to: {DAILY_OUTPUT_CSV}")
        
        # Summary
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        print(f"📊 Aggregated metrics: {OUTPUT_CSV}")
        print(f"📊 Daily metrics: {DAILY_OUTPUT_CSV}")
        print(f"📊 JSON output: {OUTPUT_JSON}")
        print(f"📅 Date range: {date_after} to {inclusive_end_date_str(date_before)}")
        print(f"👥 Paypay India teams included: {len(teams)}")
        
    except LinearBError as e:
        print(f"❌ LinearB API Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

