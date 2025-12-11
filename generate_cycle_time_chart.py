#!/usr/bin/env python3
"""
Generate Monthly Cycle Time (P50) Charts for LinearB Teams.

This script generates:
1. Individual Cycle Time charts for each team (separate PNG per team)
2. A consolidated PPIND Cycle Time chart (average across all teams)

Metrics:
- Cycle Time (P50) = Coding Time (P50) + Pickup Time (P50) + Review Time (P50)
- PRs Created (pr.new) - Number of PRs opened
- PRs Merged (pr.merged) - Number of PRs merged

Usage:
    python generate_cycle_time_chart.py --start-month 2024-01 --end-month 2024-06

Environment Variables:
    LINEARB_API_KEY: Your LinearB API key (required)

Output:
    - {TeamName}_Cycle_Time.png - Individual chart per team (with PR metrics)
    - PPIND_Cycle_Time.png - Consolidated PPIND-level chart (with PR totals)
    - cycle_time_monthly_data.csv - Raw data used for charts

API Reference: https://docs.linearb.io/api-measurements-v2/
"""

import os
import sys
import time
import csv
import json
import argparse
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple
from collections import deque
from calendar import monthrange

# Chart libraries
try:
    import pandas as pd
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
    CHARTS_AVAILABLE = True
except ImportError:
    CHARTS_AVAILABLE = False
    print("⚠️ matplotlib/pandas not available. Install with: pip install matplotlib pandas")

# =============================================================================
# CONFIGURATION
# =============================================================================

LINEARB_BASE = "https://public-api.linearb.io"
TEAMS_ENDPOINT = f"{LINEARB_BASE}/api/v2/teams"
EXPORT_ENDPOINT = f"{LINEARB_BASE}/api/v2/measurements/export"

TARGET_PARENT_NAME = "Paypay India"
N_TEAMS = 10

# Teams to exclude (case-insensitive)
EXCLUDED_TEAM_NAMES = {"payments qa", "paypay india merchant & finance qa", "sdet"}
EXCLUDED_IDS_FILE = "excluded_team_ids.json"

# Output directory for charts
OUTPUT_DIR = "charts"


class LinearBError(Exception):
    """Custom exception for LinearB API errors."""
    pass


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _req(method: str, url: str, headers: dict, **kwargs) -> requests.Response:
    """Make an HTTP request with retry logic for transient errors."""
    backoff = 1.0
    timeout = kwargs.pop("timeout", 30)
    
    for attempt in range(5):
        try:
            response = requests.request(method, url, headers=headers, timeout=timeout, **kwargs)
            
            if response.status_code < 400:
                return response
            
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


def minutes_to_dh(minutes: Optional[float]) -> str:
    """Convert minutes to days/hours format (no minutes) for cleaner bar labels."""
    try:
        total = int(round(float(minutes)))
    except (TypeError, ValueError):
        return ""
    
    d, rem = divmod(total, 1440)  # 1440 minutes in a day
    h, _ = divmod(rem, 60)
    
    parts = []
    if d:
        parts.append(f"{d}d")
    parts.append(f"{h}h")
    
    return " ".join(parts)


def parse_month(month_str: str) -> Tuple[int, int]:
    """Parse YYYY-MM string to (year, month) tuple."""
    try:
        dt = datetime.strptime(month_str, "%Y-%m")
        return dt.year, dt.month
    except ValueError:
        raise ValueError(f"Invalid month format: {month_str}. Expected YYYY-MM (e.g., 2024-01)")


def get_month_date_range(year: int, month: int) -> Tuple[str, str]:
    """
    Get ISO date range for a given month.
    Returns (first_day, day_after_last) for LinearB API (exclusive end).
    """
    first_day = f"{year:04d}-{month:02d}-01"
    _, last_day_num = monthrange(year, month)
    
    # Calculate the day after the last day of the month
    if month == 12:
        next_month_first = f"{year + 1:04d}-01-01"
    else:
        next_month_first = f"{year:04d}-{month + 1:02d}-01"
    
    return first_day, next_month_first


def get_months_in_range(start_year: int, start_month: int, end_year: int, end_month: int) -> List[Tuple[int, int]]:
    """Get list of (year, month) tuples between start and end (inclusive)."""
    months = []
    year, month = start_year, start_month
    
    while (year, month) <= (end_year, end_month):
        months.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    
    return months


def month_label(year: int, month: int) -> str:
    """Get human-readable month label (e.g., 'Jan 2024')."""
    return datetime(year, month, 1).strftime("%b %Y")


def month_label_short(year: int, month: int) -> str:
    """Get short month label for chart titles (e.g., 'Jan-2024')."""
    return datetime(year, month, 1).strftime("%b-%Y")


def sanitize_filename(name: str) -> str:
    """Sanitize team name for use in filename."""
    # Replace spaces and special characters with underscores
    invalid_chars = '<>:"/\\|?*& '
    result = name
    for char in invalid_chars:
        result = result.replace(char, '_')
    # Remove consecutive underscores
    while '__' in result:
        result = result.replace('__', '_')
    return result.strip('_')


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


def _build_indexes(teams: List[Dict]) -> Tuple[Dict, Dict]:
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
    """Get teams at specified depth under the parent organization."""
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
    
    # Sort alphabetically and limit
    filtered = sorted(filtered, key=lambda t: (t.get("name") or "").lower())[:N_TEAMS]
    
    print(f"✅ Found {len(filtered)} teams at depth {depth} under '{parent_name}'")
    return filtered


# =============================================================================
# METRICS EXPORT
# =============================================================================

def export_metrics_for_month(
    api_key: str,
    team_ids: List[str],
    year: int,
    month: int
) -> Dict[str, Dict[str, float]]:
    """
    Export cycle time metrics for a single month.
    
    Returns:
        Dict mapping team_id -> {coding, pickup, review, cycle_time, prs_created, prs_merged}
    """
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    date_after, date_before = get_month_date_range(year, month)
    
    payload = {
        "group_by": "team",
        "roll_up": "custom",
        "team_ids": [int(tid) for tid in team_ids],
        "requested_metrics": [
            {"name": "branch.time_to_pr", "agg": "p50"},
            {"name": "branch.time_to_review", "agg": "p50"},
            {"name": "branch.review_time", "agg": "p50"},
            {"name": "pr.new"},      # PRs Created (count metric, no aggregation needed)
            {"name": "pr.merged"},   # PRs Merged (count metric, no aggregation needed)
        ],
        "time_ranges": [{"after": date_after, "before": date_before}],
        "limit": len(team_ids)
    }
    
    response = _req("POST", EXPORT_ENDPOINT, headers, params={"file_format": "csv"}, json=payload)
    
    if response.status_code == 204:
        print(f"    ⚠️ No data for {month_label(year, month)}")
        return {}
    
    report_url = response.json().get("report_url")
    if not report_url:
        print(f"    ⚠️ No report URL for {month_label(year, month)}")
        return {}
    
    # Download and parse CSV
    csv_data = _req("GET", report_url, headers={}).content.decode("utf-8")
    lines = csv_data.strip().split("\n")
    
    if len(lines) < 2:
        return {}
    
    # Parse CSV
    reader = csv.DictReader(lines)
    results = {}
    
    for row in reader:
        team_id = row.get("team_id", "")
        team_name = row.get("team_name", "")
        
        coding = float(row.get("branch.time_to_pr:p50", 0) or 0)
        pickup = float(row.get("branch.time_to_review:p50", 0) or 0)
        review = float(row.get("branch.review_time:p50", 0) or 0)
        prs_created = int(float(row.get("pr.new", 0) or 0))
        prs_merged = int(float(row.get("pr.merged", 0) or 0))
        
        cycle_time = coding + pickup + review
        
        results[team_id] = {
            "team_name": team_name,
            "coding": coding,
            "pickup": pickup,
            "review": review,
            "cycle_time": cycle_time,
            "prs_created": prs_created,
            "prs_merged": prs_merged
        }
    
    return results


def fetch_all_monthly_data(
    api_key: str,
    team_ids: List[str],
    team_names: Dict[str, str],
    months: List[Tuple[int, int]]
) -> pd.DataFrame:
    """
    Fetch cycle time data for all teams across all months.
    
    Returns:
        DataFrame with columns: team_id, team_name, year, month, month_label, 
                               coding, pickup, review, cycle_time, prs_created, prs_merged
    """
    all_data = []
    
    print(f"\n📊 Fetching metrics for {len(months)} months...")
    
    for year, month in months:
        print(f"  📅 Fetching {month_label(year, month)}...")
        
        month_data = export_metrics_for_month(api_key, team_ids, year, month)
        
        for team_id, metrics in month_data.items():
            all_data.append({
                "team_id": team_id,
                "team_name": metrics["team_name"] or team_names.get(team_id, "Unknown"),
                "year": year,
                "month": month,
                "month_label": month_label(year, month),
                "coding": metrics["coding"],
                "pickup": metrics["pickup"],
                "review": metrics["review"],
                "cycle_time": metrics["cycle_time"],
                "prs_created": metrics["prs_created"],
                "prs_merged": metrics["prs_merged"]
            })
    
    df = pd.DataFrame(all_data)
    
    # Sort by team name and date
    if not df.empty:
        df = df.sort_values(["team_name", "year", "month"])
    
    return df


# =============================================================================
# CHART GENERATION
# =============================================================================

def generate_team_chart(
    df: pd.DataFrame,
    team_name: str,
    output_dir: str,
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int
) -> str:
    """
    Generate a Cycle Time chart for a single team.
    Includes Cycle Time (bars) and PR metrics (lines on secondary y-axis).
    
    Returns:
        Path to the generated PNG file
    """
    team_df = df[df["team_name"] == team_name].sort_values(["year", "month"])
    
    if team_df.empty:
        print(f"    ⚠️ No data for team: {team_name}")
        return ""
    
    # Format date range for title (e.g., "Jul-2025 to Nov-2025")
    date_range_str = f"{month_label_short(start_year, start_month)} to {month_label_short(end_year, end_month)}"
    
    # Create figure with dual y-axis
    fig, ax = plt.subplots(figsize=(14, 7))
    ax2 = ax.twinx()
    
    # Bar chart for Cycle Time
    x = list(range(len(team_df)))
    bars = ax.bar(x, team_df["cycle_time"], color="#2c5282", width=0.6, label="Cycle Time (P50)", zorder=2)
    
    # Line charts for PR metrics on secondary axis
    line_created = ax2.plot(x, team_df["prs_created"], color="#c05621", marker="o", 
                            markersize=6, linewidth=2.5, label="PRs Created", zorder=3)
    line_merged = ax2.plot(x, team_df["prs_merged"], color="#276749", marker="s", 
                           markersize=6, linewidth=2.5, label="PRs Merged", zorder=3)
    
    # X-axis labels
    ax.set_xticks(x)
    ax.set_xticklabels(team_df["month_label"], rotation=45, ha="right")
    
    # Left Y-axis formatting (Cycle Time in dhm)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: minutes_to_dhm(y)))
    ax.set_ylabel("Cycle Time (P50)", fontsize=11, color="#2c5282")
    ax.tick_params(axis="y", labelcolor="#2c5282")
    
    # Right Y-axis for PR counts
    ax2.set_ylabel("PR Count", fontsize=11, color="#5c2c00")
    ax2.tick_params(axis="y", labelcolor="#5c2c00")
    pr_max = max(team_df["prs_created"].max(), team_df["prs_merged"].max(), 1)
    ax2.set_ylim(0, pr_max * 1.3)
    
    # Grid
    ax.yaxis.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)
    ax.set_axisbelow(True)
    
    # Add value labels on bars (days and hours only for readability)
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2.,
                height,
                minutes_to_dh(height),
                ha="center",
                va="bottom",
                fontsize=8,
                fontweight="bold",
                color="#2c5282"
            )
    
    # Add PR count labels
    for i, (created, merged) in enumerate(zip(team_df["prs_created"], team_df["prs_merged"])):
        # PRs Created label (above the point)
        ax2.text(i, created + pr_max * 0.03, str(int(created)), 
                 ha="center", va="bottom", fontsize=8, color="#c05621", fontweight="bold")
        # PRs Merged label (below the point)
        ax2.text(i, merged - pr_max * 0.06, str(int(merged)), 
                 ha="center", va="top", fontsize=8, color="#276749", fontweight="bold")
    
    # Title
    ax.set_title(
        f"{team_name} - Cycle Time (P50) & PR Metrics\n{date_range_str}",
        fontsize=14,
        fontweight="bold",
        pad=15
    )
    
    # Combined legend
    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=9)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save
    filename = f"{sanitize_filename(team_name)}_Cycle_Time.png"
    filepath = os.path.join(output_dir, filename)
    plt.savefig(filepath, dpi=150, bbox_inches="tight")
    plt.close()
    
    print(f"    📊 Generated: {filename}")
    return filepath


def generate_ppind_chart(
    df: pd.DataFrame,
    output_dir: str,
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int
) -> str:
    """
    Generate the consolidated PPIND Cycle Time chart.
    
    For each month:
    - Cycle Time: Average of all teams' Cycle Times
    - PRs Created: Sum of all teams' PRs Created
    - PRs Merged: Sum of all teams' PRs Merged
    
    Returns:
        Path to the generated PNG file
    """
    if df.empty:
        print("    ⚠️ No data for PPIND chart")
        return ""
    
    # Format date range for title (e.g., "Jul-2025 to Nov-2025")
    date_range_str = f"{month_label_short(start_year, start_month)} to {month_label_short(end_year, end_month)}"
    
    # Calculate aggregates per month across all teams
    monthly_agg = df.groupby(["year", "month", "month_label"]).agg({
        "cycle_time": "mean",      # Average cycle time
        "prs_created": "sum",      # Total PRs created
        "prs_merged": "sum"        # Total PRs merged
    }).reset_index()
    
    monthly_agg = monthly_agg.sort_values(["year", "month"])
    
    # Create figure with dual y-axis
    fig, ax = plt.subplots(figsize=(14, 7))
    ax2 = ax.twinx()
    
    # Bar chart for Cycle Time
    x = list(range(len(monthly_agg)))
    bars = ax.bar(x, monthly_agg["cycle_time"], color="#9b2c2c", width=0.6, 
                  label="Avg Cycle Time (P50)", zorder=2)
    
    # Line charts for PR metrics on secondary axis
    line_created = ax2.plot(x, monthly_agg["prs_created"], color="#c05621", marker="o", 
                            markersize=6, linewidth=2.5, label="Total PRs Created", zorder=3)
    line_merged = ax2.plot(x, monthly_agg["prs_merged"], color="#276749", marker="s", 
                           markersize=6, linewidth=2.5, label="Total PRs Merged", zorder=3)
    
    # X-axis labels
    ax.set_xticks(x)
    ax.set_xticklabels(monthly_agg["month_label"], rotation=45, ha="right")
    
    # Left Y-axis formatting (Cycle Time in dhm)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: minutes_to_dhm(y)))
    ax.set_ylabel("Avg Cycle Time (P50)", fontsize=11, color="#9b2c2c")
    ax.tick_params(axis="y", labelcolor="#9b2c2c")
    
    # Right Y-axis for PR counts
    ax2.set_ylabel("Total PR Count", fontsize=11, color="#5c2c00")
    ax2.tick_params(axis="y", labelcolor="#5c2c00")
    pr_max = max(monthly_agg["prs_created"].max(), monthly_agg["prs_merged"].max(), 1)
    ax2.set_ylim(0, pr_max * 1.3)
    
    # Grid
    ax.yaxis.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)
    ax.set_axisbelow(True)
    
    # Add value labels on bars (days and hours only for readability)
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            ax.text(
                bar.get_x() + bar.get_width() / 2.,
                height,
                minutes_to_dh(height),
                ha="center",
                va="bottom",
                fontsize=8,
                fontweight="bold",
                color="#9b2c2c"
            )
    
    # Add PR count labels
    for i, (created, merged) in enumerate(zip(monthly_agg["prs_created"], monthly_agg["prs_merged"])):
        # PRs Created label (above the point)
        ax2.text(i, created + pr_max * 0.03, str(int(created)), 
                 ha="center", va="bottom", fontsize=8, color="#c05621", fontweight="bold")
        # PRs Merged label (below the point)
        ax2.text(i, merged - pr_max * 0.06, str(int(merged)), 
                 ha="center", va="top", fontsize=8, color="#276749", fontweight="bold")
    
    # Title
    num_teams = df["team_name"].nunique()
    ax.set_title(
        f"PPIND Cycle Time (P50) & PR Metrics - {num_teams} Teams\n{date_range_str}",
        fontsize=14,
        fontweight="bold",
        pad=15
    )
    
    # Combined legend
    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=9)
    
    # Add subtitle explaining the calculation
    fig.text(
        0.5, 0.01,
        "Cycle Time = Avg(Coding + Pickup + Review) | PRs = Sum across all teams",
        ha="center",
        fontsize=9,
        color="gray",
        style="italic"
    )
    
    # Adjust layout
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.12)
    
    # Save
    filename = "PPIND_Cycle_Time.png"
    filepath = os.path.join(output_dir, filename)
    plt.savefig(filepath, dpi=150, bbox_inches="tight")
    plt.close()
    
    print(f"    📊 Generated: {filename}")
    return filepath


def save_data_to_csv(df: pd.DataFrame, output_dir: str) -> str:
    """Save the raw data to a CSV file."""
    if df.empty:
        print("    ⚠️ No data to save")
        return ""
    
    # Add human-readable columns
    df_out = df.copy()
    df_out["coding_dhm"] = df_out["coding"].apply(minutes_to_dhm)
    df_out["pickup_dhm"] = df_out["pickup"].apply(minutes_to_dhm)
    df_out["review_dhm"] = df_out["review"].apply(minutes_to_dhm)
    df_out["cycle_time_dhm"] = df_out["cycle_time"].apply(minutes_to_dhm)
    
    # Reorder columns
    columns = [
        "team_name", "month_label", "year", "month",
        "coding", "coding_dhm",
        "pickup", "pickup_dhm",
        "review", "review_dhm",
        "cycle_time", "cycle_time_dhm",
        "prs_created", "prs_merged",
        "team_id"
    ]
    df_out = df_out[columns]
    
    # Rename columns for readability
    df_out = df_out.rename(columns={
        "team_name": "Team Name",
        "month_label": "Month",
        "year": "Year",
        "month": "Month Num",
        "coding": "Coding Time (P50) mins",
        "coding_dhm": "Coding Time (P50)",
        "pickup": "Pickup Time (P50) mins",
        "pickup_dhm": "Pickup Time (P50)",
        "review": "Review Time (P50) mins",
        "review_dhm": "Review Time (P50)",
        "cycle_time": "Cycle Time (P50) mins",
        "cycle_time_dhm": "Cycle Time (P50)",
        "prs_created": "PRs Created",
        "prs_merged": "PRs Merged",
        "team_id": "Team ID"
    })
    
    filepath = os.path.join(output_dir, "cycle_time_monthly_data.csv")
    df_out.to_csv(filepath, index=False)
    
    print(f"    📄 Saved: cycle_time_monthly_data.csv")
    return filepath


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate Monthly Cycle Time (P50) Charts for LinearB Teams",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate charts for Jan 2024 to Jun 2024
  python generate_cycle_time_chart.py --start-month 2024-01 --end-month 2024-06
  
  # Generate charts for a single month
  python generate_cycle_time_chart.py --start-month 2024-11 --end-month 2024-11
  
  # Specify output directory
  python generate_cycle_time_chart.py --start-month 2024-01 --end-month 2024-06 --output-dir ./my-charts

Environment Variables:
  LINEARB_API_KEY: Your LinearB API key (required)
        """
    )
    
    parser.add_argument(
        "--start-month",
        required=True,
        help="Start month in YYYY-MM format (e.g., 2024-01)"
    )
    
    parser.add_argument(
        "--end-month",
        required=True,
        help="End month in YYYY-MM format (e.g., 2024-06)"
    )
    
    parser.add_argument(
        "--output-dir",
        default=OUTPUT_DIR,
        help=f"Output directory for charts (default: {OUTPUT_DIR})"
    )
    
    args = parser.parse_args()
    
    # Check for charts library
    if not CHARTS_AVAILABLE:
        print("❌ Error: matplotlib and pandas are required.")
        print("   Install with: pip install matplotlib pandas")
        sys.exit(1)
    
    # Validate API key
    api_key = os.getenv("LINEARB_API_KEY")
    if not api_key:
        print("❌ Error: LINEARB_API_KEY environment variable is not set.")
        print("\nTo set it:")
        print("  export LINEARB_API_KEY='your_api_key_here'")
        sys.exit(1)
    
    # Show masked API key
    masked = api_key[:6] + "..." + api_key[-4:] if len(api_key) > 10 else "***"
    print(f"🔑 Using API key: {masked}")
    
    # Parse and validate months
    try:
        start_year, start_month = parse_month(args.start_month)
        end_year, end_month = parse_month(args.end_month)
    except ValueError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    
    if (end_year, end_month) < (start_year, start_month):
        print("❌ Error: End month must be on or after start month")
        sys.exit(1)
    
    # Get list of months
    months = get_months_in_range(start_year, start_month, end_year, end_month)
    start_label = month_label(start_year, start_month)
    end_label = month_label(end_year, end_month)
    
    print(f"\n📅 Date range: {start_label} to {end_label} ({len(months)} months)")
    
    # Create output directory
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)
    print(f"📁 Output directory: {output_dir}")
    
    try:
        # Get teams
        teams = get_teams_under_parent(api_key, TARGET_PARENT_NAME, depth=2)
        
        if not teams:
            print("❌ No teams found under the parent organization.")
            sys.exit(1)
        
        team_names_list = [t.get("name") for t in teams]
        team_ids = [str(t.get("id") or t.get("_id")) for t in teams]
        team_names_map = {str(t.get("id") or t.get("_id")): t.get("name") for t in teams}
        
        print(f"\n📋 Teams included ({len(teams)}):")
        for name in team_names_list:
            print(f"   • {name}")
        
        # Fetch all data
        df = fetch_all_monthly_data(api_key, team_ids, team_names_map, months)
        
        if df.empty:
            print("\n❌ No data available for the specified date range.")
            sys.exit(1)
        
        print(f"\n✅ Fetched {len(df)} data points")
        
        # Save raw data to CSV
        print(f"\n📄 Saving data...")
        save_data_to_csv(df, output_dir)
        
        # Generate individual team charts
        print(f"\n📊 Generating individual team charts...")
        generated_charts = []
        
        for team_name in sorted(team_names_list):
            filepath = generate_team_chart(
                df, team_name, output_dir,
                start_year, start_month, end_year, end_month
            )
            if filepath:
                generated_charts.append(filepath)
        
        # Generate PPIND consolidated chart
        print(f"\n📊 Generating PPIND consolidated chart...")
        ppind_path = generate_ppind_chart(
            df, output_dir,
            start_year, start_month, end_year, end_month
        )
        if ppind_path:
            generated_charts.append(ppind_path)
        
        # Summary
        print(f"\n{'=' * 60}")
        print("SUMMARY")
        print(f"{'=' * 60}")
        print(f"📅 Date range: {start_label} to {end_label}")
        print(f"👥 Teams: {len(team_names_list)}")
        print(f"📊 Charts generated: {len(generated_charts)}")
        print(f"📁 Output directory: {os.path.abspath(output_dir)}")
        print(f"\nGenerated files:")
        for chart in generated_charts:
            print(f"   • {os.path.basename(chart)}")
        print(f"   • cycle_time_monthly_data.csv")
        
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

