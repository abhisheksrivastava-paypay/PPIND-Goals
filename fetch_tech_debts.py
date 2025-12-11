#!/usr/bin/env python3
"""
Fetch Tech Debt Metrics from Jira.

This script calculates Tech Debt counts for each team based on their tech debt epics.
For each team's epic, it counts:
- Issues at the start of the quarter
- Issues resolved during the quarter
- Issues at the end of the quarter

Usage:
    1. Set the JIRA_API_KEY environment variable with your PAT
    2. Update config/tech_debt_epics.json with team epic mappings
    3. Run: python fetch_tech_debts.py

Output:
    - tech_debts_data.json - Tech debt data for dashboard
    - tech_debts_data.csv - CSV export for reference
"""

import os
import sys
import csv
import json
import time
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import quote

# =============================================================================
# CONFIGURATION
# =============================================================================

# Jira Server Configuration
JIRA_SERVER_URL = "https://paypay-corp.rickcloud.jp/jira"
JIRA_API_BASE = f"{JIRA_SERVER_URL}/rest/api/2"

# Config files
CONFIG_EPICS = "config/tech_debt_epics.json"
CONFIG_QUARTERS = "config/quarters.json"

# Output files
OUTPUT_JSON = "dashboard/data/tech_debts_data.json"
OUTPUT_CSV = "tech_debts_data.csv"

# Issue types to count as tech debt
TECH_DEBT_ISSUE_TYPES = ["Story", "Task", "Sub-task", "Bug"]


class JiraError(Exception):
    """Custom exception for Jira API errors."""
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
            
            raise JiraError(f"{response.status_code}: {response.text[:500]}")
        
        except requests.exceptions.RequestException as e:
            if attempt < 4:
                print(f"  ⚠️ Request failed: {e}, retrying in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 16)
                continue
            raise JiraError(f"Request failed after retries: {e}")
    
    raise JiraError("Request failed after maximum retries")


def get_auth_headers(pat: str) -> dict:
    """Get authentication headers for Jira Server with PAT."""
    return {
        "Authorization": f"Bearer {pat}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def load_config(config_path: str) -> Dict:
    """Load a JSON config file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_current_quarter() -> Dict[str, str]:
    """Get the current quarter's date range."""
    today = datetime.now()
    q = (today.month - 1) // 3 + 1
    year = today.year
    
    start_month = (q - 1) * 3 + 1
    end_month = start_month + 3
    
    if end_month > 12:
        end_year = year + 1
        end_month = 1
    else:
        end_year = year
    
    return {
        "name": f"{year} Q{q}",
        "start": f"{year}-{start_month:02d}-01",
        "end": f"{end_year}-{end_month:02d}-01"
    }


# =============================================================================
# JIRA API FUNCTIONS
# =============================================================================

def search_issues_count(pat: str, jql: str) -> int:
    """Get count of issues matching JQL without fetching all data."""
    headers = get_auth_headers(pat)
    
    payload = {
        "jql": jql,
        "maxResults": 0,
        "fields": ["key"],
    }
    
    response = _req("POST", f"{JIRA_API_BASE}/search", headers, json=payload)
    data = response.json()
    
    return data.get("total", 0)


def search_issues(pat: str, jql: str, fields: List[str], max_results: int = 1000) -> List[Dict]:
    """Search for issues using JQL with pagination."""
    headers = get_auth_headers(pat)
    all_issues = []
    start_at = 0
    page_size = 100
    
    while start_at < max_results:
        payload = {
            "jql": jql,
            "startAt": start_at,
            "maxResults": page_size,
            "fields": fields,
        }
        
        response = _req("POST", f"{JIRA_API_BASE}/search", headers, json=payload)
        data = response.json()
        
        issues = data.get("issues", [])
        all_issues.extend(issues)
        
        total = data.get("total", 0)
        
        if start_at + len(issues) >= total or not issues:
            break
        
        start_at += page_size
    
    return all_issues


# =============================================================================
# TECH DEBT CALCULATION
# =============================================================================

def get_tech_debt_counts(pat: str, epic_key: str, quarter: Dict[str, str]) -> Dict[str, int]:
    """
    Get tech debt counts for an epic for a specific quarter.
    
    Returns:
        - start_count: Issues existing at start of quarter (created before quarter start, not resolved before quarter start)
        - resolved_count: Issues resolved during the quarter
        - end_count: Issues existing at end of quarter (created before quarter end, not resolved before quarter end)
        - created_count: Issues created during the quarter
    """
    q_start = quarter["start"]
    q_end = quarter["end"]
    
    # Build issue type filter
    issue_types_str = ", ".join([f'"{t}"' for t in TECH_DEBT_ISSUE_TYPES])
    
    # Base JQL: Issues under this epic (Epic Link or parent)
    base_jql = f'("Epic Link" = {epic_key} OR parent = {epic_key}) AND issuetype IN ({issue_types_str})'
    
    # Count at START of quarter:
    # Created before quarter start AND (not resolved OR resolved after quarter start)
    start_jql = f'{base_jql} AND created < "{q_start}" AND (resolved IS EMPTY OR resolved >= "{q_start}")'
    start_count = search_issues_count(pat, start_jql)
    
    # Count RESOLVED during quarter:
    # Resolved between quarter start (inclusive) and quarter end (exclusive)
    resolved_jql = f'{base_jql} AND resolved >= "{q_start}" AND resolved < "{q_end}"'
    resolved_count = search_issues_count(pat, resolved_jql)
    
    # Count at END of quarter:
    # Created before quarter end AND (not resolved OR resolved after quarter end)
    end_jql = f'{base_jql} AND created < "{q_end}" AND (resolved IS EMPTY OR resolved >= "{q_end}")'
    end_count = search_issues_count(pat, end_jql)
    
    # Count CREATED during quarter:
    created_jql = f'{base_jql} AND created >= "{q_start}" AND created < "{q_end}"'
    created_count = search_issues_count(pat, created_jql)
    
    return {
        "start_count": start_count,
        "resolved_count": resolved_count,
        "end_count": end_count,
        "created_count": created_count,
    }


def process_tech_debts(pat: str, teams_config: List[Dict], quarters: List[Dict]) -> List[Dict]:
    """Process tech debt counts for all teams across all quarters."""
    results = []
    
    for team in teams_config:
        team_name = team["name"]
        epic_key = team["epic_key"]
        
        if not epic_key or epic_key.endswith("XXXXX"):
            print(f"  ⚠️ Skipping {team_name} - epic key not configured")
            continue
        
        print(f"  📊 Processing {team_name} ({epic_key})...")
        
        team_data = {
            "team_name": team_name,
            "epic_key": epic_key,
            "quarters": {}
        }
        
        for quarter in quarters:
            q_name = quarter["name"]
            counts = get_tech_debt_counts(pat, epic_key, quarter)
            team_data["quarters"][q_name] = counts
            
            print(f"      {q_name}: start={counts['start_count']}, resolved={counts['resolved_count']}, created={counts['created_count']}, end={counts['end_count']}")
        
        results.append(team_data)
    
    return results


def calculate_summary(results: List[Dict], quarters: List[Dict]) -> Dict[str, Any]:
    """Calculate summary statistics across all teams."""
    summary = {
        "total_teams": len(results),
        "by_quarter": {}
    }
    
    for quarter in quarters:
        q_name = quarter["name"]
        total_start = 0
        total_resolved = 0
        total_created = 0
        total_end = 0
        
        for team in results:
            q_data = team.get("quarters", {}).get(q_name, {})
            total_start += q_data.get("start_count", 0)
            total_resolved += q_data.get("resolved_count", 0)
            total_created += q_data.get("created_count", 0)
            total_end += q_data.get("end_count", 0)
        
        summary["by_quarter"][q_name] = {
            "total_start": total_start,
            "total_resolved": total_resolved,
            "total_created": total_created,
            "total_end": total_end,
            "net_change": total_created - total_resolved,
        }
    
    return summary


def save_results(results: List[Dict], summary: Dict, quarters: List[Dict], output_json: str, output_csv: str):
    """Save results to JSON and CSV files."""
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_json) or ".", exist_ok=True)
    
    # Prepare JSON output
    output_data = {
        "generated_at": datetime.now().isoformat(),
        "summary": summary,
        "quarters": [q["name"] for q in quarters],
        "teams": results,
    }
    
    # Save JSON
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    print(f"✅ JSON saved to: {output_json}")
    
    # Prepare CSV (flattened format)
    csv_rows = []
    for team in results:
        for q_name, q_data in team.get("quarters", {}).items():
            csv_rows.append({
                "team_name": team["team_name"],
                "epic_key": team["epic_key"],
                "quarter": q_name,
                "start_count": q_data.get("start_count", 0),
                "resolved_count": q_data.get("resolved_count", 0),
                "created_count": q_data.get("created_count", 0),
                "end_count": q_data.get("end_count", 0),
            })
    
    # Save CSV
    if csv_rows:
        fieldnames = ["team_name", "epic_key", "quarter", "start_count", "resolved_count", "created_count", "end_count"]
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)
        print(f"✅ CSV saved to: {output_csv}")


def main():
    """Main entry point."""
    pat = os.getenv("JIRA_API_KEY")
    if not pat:
        print("❌ Error: JIRA_API_KEY environment variable not set")
        print("   Set it with: export JIRA_API_KEY='your-personal-access-token'")
        sys.exit(1)
    
    print("=" * 60)
    print("📊 Tech Debt Metrics Calculator")
    print("=" * 60)
    
    # Load configuration
    print("\n📁 Loading configuration...")
    
    try:
        epics_config = load_config(CONFIG_EPICS)
        teams = epics_config.get("teams", [])
        print(f"   Found {len(teams)} teams in config")
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print(f"   Please create {CONFIG_EPICS} with team-epic mappings")
        sys.exit(1)
    
    try:
        quarters_config = load_config(CONFIG_QUARTERS)
        quarters = quarters_config.get("quarters", [])
        print(f"   Found {len(quarters)} quarters in config")
    except FileNotFoundError:
        print(f"   ⚠️ {CONFIG_QUARTERS} not found, using current quarter only")
        quarters = [get_current_quarter()]
    
    # Filter to relevant quarters (current and recent past)
    today = datetime.now()
    relevant_quarters = []
    for q in quarters:
        q_end = datetime.strptime(q["end"], "%Y-%m-%d")
        # Include quarters that haven't ended yet or ended within last 6 months
        if q_end > today - timedelta(days=180):
            relevant_quarters.append(q)
    
    if not relevant_quarters:
        relevant_quarters = quarters[-4:]  # Last 4 quarters if all are old
    
    print(f"   Processing quarters: {[q['name'] for q in relevant_quarters]}")
    
    # Process tech debts
    print("\n🔍 Fetching Tech Debt metrics...")
    results = process_tech_debts(pat, teams, relevant_quarters)
    
    if not results:
        print("⚠️ No teams processed (check epic key configuration)")
        save_results([], {"total_teams": 0, "by_quarter": {}}, relevant_quarters, OUTPUT_JSON, OUTPUT_CSV)
        return
    
    # Calculate summary
    summary = calculate_summary(results, relevant_quarters)
    
    # Print summary
    print("\n" + "=" * 60)
    print("📈 SUMMARY")
    print("=" * 60)
    print(f"Total Teams: {summary['total_teams']}")
    
    for q_name, q_stats in summary["by_quarter"].items():
        print(f"\n{q_name}:")
        print(f"   Start of Quarter: {q_stats['total_start']} tickets")
        print(f"   Created: +{q_stats['total_created']}")
        print(f"   Resolved: -{q_stats['total_resolved']}")
        print(f"   End of Quarter: {q_stats['total_end']} tickets")
        print(f"   Net Change: {q_stats['net_change']:+d}")
    
    # Save results
    save_results(results, summary, relevant_quarters, OUTPUT_JSON, OUTPUT_CSV)
    
    print("\n✅ Tech Debt metrics calculation complete!")


if __name__ == "__main__":
    main()

