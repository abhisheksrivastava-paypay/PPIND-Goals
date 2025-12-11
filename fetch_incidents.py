#!/usr/bin/env python3
"""
Fetch Incident Metrics from Jira.

This script counts incidents for each team per quarter from the PAYPAY Jira project.
Incidents are identified by issue type (typically "Incident" or "Bug" with specific labels).

Usage:
    1. Set the JIRA_API_KEY environment variable with your PAT
    2. Run: python fetch_incidents.py

Environment Variables:
    - JIRA_API_KEY: Personal Access Token for Jira (required)
    - INCIDENT_JQL: Custom JQL query for incidents (optional override)

Output:
    - incidents_data.json - Incident data for dashboard
    - incidents_data.csv - CSV export for reference
"""

import os
import sys
import csv
import json
import time
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from collections import defaultdict

# =============================================================================
# CONFIGURATION
# =============================================================================

# Jira Server Configuration
JIRA_SERVER_URL = "https://paypay-corp.rickcloud.jp/jira"
JIRA_API_BASE = f"{JIRA_SERVER_URL}/rest/api/2"

# Default JQL for incidents - adjust as needed
# This query looks for issues of type "Incident" or "Bug" with incident-related labels
DEFAULT_INCIDENT_JQL = (
    'project = PAYPAY AND '
    '(issuetype = Incident OR (issuetype = Bug AND labels in (incident, production-incident, P0, P1)))'
)

# Config files
CONFIG_QUARTERS = "config/quarters.json"

# Output files
OUTPUT_JSON = "dashboard/data/incidents_data.json"
OUTPUT_CSV = "incidents_data.csv"

# Custom field for team assignment (adjust if your Jira uses different field)
FIELD_TECH_TEAM = "customfield_16028"

# Team name mapping (Jira team field value -> display name)
# This maps the Jira custom field values to cleaner team names for display
TEAM_NAME_MAPPING = {
    "Utility_PPIND Point": "Point",
    "Utility_PPIND GenAI": "GenAI Solutions",
    "FS_PPIND Financial Solutions": "Factoring",
    "Utility_PPIND Gift Voucher": "Gift Voucher",
    "Utility_PPIND Mobile": "Front End App Team",
    "Utility_PPIND PP4B": "Payroll & External PSP",
    "Utility_PPIND Web": "Front End Web Team",
    "Utility_Infrastructure": "Infrastructure",
    "Utility_PPIND Notification": "Notification",
    "O2O_PPIND Gift Voucher Reward Engine": "Gift Voucher Reward Engine",
    "Utility_PPIND Merchant Intelligence": "Merchant Intelligence",
    "Utility_PPIND Notification Delivery": "Notification Delivery",
    "Utility_PPIND Notification Platform": "Notification Platform",
    "Utility_PPIND Websocket BE": "Websocket BE",
    "Utility_PPIND Risk": "Risk Team",
}


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
        return {}
    
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_quarter_for_date(date_str: str, quarters: List[Dict]) -> Optional[str]:
    """Determine which quarter a date falls into."""
    if not date_str:
        return None
    
    try:
        date = datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None
    
    for q in quarters:
        q_start = datetime.strptime(q["start"], "%Y-%m-%d")
        q_end = datetime.strptime(q["end"], "%Y-%m-%d")
        
        if q_start <= date < q_end:
            return q["name"]
    
    # If not in defined quarters, calculate dynamically
    quarter_num = (date.month - 1) // 3 + 1
    return f"{date.year} Q{quarter_num}"


def get_default_quarters() -> List[Dict]:
    """Generate default quarters for the current and recent past years."""
    today = datetime.now()
    quarters = []
    
    # Generate quarters for current year and previous year
    for year in [today.year - 1, today.year]:
        for q in range(1, 5):
            start_month = (q - 1) * 3 + 1
            end_month = start_month + 3
            
            if end_month > 12:
                end_year = year + 1
                end_month = 1
            else:
                end_year = year
            
            quarters.append({
                "name": f"{year} Q{q}",
                "start": f"{year}-{start_month:02d}-01",
                "end": f"{end_year}-{end_month:02d}-01"
            })
    
    return quarters


# =============================================================================
# JIRA API FUNCTIONS
# =============================================================================

def search_issues(pat: str, jql: str, fields: List[str], max_results: int = 5000) -> List[Dict]:
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
        print(f"   Fetched {len(all_issues)}/{total} issues...")
        
        if start_at + len(issues) >= total or not issues:
            break
        
        start_at += page_size
    
    return all_issues


def get_custom_field_value(issue: Dict, field_id: str) -> Optional[str]:
    """Get a custom field value from an issue."""
    fields = issue.get("fields", {})
    value = fields.get(field_id)
    
    if value is None:
        return None
    
    if isinstance(value, str):
        return value
    
    if isinstance(value, dict):
        return value.get("value") or value.get("name") or str(value)
    
    if isinstance(value, list):
        values = []
        for v in value:
            if isinstance(v, dict):
                values.append(v.get("value") or v.get("name") or str(v))
            else:
                values.append(str(v))
        return ", ".join(values) if values else None
    
    return str(value)


def format_date(date_str: Optional[str]) -> str:
    """Format a Jira date string to YYYY-MM-DD."""
    if not date_str:
        return ""
    try:
        if "T" in date_str:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d")
        return date_str[:10] if len(date_str) >= 10 else date_str
    except (ValueError, TypeError):
        return date_str or ""


# =============================================================================
# INCIDENT PROCESSING
# =============================================================================

def process_incidents(pat: str, jql: str, quarters: List[Dict]) -> Dict[str, Any]:
    """
    Fetch and process incident data, grouping by team and quarter.
    
    Returns:
        Dictionary with incident counts by team and quarter
    """
    print(f"🔍 Searching for incidents...")
    print(f"   JQL: {jql[:100]}...")
    
    # Fields to retrieve
    fields = [
        "key",
        "summary",
        "status",
        "created",
        "resolutiondate",
        "priority",
        "labels",
        FIELD_TECH_TEAM,
    ]
    
    issues = search_issues(pat, jql, fields)
    print(f"✅ Found {len(issues)} incidents")
    
    if not issues:
        return {"teams": {}, "by_quarter": {}, "issues": []}
    
    # Process each issue
    incidents_by_team_quarter = defaultdict(lambda: defaultdict(list))
    all_incidents = []
    
    for issue in issues:
        issue_fields = issue.get("fields", {})
        
        # Get team
        team_raw = get_custom_field_value(issue, FIELD_TECH_TEAM)
        team_name = TEAM_NAME_MAPPING.get(team_raw, team_raw) if team_raw else "Unassigned"
        
        # Get created date and determine quarter
        created_date = format_date(issue_fields.get("created"))
        quarter = get_quarter_for_date(created_date, quarters)
        
        # Get priority
        priority = issue_fields.get("priority", {})
        priority_name = priority.get("name", "") if isinstance(priority, dict) else str(priority)
        
        # Get status
        status = issue_fields.get("status", {})
        status_name = status.get("name", "") if isinstance(status, dict) else str(status)
        
        incident_data = {
            "key": issue.get("key"),
            "summary": issue_fields.get("summary", ""),
            "team": team_name,
            "team_raw": team_raw,
            "created_date": created_date,
            "quarter": quarter,
            "priority": priority_name,
            "status": status_name,
            "resolved_date": format_date(issue_fields.get("resolutiondate")),
        }
        
        all_incidents.append(incident_data)
        
        if quarter:
            incidents_by_team_quarter[team_name][quarter].append(incident_data)
    
    # Build summary by team
    teams_summary = {}
    for team_name, quarters_data in incidents_by_team_quarter.items():
        teams_summary[team_name] = {
            "total": sum(len(issues) for issues in quarters_data.values()),
            "by_quarter": {q: len(issues) for q, issues in quarters_data.items()}
        }
    
    # Build summary by quarter
    quarters_summary = defaultdict(lambda: {"total": 0, "by_team": {}})
    for team_name, quarters_data in incidents_by_team_quarter.items():
        for quarter, issues in quarters_data.items():
            quarters_summary[quarter]["total"] += len(issues)
            quarters_summary[quarter]["by_team"][team_name] = len(issues)
    
    return {
        "teams": teams_summary,
        "by_quarter": dict(quarters_summary),
        "issues": all_incidents,
    }


def calculate_summary_stats(data: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate summary statistics for incident data."""
    issues = data.get("issues", [])
    teams = data.get("teams", {})
    by_quarter = data.get("by_quarter", {})
    
    # Priority breakdown
    priority_counts = defaultdict(int)
    for issue in issues:
        priority = issue.get("priority", "Unknown")
        priority_counts[priority] += 1
    
    # Status breakdown
    status_counts = defaultdict(int)
    for issue in issues:
        status = issue.get("status", "Unknown")
        status_counts[status] += 1
    
    return {
        "total_incidents": len(issues),
        "total_teams": len(teams),
        "total_quarters": len(by_quarter),
        "by_priority": dict(priority_counts),
        "by_status": dict(status_counts),
    }


def save_results(data: Dict[str, Any], summary: Dict[str, Any], output_json: str, output_csv: str):
    """Save results to JSON and CSV files."""
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_json) or ".", exist_ok=True)
    
    # Prepare JSON output
    output_data = {
        "generated_at": datetime.now().isoformat(),
        "summary": summary,
        "teams": data["teams"],
        "by_quarter": data["by_quarter"],
        # Exclude individual issues from JSON to keep file size manageable
        "issue_count": len(data["issues"]),
    }
    
    # Save JSON
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    print(f"✅ JSON saved to: {output_json}")
    
    # Prepare CSV (flattened format - one row per team per quarter)
    csv_rows = []
    for team_name, team_data in data["teams"].items():
        for quarter, count in team_data.get("by_quarter", {}).items():
            csv_rows.append({
                "team_name": team_name,
                "quarter": quarter,
                "incident_count": count,
            })
    
    # Sort by quarter then team
    csv_rows.sort(key=lambda x: (x["quarter"], x["team_name"]))
    
    # Save CSV
    if csv_rows:
        fieldnames = ["team_name", "quarter", "incident_count"]
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)
        print(f"✅ CSV saved to: {output_csv}")
    
    # Also save detailed incidents CSV
    detailed_csv = output_csv.replace(".csv", "_detailed.csv")
    if data["issues"]:
        fieldnames = ["key", "summary", "team", "quarter", "priority", "status", "created_date", "resolved_date"]
        with open(detailed_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(data["issues"])
        print(f"✅ Detailed CSV saved to: {detailed_csv}")


def main():
    """Main entry point."""
    pat = os.getenv("JIRA_API_KEY")
    if not pat:
        print("❌ Error: JIRA_API_KEY environment variable not set")
        print("   Set it with: export JIRA_API_KEY='your-personal-access-token'")
        sys.exit(1)
    
    print("=" * 60)
    print("📊 Incident Metrics Calculator")
    print("=" * 60)
    
    # Load quarters configuration
    quarters_config = load_config(CONFIG_QUARTERS)
    quarters = quarters_config.get("quarters", get_default_quarters())
    
    # Filter to recent quarters
    today = datetime.now()
    relevant_quarters = []
    for q in quarters:
        q_start = datetime.strptime(q["start"], "%Y-%m-%d")
        # Include quarters from last 2 years
        if q_start > today - timedelta(days=730):
            relevant_quarters.append(q)
    
    if not relevant_quarters:
        relevant_quarters = quarters[-4:]  # Last 4 quarters
    
    print(f"   Processing quarters: {[q['name'] for q in relevant_quarters]}")
    
    # Get JQL (allow override via environment variable)
    jql = os.getenv("INCIDENT_JQL", DEFAULT_INCIDENT_JQL)
    
    # Add date filter to JQL
    earliest_quarter = min(relevant_quarters, key=lambda q: q["start"])
    date_filter = f' AND created >= "{earliest_quarter["start"]}"'
    full_jql = jql + date_filter
    
    # Process incidents
    data = process_incidents(pat, full_jql, relevant_quarters)
    
    if not data["issues"]:
        print("⚠️ No incidents found matching the query")
        save_results({"teams": {}, "by_quarter": {}, "issues": []}, {"total_incidents": 0}, OUTPUT_JSON, OUTPUT_CSV)
        return
    
    # Calculate summary statistics
    summary = calculate_summary_stats(data)
    
    # Print summary
    print("\n" + "=" * 60)
    print("📈 SUMMARY")
    print("=" * 60)
    print(f"Total Incidents: {summary['total_incidents']}")
    print(f"Teams Affected: {summary['total_teams']}")
    
    print("\n📅 By Quarter:")
    for quarter in sorted(data["by_quarter"].keys()):
        q_data = data["by_quarter"][quarter]
        print(f"   {quarter}: {q_data['total']} incidents")
    
    print("\n👥 By Team (top 10):")
    sorted_teams = sorted(data["teams"].items(), key=lambda x: x[1]["total"], reverse=True)[:10]
    for team_name, team_data in sorted_teams:
        print(f"   {team_name}: {team_data['total']} incidents")
    
    print("\n🔴 By Priority:")
    for priority, count in sorted(summary["by_priority"].items(), key=lambda x: -x[1]):
        print(f"   {priority}: {count}")
    
    # Save results
    save_results(data, summary, OUTPUT_JSON, OUTPUT_CSV)
    
    print("\n✅ Incident metrics calculation complete!")


if __name__ == "__main__":
    main()

