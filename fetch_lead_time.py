#!/usr/bin/env python3
"""
Fetch Lead Time Metrics from Jira Epics.

This script calculates Lead Time for Jira Epics using the following logic:
- Primary: PRD Start Date → Release Date
- Fallback 1 (No PRD Start Date): Created Date → Release Date
- Fallback 2 (No Release Date): Use Resolved Date instead

Usage:
    1. Set the JIRA_API_KEY environment variable with your PAT
    2. Run: python fetch_lead_time.py

Output:
    - lead_time_data.json - Lead time data for dashboard
    - lead_time_data.csv - CSV export for reference
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

# JQL Query for Epics - Only Done epics for lead time calculation
# Uses Tech Modules field to identify PPIND epics
# Covers all quarters from 2024 Q3 (Jul 2024) onwards
JQL_QUERY = (
    'project = PAYPAY AND issuetype = Epic AND status = Done AND '
    '"Tech Modules" in ('
    '"Utility_PPIND Point", '
    '"Utility_PPIND GenAI", '
    '"FS_PPIND Financial Solutions", '
    '"Utility_PPIND Gift Voucher", '
    '"Utility_PPIND Mobile", '
    '"Utility_PPIND PP4B", '
    '"Utility_PPIND Web", '
    '"Utility_PPIND Notification", '
    '"O2O_PPIND Gift Voucher Reward Engine", '
    '"Utility_PPIND Merchant Intelligence", '
    '"Utility_PPIND Notification Delivery", '
    '"Utility_PPIND Notification Platform", '
    '"Utility_PPIND Websocket BE", '
    '"Utility_PPIND Risk", '
    '"O2O_Stamp Card FE", '
    '"O2O_Stamp Card BE"'
    ') AND resolved >= 2024-07-01'
)

# Custom Field IDs
FIELD_PRD_START_DATE = "customfield_15410"
FIELD_RELEASE_DATE = "customfield_10613"
FIELD_TECH_TEAM = "customfield_16028"
FIELD_QA_TEAM = "customfield_16032"

# Output files
OUTPUT_JSON = "dashboard/data/lead_time_data.json"
OUTPUT_CSV = "lead_time_data.csv"

# PPIND Tech Modules (for filtering - must match JQL query)
PPIND_TECH_TEAMS = [
    "Utility_PPIND Point",
    "Utility_PPIND GenAI",
    "FS_PPIND Financial Solutions",
    "Utility_PPIND Gift Voucher",
    "Utility_PPIND Mobile",
    "Utility_PPIND PP4B",
    "Utility_PPIND Web",
    "Utility_PPIND Notification",
    "O2O_PPIND Gift Voucher Reward Engine",
    "Utility_PPIND Merchant Intelligence",
    "Utility_PPIND Notification Delivery",
    "Utility_PPIND Notification Platform",
    "Utility_PPIND Websocket BE",
    "Utility_PPIND Risk",
    "O2O_Stamp Card FE",
    "O2O_Stamp Card BE",
]

PPIND_QA_TEAMS = [
    "FS_PPIND Finance QA",
    "O2O_PPIND_Gift Voucher QA",
    "O2O_PPIND Merchant QA",
    "P2P_PPIND QA-Core",
    "P2P_PPIND QA-Product",
    "Utility_PPIND_Communication QA",
    "Utility_PPIND GenAI QA",
    "Utility_PPIND Payment QA",
    "Utility_PPIND SDET QA",
]


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


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse a date string to datetime object."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def calculate_lead_time_days(start_date: str, end_date: str) -> Optional[int]:
    """Calculate lead time in days between two dates."""
    start = parse_date(start_date)
    end = parse_date(end_date)
    
    if start and end:
        delta = end - start
        return delta.days
    return None


def days_to_readable(days: Optional[int]) -> str:
    """Convert days to human-readable format."""
    if days is None:
        return ""
    
    if days < 0:
        return f"{days}d (negative)"
    
    weeks = days // 7
    remaining_days = days % 7
    
    if weeks > 0:
        if remaining_days > 0:
            return f"{weeks}w {remaining_days}d"
        return f"{weeks}w"
    return f"{days}d"


# =============================================================================
# JIRA API FUNCTIONS
# =============================================================================

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


def is_ppind_team_epic(issue: Dict) -> bool:
    """Check if an Epic belongs to a PPIND team."""
    tech_team = get_custom_field_value(issue, FIELD_TECH_TEAM)
    qa_team = get_custom_field_value(issue, FIELD_QA_TEAM)
    
    if tech_team and tech_team in PPIND_TECH_TEAMS:
        return True
    
    if qa_team and qa_team in PPIND_QA_TEAMS:
        return True
    
    return False


# =============================================================================
# LEAD TIME CALCULATION
# =============================================================================

def calculate_epic_lead_time(epic: Dict) -> Dict[str, Any]:
    """
    Calculate lead time for a single Epic.
    
    Logic:
    - Primary: PRD Start Date → Release Date
    - Fallback 1: Created Date → Release Date (if no PRD Start Date)
    - Fallback 2: Use Resolved Date instead of Release Date (if no Release Date)
    """
    fields = epic.get("fields", {})
    epic_key = epic.get("key", "")
    
    # Get all relevant dates
    prd_start_date = format_date(get_custom_field_value(epic, FIELD_PRD_START_DATE))
    release_date = format_date(get_custom_field_value(epic, FIELD_RELEASE_DATE))
    created_date = format_date(fields.get("created"))
    resolved_date = format_date(fields.get("resolutiondate"))
    
    # Get status
    status = fields.get("status", {})
    status_name = status.get("name", "") if isinstance(status, dict) else str(status)
    
    # Get team info
    tech_team = get_custom_field_value(epic, FIELD_TECH_TEAM) or ""
    
    # Determine start date (PRD Start Date or Created Date)
    start_date = prd_start_date if prd_start_date else created_date
    start_date_source = "PRD Start Date" if prd_start_date else "Created Date"
    
    # Determine end date (Release Date or Resolved Date)
    end_date = release_date if release_date else resolved_date
    end_date_source = "Release Date" if release_date else "Resolved Date"
    
    # Calculate lead time
    lead_time_days = calculate_lead_time_days(start_date, end_date)
    
    return {
        "epic_key": epic_key,
        "summary": fields.get("summary", ""),
        "status": status_name,
        "tech_team": tech_team,
        "prd_start_date": prd_start_date,
        "release_date": release_date,
        "created_date": created_date,
        "resolved_date": resolved_date,
        "lead_time_start": start_date,
        "lead_time_start_source": start_date_source,
        "lead_time_end": end_date,
        "lead_time_end_source": end_date_source,
        "lead_time_days": lead_time_days,
        "lead_time_readable": days_to_readable(lead_time_days),
    }


def process_epics(pat: str, jql: str) -> List[Dict]:
    """Process all Epics matching the JQL and calculate lead times."""
    print(f"🔍 Searching for Epics...")
    print(f"   JQL: {jql}")
    
    epic_fields = [
        "key",
        "summary",
        "status",
        "created",
        "resolutiondate",
        FIELD_PRD_START_DATE,
        FIELD_RELEASE_DATE,
        FIELD_TECH_TEAM,
        FIELD_QA_TEAM,
    ]
    
    epics = search_issues(pat, jql, epic_fields)
    print(f"✅ Found {len(epics)} Epics")
    
    if not epics:
        return []
    
    # Filter to PPIND team epics (optional - can be removed if not needed)
    # epics = [e for e in epics if is_ppind_team_epic(e)]
    # print(f"✅ {len(epics)} PPIND team Epics after filtering")
    
    print(f"\n📊 Calculating Lead Times...")
    results = []
    
    for epic in epics:
        lead_time_data = calculate_epic_lead_time(epic)
        results.append(lead_time_data)
    
    # Sort by lead time (longest first), None values at end
    results.sort(key=lambda x: (x["lead_time_days"] is None, -(x["lead_time_days"] or 0)))
    
    return results


def calculate_summary_stats(results: List[Dict]) -> Dict[str, Any]:
    """Calculate summary statistics for lead time data."""
    valid_lead_times = [r["lead_time_days"] for r in results if r["lead_time_days"] is not None]
    
    if not valid_lead_times:
        return {
            "total_epics": len(results),
            "epics_with_lead_time": 0,
            "avg_lead_time_days": None,
            "median_lead_time_days": None,
            "min_lead_time_days": None,
            "max_lead_time_days": None,
        }
    
    sorted_times = sorted(valid_lead_times)
    n = len(sorted_times)
    
    return {
        "total_epics": len(results),
        "epics_with_lead_time": n,
        "avg_lead_time_days": round(sum(sorted_times) / n, 1),
        "median_lead_time_days": sorted_times[n // 2] if n % 2 == 1 else (sorted_times[n // 2 - 1] + sorted_times[n // 2]) / 2,
        "min_lead_time_days": min(sorted_times),
        "max_lead_time_days": max(sorted_times),
        "avg_lead_time_readable": days_to_readable(round(sum(sorted_times) / n)),
        "median_lead_time_readable": days_to_readable(int(sorted_times[n // 2] if n % 2 == 1 else (sorted_times[n // 2 - 1] + sorted_times[n // 2]) / 2)),
    }


def get_fy_quarter_end_date(fy_year: int, quarter: int) -> datetime:
    """
    Get the last day of a fiscal year quarter.
    
    FY Quarters (FY starts April 1):
    - Q1 = Apr-Jun (ends June 30)
    - Q2 = Jul-Sep (ends Sep 30)
    - Q3 = Oct-Dec (ends Dec 31)
    - Q4 = Jan-Mar (ends Mar 31)
    
    FY25 = Apr 2024 - Mar 2025
    """
    if quarter == 1:
        # Q1 ends June 30 of the FY start year (FY25 Q1 ends June 30, 2024)
        return datetime(fy_year - 1, 6, 30)
    elif quarter == 2:
        # Q2 ends Sep 30 of the FY start year
        return datetime(fy_year - 1, 9, 30)
    elif quarter == 3:
        # Q3 ends Dec 31 of the FY start year
        return datetime(fy_year - 1, 12, 31)
    else:  # quarter == 4
        # Q4 ends Mar 31 of the FY end year
        return datetime(fy_year, 3, 31)


def get_fy_quarter(dt: datetime) -> tuple:
    """
    Get the fiscal year and quarter for a date.
    
    FY Quarters:
    - Q1 = Apr-Jun
    - Q2 = Jul-Sep
    - Q3 = Oct-Dec
    - Q4 = Jan-Mar
    
    Returns: (fy_year, quarter)
    Example: May 2024 → (FY25, Q1), Jan 2025 → (FY25, Q4)
    """
    month = dt.month
    year = dt.year
    
    if month >= 4 and month <= 6:  # Apr-Jun = Q1
        fy_year = year + 1  # FY25 for Apr-Jun 2024
        quarter = 1
    elif month >= 7 and month <= 9:  # Jul-Sep = Q2
        fy_year = year + 1
        quarter = 2
    elif month >= 10 and month <= 12:  # Oct-Dec = Q3
        fy_year = year + 1
        quarter = 3
    else:  # Jan-Mar = Q4
        fy_year = year  # FY25 for Jan-Mar 2025
        quarter = 4
    
    return (fy_year, quarter)


def assign_quarter_with_grace_period(dt: datetime, grace_days: int = 10) -> str:
    """
    Assign a fiscal year quarter to a date, with grace period for dates just after quarter end.
    
    If the date is within 'grace_days' of the previous quarter's end, 
    assign it to the previous quarter.
    
    Example: July 7, 2024 is within 10 days of FY25 Q1 end (June 30), so it's FY25 Q1.
    """
    # Get natural FY quarter
    fy_year, quarter = get_fy_quarter(dt)
    
    # Check if we're in the grace period of the previous quarter
    if quarter == 1:
        prev_quarter = 4
        prev_fy_year = fy_year - 1
    else:
        prev_quarter = quarter - 1
        prev_fy_year = fy_year
    
    prev_quarter_end = get_fy_quarter_end_date(prev_fy_year, prev_quarter)
    days_after_prev_quarter = (dt - prev_quarter_end).days
    
    # If within grace period, assign to previous quarter
    if 0 < days_after_prev_quarter <= grace_days:
        return f"FY{prev_fy_year % 100} Q{prev_quarter}"
    
    return f"FY{fy_year % 100} Q{quarter}"


def group_by_quarter(results: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Group results by FISCAL YEAR quarter based on Release Date (or Resolved Date as fallback).
    
    FY Quarters (FY starts April 1):
    - Q1 = Apr-Jun, Q2 = Jul-Sep, Q3 = Oct-Dec, Q4 = Jan-Mar
    
    Uses a 10-day grace period: if end date is within 10 days of quarter end,
    the epic is still assigned to that quarter.
    Example: July 7, 2024 → FY25 Q1 (within 10 days of June 30)
    """
    quarters = {}
    
    for r in results:
        end_date = r.get("lead_time_end")
        if not end_date:
            quarter = "No End Date"
        else:
            try:
                dt = datetime.strptime(end_date[:10], "%Y-%m-%d")
                quarter = assign_quarter_with_grace_period(dt, grace_days=10)
            except:
                quarter = "Unknown"
        
        if quarter not in quarters:
            quarters[quarter] = []
        quarters[quarter].append(r)
    
    return quarters


def is_ppind_epic(tech_team_value: str) -> bool:
    """
    Check if an epic belongs to PPIND based on Tech Modules field.
    Tech Modules can be a comma-separated list of values.
    Returns True if ANY of the values match PPIND_TECH_TEAMS.
    """
    if not tech_team_value:
        return False
    
    # Split by comma in case of multiple values
    tech_modules = [t.strip() for t in tech_team_value.split(",")]
    
    # Check if any module matches PPIND list
    for module in tech_modules:
        if module in PPIND_TECH_TEAMS:
            return True
    
    return False


def categorize_epics(results: List[Dict]) -> Dict[str, List[Dict]]:
    """Categorize epics into PayPay All, PPIND Only, and PayPay excl. PPIND."""
    ppind_only = []
    excl_ppind = []
    
    for epic in results:
        tech_team = epic.get("tech_team", "")
        # Check if epic belongs to PPIND team (handles multi-value field)
        if is_ppind_epic(tech_team):
            ppind_only.append(epic)
        else:
            excl_ppind.append(epic)
    
    print(f"\n📊 Categorization Results:")
    print(f"   PPIND Only: {len(ppind_only)} epics")
    print(f"   Excl. PPIND: {len(excl_ppind)} epics")
    
    return {
        "paypay_all": results,  # All epics
        "ppind_only": ppind_only,  # Only PPIND team epics
        "excl_ppind": excl_ppind,  # PayPay excluding PPIND
    }


def build_dataset(epics: List[Dict], name: str) -> Dict[str, Any]:
    """Build a complete dataset with summary and by_quarter grouping."""
    summary = calculate_summary_stats(epics)
    by_quarter = group_by_quarter(epics)
    
    return {
        "name": name,
        "summary": summary,
        "by_quarter": by_quarter,
        "epics": epics,
    }


def save_results(results: List[Dict], output_json: str, output_csv: str):
    """Save results to JSON (with 3 datasets) and CSV files."""
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_json) or ".", exist_ok=True)
    
    # Categorize epics into 3 groups
    categories = categorize_epics(results)
    
    # Build datasets for each category
    datasets = {
        "paypay_all": build_dataset(categories["paypay_all"], "PayPay All"),
        "ppind_only": build_dataset(categories["ppind_only"], "PPIND Only"),
        "excl_ppind": build_dataset(categories["excl_ppind"], "PayPay (excl. PPIND)"),
    }
    
    # Prepare JSON output with all 3 datasets
    output_data = {
        "generated_at": datetime.now().isoformat(),
        "default_scope": "ppind_only",  # Default to PPIND Only
        "scopes": ["ppind_only", "paypay_all", "excl_ppind"],
        "scope_labels": {
            "paypay_all": "PayPay All",
            "ppind_only": "PPIND Only",
            "excl_ppind": "PayPay (excl. PPIND)"
        },
        "datasets": datasets,
    }
    
    # Save JSON
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    print(f"✅ JSON saved to: {output_json}")
    
    # Print breakdown
    print(f"\n📊 Dataset Breakdown:")
    print(f"   PayPay All: {len(categories['paypay_all'])} epics")
    print(f"   PPIND Only: {len(categories['ppind_only'])} epics")
    print(f"   PayPay (excl. PPIND): {len(categories['excl_ppind'])} epics")
    
    # Save CSV (all epics)
    if results:
        fieldnames = list(results[0].keys())
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f"✅ CSV saved to: {output_csv}")


def main():
    """Main entry point."""
    pat = os.getenv("JIRA_API_KEY")
    if not pat:
        print("❌ Error: JIRA_API_KEY environment variable not set")
        print("   Set it with: export JIRA_API_KEY='your-personal-access-token'")
        sys.exit(1)
    
    # Allow JQL override via environment variable
    jql = os.getenv("LEAD_TIME_JQL", JQL_QUERY)
    
    print("=" * 60)
    print("📊 Jira Lead Time Calculator")
    print("=" * 60)
    print(f"   Note: Only considering Done epics")
    
    # Process epics
    results = process_epics(pat, jql)
    
    if not results:
        print("⚠️ No Epics found matching the query")
        # Still save empty results
        save_results([], OUTPUT_JSON, OUTPUT_CSV)
        return
    
    # Categorize and print summary for each scope
    categories = categorize_epics(results)
    
    print("\n" + "=" * 60)
    print("📈 SUMMARY BY SCOPE")
    print("=" * 60)
    
    for scope_key, scope_name in [("paypay_all", "PayPay All"), ("ppind_only", "PPIND Only"), ("excl_ppind", "PayPay excl. PPIND")]:
        epics = categories[scope_key]
        summary = calculate_summary_stats(epics)
        print(f"\n🔹 {scope_name}: {summary['total_epics']} epics")
        if summary['avg_lead_time_days']:
            print(f"   Avg: {summary['avg_lead_time_readable']} | Median: {summary['median_lead_time_readable']}")
    
    # Print PPIND by quarter (since that's the default)
    ppind_results = categories["ppind_only"]
    if ppind_results:
        by_quarter = group_by_quarter(ppind_results)
        print("\n📅 PPIND Only - By Quarter:")
        for quarter in sorted(by_quarter.keys()):
            quarter_results = by_quarter[quarter]
            valid_times = [r["lead_time_days"] for r in quarter_results if r["lead_time_days"] is not None]
            avg = round(sum(valid_times) / len(valid_times), 1) if valid_times else 0
            print(f"   {quarter}: {len(quarter_results)} epics, avg {days_to_readable(int(avg))}")
    
    # Save results (includes all 3 scopes)
    save_results(results, OUTPUT_JSON, OUTPUT_CSV)
    
    print("\n✅ Lead Time calculation complete!")


if __name__ == "__main__":
    main()

