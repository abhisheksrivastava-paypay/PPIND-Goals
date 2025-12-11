#!/usr/bin/env python3
"""
Fetch Jira Epics with Tech Module Percentage >= 50%.

This script queries Jira Server for Epics and calculates the "Tech Module" percentage
based on the contribution of PPIND teams (Tech, QA, PM, Designer) to the Roadmap issues
within each Epic.

The calculation replicates the Jira Structure formula logic:
- Filters Roadmap issues that are not Done/Not Needed
- Identifies issues belonging to PPIND teams or assigned to specific PMs/Designers
- Calculates the percentage of work assigned to Tech Module teams

Usage:
    1. Set the JIRA_API_KEY environment variable with your PAT
    2. Run: python fetch_jira_tech_module_epics.py

Output:
    - tech_module_epics.csv - Epics with >= 50% Tech Module contribution
    - Console summary with statistics
"""

import os
import sys
import csv
import json
import time
import requests
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import quote

# =============================================================================
# CONFIGURATION
# =============================================================================

# Jira Server Configuration
JIRA_SERVER_URL = "https://paypay-corp.rickcloud.jp/jira"
JIRA_API_BASE = f"{JIRA_SERVER_URL}/rest/api/2"

# JQL Query for Epics (hardcoded)
JQL_QUERY = (
    "project = PAYPAY AND issuetype = Epic AND "
    "labels in (2025Q1_Delivery, 2025Q2_Delivery, 2024Q3_Delivery, 2024Q4_Delivery)"
)

# Custom Field IDs
FIELD_PRD_START_DATE = "customfield_15410"
FIELD_RELEASE_DATE = "customfield_10613"
FIELD_TECH_TEAM = "customfield_16028"
FIELD_QA_TEAM = "customfield_16032"
FIELD_ROLE_16911 = "customfield_16911"
FIELD_ROLE_16029 = "customfield_16029"

# Epic Link field (links Roadmap issues to their parent Epic)
FIELD_EPIC_LINK = "customfield_10101"

# Parent field for hierarchy (Jira next-gen / team-managed projects)
FIELD_PARENT = "parent"

# =============================================================================
# TEAM ARRAYS (from Jira Structure formula)
# =============================================================================

PPIND_TECH_TEAMS = [
    "Utility_PPIND Point",
    "Utility_PPIND GenAI",
    "FS_PPIND Financial Solutions",
    "Utility_PPIND Gift Voucher",
    "Utility_PPIND Mobile",
    "Utility_PPIND PP4B",
    "Utility_PPIND Web",
    "Utility_Infrastructure",
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

PPIND_PM_EMAILS = [
    "abhishek.kumar@paypay-corp.co.jp",
    "shriansh.kumar@paypay-corp.co.jp",
    "gaurav.jain@paypay-corp.co.jp",
    "rishabh.garg@paypay-corp.co.jp",
    "abhishek.saraswat@paypay-corp.co.jp",
]

PPIND_DESIGNER_EMAILS = [
    "ambreesh.arya@paypay-corp.co.jp",
    "nidhi.patidar@paypay-corp.co.jp",
    "vivek.verma@paypay-corp.co.jp",
]

# Output files
OUTPUT_CSV = "tech_module_epics.csv"

# Minimum Tech Module percentage to include in output
MIN_TECH_MODULE_PCT = 50


class JiraError(Exception):
    """Custom exception for Jira API errors."""
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


def seconds_to_person_weeks(seconds: float) -> float:
    """
    Convert seconds to person-weeks.
    Formula from Jira Structure: originalEstimate / (5 * 8 * 3600) / 1000
    
    Note: The original formula divides by 1000, which seems to assume
    the input is in milliseconds. Jira API returns seconds, so we adjust.
    
    5 days * 8 hours * 3600 seconds = 144000 seconds per person-week
    """
    if not seconds:
        return 0.0
    return round(seconds / (5 * 8 * 3600), 1)


def format_date(date_str: Optional[str]) -> str:
    """Format a Jira date string to YYYY-MM-DD."""
    if not date_str:
        return ""
    try:
        # Jira dates can be in various formats
        if "T" in date_str:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d")
        return date_str[:10] if len(date_str) >= 10 else date_str
    except (ValueError, TypeError):
        return date_str or ""


def format_estimate_dhm(seconds: Optional[float]) -> str:
    """Convert seconds to human-readable days/hours/minutes format."""
    if not seconds:
        return ""
    try:
        total_minutes = int(seconds / 60)
    except (TypeError, ValueError):
        return ""
    
    d, rem = divmod(total_minutes, 1440)  # 1440 minutes in a day
    h, m = divmod(rem, 60)
    
    parts = []
    if d:
        parts.append(f"{d}d")
    if h or d:
        parts.append(f"{h}h")
    parts.append(f"{m}m")
    
    return " ".join(parts)


# =============================================================================
# JIRA API FUNCTIONS
# =============================================================================

def search_issues(pat: str, jql: str, fields: List[str], max_results: int = 1000) -> List[Dict]:
    """
    Search for issues using JQL with pagination.
    
    Args:
        pat: Personal Access Token
        jql: JQL query string
        fields: List of field names to retrieve
        max_results: Maximum total results to fetch
    
    Returns:
        List of issue dictionaries
    """
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
    """
    Get a custom field value from an issue.
    Handles various field formats (string, object with value/name).
    """
    fields = issue.get("fields", {})
    value = fields.get(field_id)
    
    if value is None:
        return None
    
    if isinstance(value, str):
        return value
    
    if isinstance(value, dict):
        return value.get("value") or value.get("name") or str(value)
    
    if isinstance(value, list):
        # For multi-select fields
        values = []
        for v in value:
            if isinstance(v, dict):
                values.append(v.get("value") or v.get("name") or str(v))
            else:
                values.append(str(v))
        return ", ".join(values) if values else None
    
    return str(value)


def get_assignee_email(issue: Dict) -> Optional[str]:
    """Get assignee email from issue."""
    fields = issue.get("fields", {})
    assignee = fields.get("assignee")
    
    if not assignee:
        return None
    
    # Try different field names for email
    return (
        assignee.get("emailAddress") or 
        assignee.get("name") or 
        assignee.get("key")
    )


def get_assignee_name(issue: Dict) -> str:
    """Get assignee display name from issue."""
    fields = issue.get("fields", {})
    assignee = fields.get("assignee")
    
    if not assignee:
        return ""
    
    return assignee.get("displayName") or assignee.get("name") or ""


# =============================================================================
# TECH MODULE CALCULATION
# =============================================================================

def is_ppind_team_issue(issue: Dict) -> bool:
    """
    Check if an issue belongs to a PPIND team based on the formula logic.
    
    Returns True if:
    - Tech team field is in PPIND_TECH_TEAMS, OR
    - QA team field is in PPIND_QA_TEAMS, OR
    - Assignee is in PPIND_PM_EMAILS or PPIND_DESIGNER_EMAILS
    """
    tech_team = get_custom_field_value(issue, FIELD_TECH_TEAM)
    qa_team = get_custom_field_value(issue, FIELD_QA_TEAM)
    assignee_email = get_assignee_email(issue)
    
    # Check tech team
    if tech_team and tech_team in PPIND_TECH_TEAMS:
        return True
    
    # Check QA team
    if qa_team and qa_team in PPIND_QA_TEAMS:
        return True
    
    # Check assignee (PM or Designer)
    if assignee_email:
        email_lower = assignee_email.lower()
        if email_lower in [e.lower() for e in PPIND_PM_EMAILS]:
            return True
        if email_lower in [e.lower() for e in PPIND_DESIGNER_EMAILS]:
            return True
    
    return False


def has_role_field_populated(issue: Dict) -> bool:
    """
    Check if at least one role-related field is populated.
    
    From formula:
    (issue.customfield_16028 OR
     issue.customfield_16032 OR
     issue.customfield_16911 OR
     issue.customfield_16028 = "PdM" OR
     issue.customfield_16029)
    """
    tech_team = get_custom_field_value(issue, FIELD_TECH_TEAM)
    qa_team = get_custom_field_value(issue, FIELD_QA_TEAM)
    role_16911 = get_custom_field_value(issue, FIELD_ROLE_16911)
    role_16029 = get_custom_field_value(issue, FIELD_ROLE_16029)
    
    if tech_team:
        return True
    if qa_team:
        return True
    if role_16911:
        return True
    if role_16029:
        return True
    if tech_team == "PdM":
        return True
    
    return False


def calculate_tech_module_percentage(roadmap_issues: List[Dict]) -> Tuple[float, float, float]:
    """
    Calculate the Tech Module percentage for a set of Roadmap issues.
    
    Filters issues based on the formula criteria:
    - Status != "Done" AND != "Not Needed"
    - Has role field populated
    - Has original estimate defined
    
    Then calculates:
    - total_estimate_full: Sum of all qualifying estimates (in person-weeks)
    - tech_module_sum: Sum of estimates for PPIND team issues (in person-weeks)
    - tech_module_pct: (tech_module_sum / total_estimate_full) * 100
    
    Returns:
        Tuple of (tech_module_pct, tech_module_sum, total_estimate_full)
    """
    total_estimate_full = 0.0
    tech_module_sum = 0.0
    
    excluded_statuses = {"Done", "Not Needed"}
    
    for issue in roadmap_issues:
        fields = issue.get("fields", {})
        
        # Check status
        status = fields.get("status", {})
        status_name = status.get("name", "") if isinstance(status, dict) else str(status)
        
        if status_name in excluded_statuses:
            continue
        
        # Check if has role field populated
        if not has_role_field_populated(issue):
            continue
        
        # Check original estimate
        original_estimate = fields.get("timeoriginalestimate")
        if not original_estimate:
            continue
        
        # Convert to person-weeks
        estimate_pw = seconds_to_person_weeks(original_estimate)
        
        # Add to total
        total_estimate_full += estimate_pw
        
        # Check if PPIND team issue
        if is_ppind_team_issue(issue):
            tech_module_sum += estimate_pw
    
    # Calculate percentage
    if total_estimate_full > 0:
        tech_module_pct = round((tech_module_sum / total_estimate_full) * 100, 1)
    else:
        tech_module_pct = 0.0
    
    return tech_module_pct, tech_module_sum, total_estimate_full


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def batch_fetch_roadmap_issues(pat: str, epic_keys: List[str], debug: bool = False) -> Dict[str, List[Dict]]:
    """
    Batch fetch ALL Roadmap issues for multiple Epics in a single query.
    This is much faster than fetching issues for each Epic individually.
    
    Args:
        pat: Personal Access Token
        epic_keys: List of Epic keys to fetch Roadmap issues for
        debug: If True, print additional debug information
    
    Returns:
        Dictionary mapping Epic key -> list of Roadmap issues
    """
    if not epic_keys:
        return {}
    
    # Build JQL to fetch all Roadmap issues for all Epics at once
    # Jira has limits on IN clause size, so we batch in groups of 100
    BATCH_SIZE = 100
    
    roadmap_fields = [
        "key",
        "summary",
        "status",
        "assignee",
        "timeoriginalestimate",
        FIELD_EPIC_LINK,
        FIELD_PARENT,
        FIELD_TECH_TEAM,
        FIELD_QA_TEAM,
        FIELD_ROLE_16911,
        FIELD_ROLE_16029,
    ]
    
    all_roadmap_issues = []
    
    # Try primary method: "Epic Link" field
    print(f"   Trying 'Epic Link' field...")
    
    for i in range(0, len(epic_keys), BATCH_SIZE):
        batch_keys = epic_keys[i:i + BATCH_SIZE]
        keys_str = ", ".join(batch_keys)
        jql = f'"Epic Link" IN ({keys_str}) AND issuetype = Roadmap'
        
        if debug and i == 0:
            print(f"   🔍 DEBUG - JQL: {jql[:200]}...")
        
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(epic_keys) + BATCH_SIZE - 1) // BATCH_SIZE
        
        if total_batches > 1:
            print(f"   📦 Fetching Roadmap issues batch {batch_num}/{total_batches}...")
        
        issues = search_issues(pat, jql, roadmap_fields, max_results=5000)
        all_roadmap_issues.extend(issues)
    
    # If no issues found with Epic Link, try "parent" field (for next-gen projects)
    if not all_roadmap_issues:
        print(f"   ⚠️ No issues found with 'Epic Link'. Trying 'parent' field...")
        
        for i in range(0, len(epic_keys), BATCH_SIZE):
            batch_keys = epic_keys[i:i + BATCH_SIZE]
            keys_str = ", ".join(batch_keys)
            jql = f'parent IN ({keys_str}) AND issuetype = Roadmap'
            
            if debug and i == 0:
                print(f"   🔍 DEBUG - JQL: {jql[:200]}...")
            
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (len(epic_keys) + BATCH_SIZE - 1) // BATCH_SIZE
            
            if total_batches > 1:
                print(f"   📦 Fetching Roadmap issues batch {batch_num}/{total_batches}...")
            
            issues = search_issues(pat, jql, roadmap_fields, max_results=5000)
            all_roadmap_issues.extend(issues)
    
    # If still no issues, try without Roadmap filter to see if issue type name is different
    if not all_roadmap_issues and debug:
        print(f"\n   🔍 DEBUG - Testing with just first Epic to check issue types...")
        test_jql = f'"Epic Link" = {epic_keys[0]} OR parent = {epic_keys[0]}'
        test_issues = search_issues(pat, test_jql, ["key", "issuetype", "summary"], max_results=10)
        if test_issues:
            print(f"   Found {len(test_issues)} child issues. Issue types:")
            for ti in test_issues[:5]:
                it = ti.get("fields", {}).get("issuetype", {})
                it_name = it.get("name", "Unknown") if isinstance(it, dict) else str(it)
                print(f"      - {ti.get('key')}: {it_name}")
        else:
            print(f"   No child issues found for {epic_keys[0]}")
    
    # Group issues by Epic Link or Parent
    issues_by_epic: Dict[str, List[Dict]] = {key: [] for key in epic_keys}
    
    for issue in all_roadmap_issues:
        # Try Epic Link field first
        epic_link = get_custom_field_value(issue, FIELD_EPIC_LINK)
        
        # If not found, try parent field
        if not epic_link:
            parent = issue.get("fields", {}).get("parent", {})
            if isinstance(parent, dict):
                epic_link = parent.get("key")
        
        if epic_link and epic_link in issues_by_epic:
            issues_by_epic[epic_link].append(issue)
    
    return issues_by_epic


def process_epics(pat: str, jql: str, debug: bool = False) -> List[Dict]:
    """
    Process all Epics matching the JQL and calculate Tech Module percentages.
    
    OPTIMIZED: Fetches all Roadmap issues in batch instead of per-Epic queries.
    
    Args:
        pat: Personal Access Token
        jql: JQL query for Epics
        debug: If True, print additional debug information
    
    Returns:
        List of Epic data dictionaries with calculated percentages
    """
    print(f"🔍 Searching for Epics...")
    print(f"   JQL: {jql}")
    
    # Fields to retrieve for Epics
    epic_fields = [
        "key",
        "issuetype",
        "summary",
        "status",
        "assignee",
        "created",
        "resolutiondate",
        FIELD_PRD_START_DATE,
        FIELD_RELEASE_DATE,
        "timeoriginalestimate",
    ]
    
    epics = search_issues(pat, jql, epic_fields)
    print(f"✅ Found {len(epics)} Epics")
    
    if not epics:
        return []
    
    # OPTIMIZATION: Batch fetch ALL Roadmap issues for all Epics at once
    epic_keys = [epic.get("key") for epic in epics if epic.get("key")]
    print(f"\n🚀 Batch fetching Roadmap issues for {len(epic_keys)} Epics...")
    
    roadmap_by_epic = batch_fetch_roadmap_issues(pat, epic_keys, debug=debug)
    
    total_roadmap = sum(len(issues) for issues in roadmap_by_epic.values())
    print(f"✅ Fetched {total_roadmap} Roadmap issues total")
    
    # Process each Epic using pre-fetched data
    print(f"\n📊 Calculating Tech Module percentages...")
    results = []
    
    for i, epic in enumerate(epics):
        epic_key = epic.get("key")
        
        # Get pre-fetched Roadmap issues for this Epic
        roadmap_issues = roadmap_by_epic.get(epic_key, [])
        
        # Calculate Tech Module percentage
        tech_pct, tech_sum, total_est = calculate_tech_module_percentage(roadmap_issues)
        
        # Extract Epic fields
        fields = epic.get("fields", {})
        
        # Get issue type name
        issuetype = fields.get("issuetype", {})
        issuetype_name = issuetype.get("name", "") if isinstance(issuetype, dict) else ""
        
        # Get dates
        prd_start_date = format_date(get_custom_field_value(epic, FIELD_PRD_START_DATE))
        release_date = format_date(get_custom_field_value(epic, FIELD_RELEASE_DATE))
        created = format_date(fields.get("created"))
        resolved = format_date(fields.get("resolutiondate"))
        
        # Get total estimate (Epic's own estimate in seconds)
        epic_estimate = fields.get("timeoriginalestimate") or 0
        
        result = {
            "issue_key": epic_key,
            "issue_type": issuetype_name,
            "summary": fields.get("summary", ""),
            "prd_start_date": prd_start_date,
            "release_date": release_date,
            "assignee": get_assignee_name(epic),
            "created": created,
            "resolved": resolved,
            "total_estimate_seconds": total_est * 5 * 8 * 3600,  # Convert back to seconds
            "total_estimate_dhm": format_estimate_dhm(total_est * 5 * 8 * 3600),
            "total_estimate_pw": total_est,
            "tech_module_pw": tech_sum,
            "tech_module_pct": tech_pct,
            "roadmap_issues_count": len(roadmap_issues),
        }
        
        results.append(result)
        
        # Progress indicator every 50 epics
        if (i + 1) % 50 == 0:
            print(f"   Processed {i + 1}/{len(epics)} Epics...")
    
    print(f"✅ Processed all {len(epics)} Epics")
    return results


def save_results_to_csv(results: List[Dict], filename: str, min_pct: float = 50.0) -> int:
    """
    Save results to CSV, filtering by minimum Tech Module percentage.
    
    Args:
        results: List of Epic result dictionaries
        filename: Output CSV filename
        min_pct: Minimum Tech Module percentage to include
    
    Returns:
        Number of rows written
    """
    # Filter by minimum percentage
    filtered = [r for r in results if r["tech_module_pct"] >= min_pct]
    
    # Sort by percentage descending
    filtered = sorted(filtered, key=lambda r: r["tech_module_pct"], reverse=True)
    
    fieldnames = [
        "Issue Key",
        "Issue Type",
        "Summary",
        "PRD Start Date",
        "Release Date",
        "Assignee",
        "Created",
        "Resolved",
        "Total Estimate (person-weeks)",
        "Tech Module %",
        "Roadmap Issues Count",
    ]
    
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        
        for r in filtered:
            writer.writerow([
                r["issue_key"],
                r["issue_type"],
                r["summary"],
                r["prd_start_date"],
                r["release_date"],
                r["assignee"],
                r["created"],
                r["resolved"],
                r["total_estimate_pw"],
                r["tech_module_pct"],
                r["roadmap_issues_count"],
            ])
    
    print(f"\n📄 Saved {len(filtered)} Epics to: {filename}")
    return len(filtered)


def display_summary(results: List[Dict], min_pct: float = 50.0) -> None:
    """Display a summary of the results."""
    total = len(results)
    above_threshold = len([r for r in results if r["tech_module_pct"] >= min_pct])
    below_threshold = total - above_threshold
    
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total Epics processed: {total}")
    print(f"Epics with Tech Module >= {min_pct}%: {above_threshold}")
    print(f"Epics with Tech Module < {min_pct}%: {below_threshold}")
    
    if above_threshold > 0:
        avg_pct = sum(r["tech_module_pct"] for r in results if r["tech_module_pct"] >= min_pct) / above_threshold
        print(f"Average Tech Module % (above threshold): {avg_pct:.1f}%")
    
    # Top 10 Epics by Tech Module %
    if results:
        print("\n" + "-" * 80)
        print("TOP 10 EPICS BY TECH MODULE %")
        print("-" * 80)
        print(f"{'Epic Key':<15} | {'Tech %':>8} | {'Est (PW)':>10} | Summary")
        print("-" * 80)
        
        top_10 = sorted(results, key=lambda r: r["tech_module_pct"], reverse=True)[:10]
        for r in top_10:
            summary = (r["summary"] or "")[:40]
            print(f"{r['issue_key']:<15} | {r['tech_module_pct']:>7.1f}% | {r['total_estimate_pw']:>10.1f} | {summary}")
    
    print("=" * 80)


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main function to fetch and process Jira Epics."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fetch Jira Epics with Tech Module percentage >= 50%",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default settings (>= 50% threshold)
  python fetch_jira_tech_module_epics.py
  
  # Change minimum percentage threshold
  python fetch_jira_tech_module_epics.py --min-pct 40
  
  # Change output file
  python fetch_jira_tech_module_epics.py --output my_epics.csv
  
  # Include all Epics (no filtering by percentage)
  python fetch_jira_tech_module_epics.py --all
        """
    )
    
    parser.add_argument(
        "--min-pct",
        type=float,
        default=MIN_TECH_MODULE_PCT,
        help=f"Minimum Tech Module percentage to include (default: {MIN_TECH_MODULE_PCT})"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default=OUTPUT_CSV,
        help=f"Output CSV filename (default: {OUTPUT_CSV})"
    )
    
    parser.add_argument(
        "--all",
        action="store_true",
        help="Include all Epics in output (ignore min-pct filter)"
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print debug information to help diagnose issues"
    )
    
    args = parser.parse_args()
    
    # Get PAT from environment
    pat = os.getenv("JIRA_API_KEY")
    
    if not pat:
        print("❌ Error: JIRA_API_KEY environment variable is not set.")
        print("\nTo set it:")
        print("  export JIRA_API_KEY='your_personal_access_token'")
        print("\nOr pass it inline:")
        print("  JIRA_API_KEY='your_pat' python fetch_jira_tech_module_epics.py")
        sys.exit(1)
    
    # Show masked PAT
    masked = pat[:6] + "..." + pat[-4:] if len(pat) > 10 else "***"
    print(f"🔑 Using PAT: {masked}")
    print(f"🌐 Jira Server: {JIRA_SERVER_URL}")
    
    try:
        # Process Epics
        results = process_epics(pat, JQL_QUERY, debug=args.debug)
        
        if not results:
            print("\n⚠️ No Epics found matching the JQL query.")
            sys.exit(0)
        
        # Display summary
        min_pct = 0 if args.all else args.min_pct
        display_summary(results, min_pct)
        
        # Save to CSV
        save_results_to_csv(results, args.output, min_pct)
        
        print(f"\n✅ Done! Output saved to: {args.output}")
        
    except JiraError as e:
        print(f"\n❌ Jira API Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

