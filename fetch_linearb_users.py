#!/usr/bin/env python3
"""
Fetch all users/contributors from LinearB API.

This script tries multiple approaches:
1. First tries the /api/v2/teams endpoint (more commonly accessible) 
   and extracts contributors from team data
2. Falls back to /api/v1/users if available

Usage:
    1. Set the LINEARB_API_KEY environment variable
    2. Run: python fetch_linearb_users.py

The script will fetch all users and display their details.
"""

import os
import time
import requests
import json
from typing import Optional, Tuple

# Configuration
LINEARB_BASE = "https://public-api.linearb.io"
USERS_ENDPOINT = f"{LINEARB_BASE}/api/v1/users"
TEAMS_ENDPOINT = f"{LINEARB_BASE}/api/v2/teams"


class LinearBError(Exception):
    """Custom exception for LinearB API errors."""
    pass


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
            
            # Return the response for caller to handle (for 403, 404, etc.)
            return response
        
        except requests.exceptions.RequestException as e:
            if attempt < 4:
                print(f"  ⚠️ Request failed: {e}, retrying in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 16)
                continue
            raise LinearBError(f"Request failed after retries: {e}")
    
    raise LinearBError("Request failed after maximum retries")


def fetch_all_teams(api_key: str) -> list:
    """
    Fetch all teams from LinearB with pagination support.
    This endpoint is typically more accessible than /users.
    """
    headers = {
        "x-api-key": api_key,
        "Accept": "application/json"
    }
    
    all_teams = []
    offset = 0
    page_size = 50
    
    print("🔍 Fetching teams from LinearB...")
    
    while True:
        params = {
            "offset": offset,
            "page_size": page_size
        }
        
        response = _req("GET", TEAMS_ENDPOINT, headers, params=params)
        
        if response.status_code == 403:
            print(f"  🔍 Debug - Response: {response.text[:500]}")
            raise LinearBError(f"Access denied to teams endpoint (403). Check if your API key has expired or is invalid.")
        
        if response.status_code >= 400:
            print(f"  🔍 Debug - Response: {response.text[:500]}")
            raise LinearBError(f"API Error {response.status_code}: {response.text}")
        
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


def fetch_all_users(api_key: str) -> Tuple[list, bool]:
    """
    Fetch all users from LinearB with pagination support.
    Deduplicates users by ID (API may return duplicates across pages).
    
    Returns:
        Tuple of (users list, success boolean)
    """
    headers = {
        "x-api-key": api_key,
        "Accept": "application/json"
    }
    
    # Use dict to deduplicate by user ID
    users_by_id = {}
    offset = 0
    page_size = 50
    fetched_count = 0
    
    print("🔍 Fetching users from LinearB /api/v1/users endpoint...")
    
    # First request to check access
    params = {"offset": 0, "page_size": page_size}
    response = _req("GET", USERS_ENDPOINT, headers, params=params)
    
    if response.status_code == 403:
        print("  ⚠️ No access to /api/v1/users endpoint (403 Forbidden)")
        print("     Your API key may not have 'users:read' permission.")
        return [], False
    
    if response.status_code >= 400:
        print(f"  ⚠️ Users endpoint returned {response.status_code}")
        return [], False
    
    # Process first response
    data = response.json()
    items = data.get("items", [])
    for user in items:
        user_id = str(user.get("id", ""))
        if user_id and user_id not in users_by_id:
            users_by_id[user_id] = user
    fetched_count += len(items)
    total = data.get("total", fetched_count)
    print(f"  📥 Fetched {fetched_count}/{total} records (unique: {len(users_by_id)})...")
    
    # Continue pagination
    offset += page_size
    while offset < total and items:
        params = {"offset": offset, "page_size": page_size}
        response = _req("GET", USERS_ENDPOINT, headers, params=params)
        
        if response.status_code >= 400:
            break
            
        data = response.json()
        items = data.get("items", [])
        for user in items:
            user_id = str(user.get("id", ""))
            if user_id and user_id not in users_by_id:
                users_by_id[user_id] = user
        fetched_count += len(items)
        print(f"  📥 Fetched {fetched_count}/{total} records (unique: {len(users_by_id)})...")
        offset += page_size
    
    all_users = list(users_by_id.values())
    print(f"✅ Total unique users: {len(all_users)} (API returned {fetched_count} records with duplicates)")
    return all_users, True


def extract_contributors_from_teams(teams: list) -> list:
    """
    Extract unique contributors/members from team data.
    Teams may include 'contributors', 'members', or similar fields.
    """
    contributors = {}  # Use dict to dedupe by ID
    
    for team in teams:
        team_name = team.get("name", "Unknown Team")
        
        # Try various field names that might contain members
        member_fields = ["contributors", "members", "users", "team_members"]
        
        for field in member_fields:
            members = team.get(field, [])
            if members:
                for member in members:
                    if isinstance(member, dict):
                        member_id = str(member.get("id") or member.get("contributor_id") or member.get("user_id", ""))
                        if member_id and member_id not in contributors:
                            # Add team name to the member info
                            member_copy = member.copy()
                            member_copy["_team_names"] = [team_name]
                            contributors[member_id] = member_copy
                        elif member_id:
                            # Add additional team name
                            if team_name not in contributors[member_id].get("_team_names", []):
                                contributors[member_id].setdefault("_team_names", []).append(team_name)
    
    return list(contributors.values())


def get_team_names(user: dict) -> str:
    """
    Extract team names from user data.
    Handles both 'team_membership' field and '_team_names' (our extracted field).
    """
    # First check our extracted field
    team_names = user.get("_team_names", [])
    if team_names:
        return ", ".join(team_names)
    
    # Then check standard team_membership field
    team_membership = user.get("team_membership", [])
    if not team_membership:
        return ""
    
    names = []
    for team in team_membership:
        if isinstance(team, dict):
            name = team.get("name") or team.get("team_name")
            if name:
                names.append(name)
        elif isinstance(team, str):
            names.append(team)
    
    return ", ".join(names)


def display_users(users: list) -> None:
    """
    Display user information in a formatted table.
    """
    if not users:
        print("No users found.")
        return
    
    print("\n" + "=" * 140)
    print(f"{'ID':<12} | {'Name':<25} | {'Email':<35} | {'Active':<6} | {'Team(s)'}")
    print("=" * 140)
    
    for user in users:
        user_id = str(user.get("id") or user.get("contributor_id") or "N/A")[:10]
        name = (user.get("name") or user.get("display_name") or user.get("login") or "N/A")[:23]
        email = (user.get("email") or "N/A")[:33]
        active = "Yes" if not user.get("deleted_at") else "No"
        teams = get_team_names(user)[:50]  # Truncate long team lists for display
        
        print(f"{user_id:<12} | {name:<25} | {email:<35} | {active:<6} | {teams}")
    
    print("=" * 140)


def display_teams(teams: list) -> None:
    """
    Display team information in a formatted table.
    """
    if not teams:
        print("No teams found.")
        return
    
    print("\n" + "=" * 100)
    print(f"{'ID':<12} | {'Team Name':<40} | {'Parent':<30} | {'Members'}")
    print("=" * 100)
    
    for team in teams:
        team_id = str(team.get("id") or team.get("_id") or "N/A")[:10]
        name = (team.get("name") or "N/A")[:38]
        
        # Get parent name
        parent = team.get("parent") or team.get("parentTeam") or team.get("parent_team")
        if isinstance(parent, dict):
            parent_name = parent.get("name", "")[:28]
        else:
            parent_name = ""
        
        # Count members if available
        member_count = 0
        for field in ["contributors", "members", "users", "team_members"]:
            members = team.get(field, [])
            if members:
                member_count = len(members)
                break
        
        member_str = str(member_count) if member_count > 0 else "-"
        
        print(f"{team_id:<12} | {name:<40} | {parent_name:<30} | {member_str}")
    
    print("=" * 100)


def save_to_json(data: list, filename: str) -> str:
    """Save data to a JSON file."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"💾 Saved to: {filename}")
    return filename


def save_users_to_csv(users: list, filename: str = "linearb_users.csv") -> str:
    """
    Save users to a CSV file with a clean, readable format.
    """
    import csv
    
    if not users:
        print("No users to save.")
        return filename
    
    fieldnames = ["id", "name", "email", "login", "active", "teams"]
    
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for user in users:
            row = {
                "id": user.get("id") or user.get("contributor_id") or "",
                "name": user.get("name") or user.get("display_name") or "",
                "email": user.get("email") or "",
                "login": user.get("login") or user.get("username") or "",
                "active": "Yes" if not user.get("deleted_at") else "No",
                "teams": get_team_names(user)
            }
            writer.writerow(row)
    
    print(f"📄 Saved users to: {filename}")
    return filename


def save_teams_to_csv(teams: list, filename: str = "linearb_teams.csv") -> str:
    """
    Save teams to a CSV file.
    """
    import csv
    
    if not teams:
        print("No teams to save.")
        return filename
    
    fieldnames = ["id", "name", "parent_name", "parent_id"]
    
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for team in teams:
            parent = team.get("parent") or team.get("parentTeam") or team.get("parent_team") or {}
            if isinstance(parent, dict):
                parent_name = parent.get("name", "")
                parent_id = parent.get("id") or parent.get("_id") or ""
            else:
                parent_name = ""
                parent_id = ""
            
            row = {
                "id": team.get("id") or team.get("_id") or "",
                "name": team.get("name") or "",
                "parent_name": parent_name,
                "parent_id": parent_id
            }
            writer.writerow(row)
    
    print(f"📄 Saved teams to: {filename}")
    return filename


def main():
    """
    Main function to fetch and display all LinearB users.
    """
    api_key = os.getenv("LINEARB_API_KEY")
    
    # No hardcoded fallback - require environment variable for security
    
    # Debug: show masked API key
    if api_key:
        masked = api_key[:6] + "..." + api_key[-4:] if len(api_key) > 10 else "***"
        print(f"🔑 Using API key: {masked}")
        print(f"   Key length: {len(api_key)} characters")
    
    if not api_key:
        print("❌ Error: LINEARB_API_KEY environment variable is not set.")
        print("\nTo set it:")
        print("  export LINEARB_API_KEY='your_api_key_here'")
        print("\nOr pass it inline:")
        print("  LINEARB_API_KEY='your_api_key' python fetch_linearb_users.py")
        return
    
    try:
        users = []
        users_from_api = False
        
        # Try to fetch users directly first
        users, users_from_api = fetch_all_users(api_key)
        
        if not users_from_api:
            print("\n📋 Trying alternative approach via Teams endpoint...")
        
        # Always fetch teams (for team info and possibly extracting contributors)
        teams = fetch_all_teams(api_key)
        
        # Display teams
        print("\n📊 Teams in your organization:")
        display_teams(teams)
        save_to_json(teams, "linearb_teams.json")
        save_teams_to_csv(teams)
        
        # If we got users from API, display them
        if users_from_api and users:
            print("\n👥 Users (from /api/v1/users):")
            display_users(users)
            save_to_json(users, "linearb_users.json")
            save_users_to_csv(users)
            
            # Print summary
            print(f"\n📊 Summary:")
            print(f"   Total users: {len(users)}")
            print(f"   Total teams: {len(teams)}")
            
            active_count = sum(1 for u in users if not u.get("deleted_at"))
            print(f"   Active users: {active_count}")
            print(f"   Inactive users: {len(users) - active_count}")
            
            users_with_teams = sum(1 for u in users if get_team_names(u))
            print(f"   Users with team(s): {users_with_teams}")
            print(f"   Users without team: {len(users) - users_with_teams}")
        else:
            # Try to extract contributors from teams
            contributors = extract_contributors_from_teams(teams)
            
            if contributors:
                print(f"\n👥 Contributors (extracted from teams data):")
                display_users(contributors)
                save_to_json(contributors, "linearb_contributors.json")
                save_users_to_csv(contributors, "linearb_contributors.csv")
                
                print(f"\n📊 Summary:")
                print(f"   Total teams: {len(teams)}")
                print(f"   Contributors found in teams: {len(contributors)}")
            else:
                print("\n" + "=" * 70)
                print("ℹ️  IMPORTANT: Your API key doesn't have access to the Users endpoint")
                print("=" * 70)
                print("""
To fetch individual users, you need to:

1. Go to LinearB Settings > API Tokens
2. Create a new API token OR edit your existing one
3. Ensure it has the 'users:read' permission/scope

Alternatively, contact your LinearB administrator to grant
your API token access to the users endpoint.

Current API key permissions appear to include:
  ✅ Teams (read)
  ❌ Users (read) - Access denied

For now, here's what we found from the Teams endpoint:
""")
                print(f"   Total teams: {len(teams)}")
                
                # List team names
                print("\n🏢 Teams in your organization:")
                for team in sorted(teams, key=lambda t: t.get("name", "")):
                    print(f"   • {team.get('name', 'Unknown')}")
        
    except LinearBError as e:
        print(f"❌ LinearB API Error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
