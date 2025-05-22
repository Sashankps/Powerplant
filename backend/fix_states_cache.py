#!/usr/bin/env python3
"""
Script to clear the states cache to force a refresh.
"""
import sys
import traceback
from app.routes.power_plants import states_cache
from app.services import get_s3_client, get_data_from_s3
from app.utils.logger import logger, log_audit
import asyncio

async def fix_states_cache():
    """
    Clear and refresh the states cache.
    """
    global states_cache
    
    try:
        print("Current states_cache value:", states_cache)
        
        # Clear the cache
        print("Clearing states cache...")
        from app.routes.power_plants import states_cache
        states_cache = None
        print("States cache cleared")
        
        # Fetch fresh data
        print("Fetching fresh data from S3...")
        s3_client = get_s3_client()
        data = await get_data_from_s3(s3_client)
        
        if data.empty:
            print("No data found in S3")
            return
        
        # Get unique states
        states = data["PSTATEABB"].unique().tolist()
        states.sort()
        
        # Update cache manually
        states_cache = states
        
        print(f"States cache updated with {len(states)} states: {', '.join(states)}")
        log_audit("system", "UPDATE", "states_cache", "SUCCESS", f"Updated with {len(states)} states")
        
    except Exception as e:
        print(f"Error fixing states cache: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(fix_states_cache()) 