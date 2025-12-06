#!/usr/bin/env python3
"""Clear DSS recommendation cache"""
import asyncio
import sys
sys.path.insert(0, 'backend')

async def clear_cache():
    from app.core.cache import cache
    
    # Initialize cache
    await cache.init()
    
    print("Clearing DSS recommendation cache...")
    await cache.delete_pattern("dss_reco:*")
    print("✅ Cache cleared!")
    
    await cache.close()

if __name__ == "__main__":
    asyncio.run(clear_cache())
