#!/usr/bin/env python3
"""
Example script for fetching FRED economic data

This script demonstrates how to use the FREDDataFetcher class to
retrieve economic indicators and save them to the data folder.
"""

import sys
import os
from pathlib import Path

# Add src directory to path to import fred_apis
sys.path.append(str(Path(__file__).parent / "src"))

from fred_apis import FREDDataFetcher

def fetch_all_data():
    """Fetch all economic indicators from Bruno files"""
    print("Starting FRED data fetching process...")
    
    # Initialize the fetcher
    fetcher = FREDDataFetcher(data_dir="data")
    
    # Fetch all indicators
    data = fetcher.fetch_all_indicators(
        bruno_dir="api/Economic Data",
        start_date="2020-01-01",  # Adjust date range as needed
        format="csv",
        delay=0.5  # Be respectful to the API
    )
    
    print(f"Successfully fetched data for {len(data)} indicators")
    return data

def fetch_recent_updates():
    """Fetch only recent data updates"""
    print("Updating recent data...")
    
    fetcher = FREDDataFetcher(data_dir="data")
    data = fetcher.update_data(days_back=30)
    
    print(f"Updated {len(data)} indicators with recent data")
    return data

def fetch_specific_indicator():
    """Example of fetching a specific economic indicator"""
    print("Fetching GDP data...")
    
    fetcher = FREDDataFetcher(api_key="5345a9b784e816d1110d3f47c97cdfd3", data_dir="data")
    
    # Fetch GDP data
    gdp_data = fetcher.fetch_series_data("GDP", start_date="2010-01-01")
    
    if gdp_data is not None:
        print(f"Fetched {len(gdp_data)} GDP observations")
        fetcher.save_data(gdp_data, "gdp_example", "json")
    else:
        print("Failed to fetch GDP data")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch FRED economic data")
    parser.add_argument("--mode", choices=["all", "update", "gdp"], default="all",
                       help="What to fetch: all data, recent updates, or GDP example")
    
    args = parser.parse_args()
    
    if args.mode == "all":
        fetch_all_data()
    elif args.mode == "update":
        fetch_recent_updates()
    elif args.mode == "gdp":
        fetch_specific_indicator()