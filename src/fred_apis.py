"""
FRED API Data Fetcher

This module fetches economic data from the Federal Reserve Economic Data (FRED) API
based on the Bruno API configuration files and stores the data in the data folder.
"""

import os
import json
import csv
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FREDDataFetcher:
    """
    Fetches economic data from FRED API based on Bruno configuration files
    """
    
    def __init__(self, api_key: Optional[str] = None, data_dir: str = "data"):
        """
        Initialize the FRED data fetcher
        
        Args:
            api_key: FRED API key (if None, will extract from Bruno files)
            data_dir: Directory to save the fetched data
        """
        self.api_key = api_key
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.base_url = "https://api.stlouisfed.org/fred/series/observations"
        
        # Series ID mappings for correct indicators
        self.series_mappings = {
            "CPI data": ["CPIAUCSL", "CPALTT01USM657N"],  # Consumer Price Index
            "GDP data": ["GDP", "GDPC1"],  # Gross Domestic Product
            "UNRATE data": ["UNRATE"],  # Unemployment Rate
            "Interest Rate data": ["INTDSRUSM193", "FEDFUNDS", "DGS10"],  # Interest Rates
            "Consumer Confidence data": ["UMCSENT"],  # Consumer Confidence
            "Housing Starts data": ["HOUST"],  # Housing Starts
            "Trade Balance data": ["BOPGSTB"],  # Trade Balance
            "Current Account data": ["NETEXP"],  # Current Account
            "Government Debt & Budget Deficit data": ["GFDEGDQ188S", "FYFSGDA188S"],  # Debt & Deficit
            "PPI data": ["PPIACO"],  # Producer Price Index
            "PPI Gold Ore data": ["PCU2122212122"],  # PPI Gold Ore
            "Crude Oil data": ["DCOILWTICO"],  # Crude Oil Prices
            "S&P 500 data": ["SP500"],  # S&P 500
            "Dow Jones Industrial Average data": ["DJIA"],  # Dow Jones
            "NASDAQ Composite data": ["NASDAQCOM"],  # NASDAQ
            "Currency Conversions data": ["DEXUSEU", "DEXJPUS", "DEXUSUK"]  # USD exchange rates
        }
    
    def parse_bruno_file(self, file_path: str) -> Dict[str, Any]:
        """
        Parse a Bruno API file to extract configuration
        
        Args:
            file_path: Path to the Bruno file
            
        Returns:
            Dictionary containing API configuration
        """
        config = {
            'name': '',
            'url': '',
            'series_id': '',
            'api_key': '',
            'params': {}
        }
        
        try:
            with open(file_path, 'r') as file:
                content = file.read()
                
            # Extract name
            name_match = re.search(r'name:\s*(.+)', content)
            if name_match:
                config['name'] = name_match.group(1).strip()
            
            # Extract URL
            url_match = re.search(r'url:\s*(.+)', content)
            if url_match:
                config['url'] = url_match.group(1).strip()
            
            # Extract API key from URL or params
            api_key_match = re.search(r'api_key[=:]([^&\s]+)', content)
            if api_key_match:
                config['api_key'] = api_key_match.group(1).strip()
                if not self.api_key:
                    self.api_key = config['api_key']
            
            # Extract series ID
            series_match = re.search(r'series_id[=:]([^&\s]+)', content)
            if series_match:
                config['series_id'] = series_match.group(1).strip()
                
        except Exception as e:
            logger.error(f"Error parsing Bruno file {file_path}: {e}")
            
        return config
    
    def get_corrected_series_ids(self, indicator_name: str) -> List[str]:
        """
        Get the correct series IDs for a given indicator
        
        Args:
            indicator_name: Name of the economic indicator
            
        Returns:
            List of series IDs for the indicator
        """
        return self.series_mappings.get(indicator_name, [])
    
    def fetch_series_data(self, series_id: str, start_date: str = "1990-01-01", 
                         end_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Fetch data for a specific FRED series
        
        Args:
            series_id: FRED series identifier
            start_date: Start date for data (YYYY-MM-DD format)
            end_date: End date for data (YYYY-MM-DD format, defaults to today)
            
        Returns:
            DataFrame with the series data or None if failed
        """
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
            
        params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'file_type': 'json',
            'observation_start': start_date,
            'observation_end': end_date
        }
        
        try:
            logger.info(f"Fetching data for series: {series_id}")
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'observations' not in data:
                logger.warning(f"No observations found for series {series_id}")
                return None
            
            # Convert to DataFrame
            observations = data['observations']
            df = pd.DataFrame(observations)
            
            if df.empty:
                logger.warning(f"Empty dataset for series {series_id}")
                return None
            
            # Clean and format data
            df['date'] = pd.to_datetime(df['date'])
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df.dropna(subset=['value'])  # Remove missing values
            df = df.sort_values('date')
            
            # Add metadata
            df['series_id'] = series_id
            df['fetched_at'] = datetime.now()
            
            logger.info(f"Successfully fetched {len(df)} observations for {series_id}")
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for series {series_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error processing series {series_id}: {e}")
            return None
    
    def save_data(self, data: pd.DataFrame, filename: str, format: str = "csv"):
        """
        Save DataFrame to specified format
        
        Args:
            data: DataFrame to save
            filename: Output filename (without extension)
            format: Output format ('csv', 'json', 'parquet')
        """
        file_path = self.data_dir / f"{filename}.{format}"
        
        try:
            if format == "csv":
                data.to_csv(file_path, index=False)
            elif format == "json":
                data.to_json(file_path, orient='records', date_format='iso', indent=2)
            elif format == "parquet":
                data.to_parquet(file_path, index=False)
            else:
                raise ValueError(f"Unsupported format: {format}")
                
            logger.info(f"Data saved to {file_path}")
            
        except Exception as e:
            logger.error(f"Error saving data to {file_path}: {e}")
    
    def fetch_all_indicators(self, bruno_dir: str = "api/Economic Data", 
                           start_date: str = "1990-01-01", 
                           format: str = "csv",
                           delay: float = 0.5) -> Dict[str, pd.DataFrame]:
        """
        Fetch data for all economic indicators from Bruno files
        
        Args:
            bruno_dir: Directory containing Bruno files
            start_date: Start date for data fetching
            format: Output format for saved files
            delay: Delay between API calls (seconds)
            
        Returns:
            Dictionary mapping indicator names to DataFrames
        """
        bruno_path = Path(bruno_dir)
        if not bruno_path.exists():
            logger.error(f"Bruno directory not found: {bruno_dir}")
            return {}
        
        all_data = {}
        bruno_files = list(bruno_path.glob("*.bru"))
        
        logger.info(f"Found {len(bruno_files)} Bruno files to process")
        
        for bruno_file in bruno_files:
            logger.info(f"Processing {bruno_file.name}")
            
            # Parse Bruno file
            config = self.parse_bruno_file(str(bruno_file))
            indicator_name = config['name'] or bruno_file.stem
            
            # Get correct series IDs for this indicator
            series_ids = self.get_corrected_series_ids(indicator_name)
            
            if not series_ids:
                # Fall back to series ID from Bruno file
                if config['series_id']:
                    series_ids = [config['series_id']]
                else:
                    logger.warning(f"No series ID found for {indicator_name}")
                    continue
            
            # Fetch data for each series ID
            indicator_data = []
            for series_id in series_ids:
                df = self.fetch_series_data(series_id, start_date)
                if df is not None:
                    indicator_data.append(df)
                
                # Rate limiting
                time.sleep(delay)
            
            if indicator_data:
                # Combine data from multiple series if needed
                if len(indicator_data) == 1:
                    combined_df = indicator_data[0]
                else:
                    combined_df = pd.concat(indicator_data, ignore_index=True)
                
                all_data[indicator_name] = combined_df
                
                # Save individual file
                clean_filename = re.sub(r'[^\w\-_]', '_', indicator_name.lower())
                self.save_data(combined_df, clean_filename, format)
                
            else:
                logger.warning(f"No data fetched for {indicator_name}")
        
        # Save combined dataset
        if all_data:
            logger.info("Creating combined dataset")
            combined_data = []
            for indicator, df in all_data.items():
                df_copy = df.copy()
                df_copy['indicator'] = indicator
                combined_data.append(df_copy)
            
            if combined_data:
                master_df = pd.concat(combined_data, ignore_index=True)
                self.save_data(master_df, "all_economic_indicators", format)
        
        logger.info(f"Data fetching completed. Processed {len(all_data)} indicators.")
        return all_data
    
    def update_data(self, days_back: int = 30, format: str = "csv"):
        """
        Update existing data with recent observations
        
        Args:
            days_back: Number of days back to fetch for updates
            format: Output format for saved files
        """
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        logger.info(f"Updating data from {start_date}")
        
        return self.fetch_all_indicators(start_date=start_date, format=format)


def main():
    """
    Main entry point for fetching FRED economic data
    """
    logger.info("Starting FRED data fetching process")
    
    # Initialize fetcher
    fetcher = FREDDataFetcher()
    
    # Fetch all data
    data = fetcher.fetch_all_indicators(
        bruno_dir="api/Economic Data",
        start_date="1990-01-01",  # Adjust as needed
        format="csv",  # Can be 'csv', 'json', or 'parquet'
        delay=0.5  # Delay between API calls
    )
    
    if data:
        logger.info("Data fetching completed successfully")
        logger.info(f"Fetched data for {len(data)} indicators")
        
        # Print summary
        for indicator, df in data.items():
            logger.info(f"{indicator}: {len(df)} observations, "
                       f"date range: {df['date'].min()} to {df['date'].max()}")
    else:
        logger.error("No data was fetched")


if __name__ == "__main__":
    main()
