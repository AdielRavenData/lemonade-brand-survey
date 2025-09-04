"""
Survey Tracker for Cloud Run
Simplified version that handles tracking processed surveys
"""

import logging
import re
import os
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from typing import Dict, Optional
from bigquery_client import BigqueryClient

logger = logging.getLogger(__name__)

class SurveyTracker:
    """
    Simplified Survey Tracker for Cloud Run
    Handles checking if surveys are already processed and marking them as processed
    """
    
    def __init__(self, project_id: str):
        """
        Initialize the survey tracker
        
        Args:
            project_id: Google Cloud Project ID
        """
        self.project_id = project_id
        
        # Initialize BigqueryClient with proper configuration
        # Define primary keys for tables that support upserts
        primary_keys = {
            'processed_brand_surveys': ['filename'],
            'processed_custom_surveys': ['filename']
        }
        
        # Define tables of interest for upsert operations
        tables_of_interest = ['processed_brand_surveys', 'processed_custom_surveys']
        
        # Initialize the BigqueryClient
        self.bq_client = BigqueryClient(primary_keys, tables_of_interest)
        self.client = self.bq_client.client  # Keep backwards compatibility
        
        # Table references
        self.brand_table_id = f"{project_id}.new_brand_survey.processed_brand_surveys"
        self.custom_table_id = f"{project_id}.new_custom_brand_survey.processed_custom_surveys"
        
        logger.info("🔍 Survey tracker initialized with BigqueryClient")



    def _extract_dma_from_filename(self, filename: str) -> Optional[str]:
        """Extract DMA from filename"""
        # Manual mapping for problematic filenames
        manual_mapping = {
            '[Lemonade] MMM _ Brand Tracker - Grand Junction, C.zip': 'Grand Junction et al, CO',
            '[Lemonade] MMM _ Brand Tracker - Tucson(Sierra Vis.zip': 'Tucson(Sierra Vista), AZ',
            '[Lemonade] MMM _ Brand Tracker - Corpus Christi, T.zip': 'Corpus Christi, TX',
            '[Lemonade] MMM _ Brand Tracker - Wichita Falls, TX.zip': 'Wichita Fls et al, TX-OK',
            '[Lemonade] MMM _ Brand Tracker - Colorado Springs,.zip': 'Colorado Sprgs et al, CO',
            '[Lemonade] MMM - Wichita Falls, TX-OK (Control).zip': 'Wichita Fls et al, TX-OK',
            '[Lemonade] MMM - Colorado Springs, CO (Control).zip': 'Colorado Sprgs et al, CO'
        }
        
        # Check manual mapping first
        if filename in manual_mapping:
            return manual_mapping[filename]
        
        # Pattern matching
        patterns = [
            r'MMM - ([^,]+, [A-Z]{2})',           # "MMM - Eugene, OR" (custom surveys)
            r'MMM _ Brand Tracker - ([^,]+, [A-Z]{2})', # "MMM _ Brand Tracker - Laredo, TX" (brand trackers)
            r'MMM - ([^,]+, [A-Z]{2}-[A-Z]{2})',  # "MMM - Tyler-Longview, TX" 
            r'MMM _ Brand Tracker - ([^,]+)',     # "MMM _ Brand Tracker - SomeName" (brand fallback)
            r'MMM - ([^(]+)',                     # "MMM - SomeName" (custom fallback)
            r'- ([^,]+, [A-Z]{2})',               # Legacy: "- Eugene, OR" 
            r'- ([^(]+)',                         # Legacy fallback: "- SomeName"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                geo = match.group(1).strip()
                
                # Clean up variations
                geo = geo.replace(' et al', '').replace('(', '').replace(')', '')
                
                # Handle special cases where state is missing
                if ', ' not in geo:
                    # Map common cities to states
                    city_to_state = {
                        'Chicago': 'IL', 'Nashville': 'TN', 'Eugene': 'OR',
                        'Amarillo': 'TX', 'Victoria': 'TX', 'Lima': 'OH',
                        'Laredo': 'TX', 'Dayton': 'OH', 'Grand Junction': 'CO',
                        'Colorado Springs': 'CO', 'Tucson': 'AZ', 'Denver': 'CO',
                        'Phoenix': 'AZ', 'Portland': 'OR', 'San Antonio': 'TX',
                        'Austin': 'TX', 'Memphis': 'TN', 'Knoxville': 'TN',
                        'Wheeling': 'WV', 'Youngstown': 'OH', 'Zanesville': 'OH',
                        'Tyler-Longview': 'TX', 'Abilene-Sweetwater': 'TX'
                    }
                    
                    # Try to find exact city match
                    for city, state in city_to_state.items():
                        if city.lower() == geo.lower():
                            geo = f"{city}, {state}"
                            break
                        elif city.lower() in geo.lower():
                            # Handle partial matches like "Tyler-Longview"
                            geo = f"{geo}, {state}"
                            break
                
                return geo
        
        return None

    def is_processed(self, filename: str, survey_type: str) -> bool:
        """
        Check if CSV file is already processed
        
        Args:
            filename: CSV filename to check
            survey_type: "BRAND_TRACKER" or "CUSTOM"
            
        Returns:
            True if already processed, False otherwise
        """
        try:
            table_id = self.brand_table_id if survey_type == "BRAND_TRACKER" else self.custom_table_id
            
            # Use BigqueryClient's read_table method with conditions
            df = self.bq_client.read_table(table_id, conditions=f"filename = '{filename}'")
            
            # Return True if any rows found
            return len(df) > 0
            
        except Exception as e:
            logger.error(f"Error checking if file is processed: {e}")
            # If we can't check, assume not processed to avoid skipping
            return False

    def mark_processed(self, filename: str, survey_type: str, metrics: Dict):
        """
        Mark CSV file as processed
        
        Args:
            filename: CSV filename
            survey_type: "BRAND_TRACKER" or "CUSTOM" 
            metrics: Metrics dictionary
        """
        try:
            table_id = self.brand_table_id if survey_type == "BRAND_TRACKER" else self.custom_table_id
            
            # Prepare simplified row data
            row_data = {
                'filename': filename,
                'processed_timestamp': datetime.now().isoformat(),
                'group_type': metrics.get('group_type'),
                'group_number': str(metrics.get('group_number', ''))
            }
            
            # Add question-specific response counts
            if survey_type == "BRAND_TRACKER":
                row_data.update({
                    'q1_response_count': int(metrics.get('q1_response_count', 0)),
                    'q2_response_count': int(metrics.get('q2_response_count', 0)),
                    'q3_response_count': int(metrics.get('q3_response_count', 0))
                })
            else:
                row_data.update({
                    'q1_response_count': int(metrics.get('q1_response_count', 0)),
                    'q2_response_count': int(metrics.get('q2_response_count', 0))
                })
            
            # Convert to DataFrame for BigqueryClient.load_table
            df = pd.DataFrame([row_data])
            
            # Use BigqueryClient's load_table method
            self.bq_client.load_table(
                data=df,
                table_id=table_id,
                detect_schema=True,
                to_truncate=False  # Don't truncate, just append
            )
            
            logger.info(f"✅ Marked {survey_type} file as processed: {filename}")
                
        except Exception as e:
            logger.error(f"Error marking file as processed: {e}")


