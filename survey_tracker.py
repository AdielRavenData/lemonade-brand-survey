"""
Survey Tracker for Cloud Run
Simplified version that handles tracking processed surveys
"""

import logging
import pandas as pd
from datetime import datetime
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

    def mark_processed(self, filename: str, survey_type: str):
        """
        Mark CSV file as processed with simple filename-only schema
        
        Args:
            filename: CSV filename
            survey_type: "BRAND_TRACKER" or "CUSTOM"
        """
        try:
            table_id = self.brand_table_id if survey_type == "BRAND_TRACKER" else self.custom_table_id
            
            # Simple row data with just filename
            row_data = {
                'filename': filename
            }
            
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


