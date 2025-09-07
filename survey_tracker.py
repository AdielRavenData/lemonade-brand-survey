"""
Survey Tracker for Cloud Run
Simplified version that handles tracking processed surveys
"""

import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Optional
import re
from cloud_utils.bigquery_client import BigqueryClient

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
        
        # Initialize the BigqueryClient (Raven Utils version)
        self.bq_client = BigqueryClient(creds=None, config={})
        self.client = self.bq_client.client  # Keep backwards compatibility
        
        # Table references
        self.brand_table_id = f"{project_id}.new_brand_survey.processed_brand_surveys"
        self.custom_table_id = f"{project_id}.new_custom_brand_survey.processed_custom_surveys"
        
        logger.info("🔍 Survey tracker initialized with BigqueryClient")
    
    def _create_file_identifier(self, filename: str, survey_type: str) -> str:
        """
        Create simplified identifier from filename
        
        Args:
            filename: Original CSV filename
            survey_type: "BRAND_TRACKER" or "CUSTOM"
            
        Returns:
            Simplified identifier like: brand_1185555613288419408_2025-03-18 or custom_1185555613288419408_2025-03-18
        """
        try:
            # Extract study ID
            study_match = re.search(r'\[Study\s+(\d+)\]', filename)
            study_id = study_match.group(1) if study_match else "unknown"
            
            # Extract date from timestamp (YYYY-MM-DD format)
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
            date_part = date_match.group(1) if date_match else "unknown"
            
            # Create prefix based on survey type
            prefix = "brand" if survey_type == "BRAND_TRACKER" else "custom"
            
            # Create identifier: prefix_studyid_date
            identifier = f"{prefix}_{study_id}_{date_part}"
            
            logger.debug(f"📝 Created identifier: {filename} -> {identifier}")
            return identifier
            
        except Exception as e:
            logger.error(f"Error creating identifier for {filename}: {e}")
            # Fallback identifier
            prefix = "brand" if survey_type == "BRAND_TRACKER" else "custom"
            return f"{prefix}_unknown_{datetime.now().strftime('%Y-%m-%d')}"


    def is_processed(self, filename: str, survey_type: str) -> bool:
        """
        Check if CSV file is already processed using simplified identifier
        
        Args:
            filename: CSV filename to check
            survey_type: "BRAND_TRACKER" or "CUSTOM"
            
        Returns:
            True if already processed, False otherwise
        """
        try:
            # Create identifier from filename
            file_identifier = self._create_file_identifier(filename, survey_type)
            table_id = self.brand_table_id if survey_type == "BRAND_TRACKER" else self.custom_table_id
            
            # Use BigqueryClient's run_query_to_df method
            query = f"SELECT file FROM `{table_id}` WHERE file = '{file_identifier}'"
            df = self.bq_client.run_query_to_df(query, log_msg=f"Checking if {file_identifier} is processed")
            
            # Return True if any rows found
            is_found = len(df) > 0
            if is_found:
                logger.info(f"✅ File already processed: {file_identifier}")
            else:
                logger.info(f"🆕 New file to process: {file_identifier}")
            return is_found
            
        except Exception as e:
            logger.error(f"Error checking if file is processed: {e}")
            # Log the table we were trying to query for debugging
            logger.error(f"Failed to query table: {table_id}")
            # If we can't check BigQuery, assume not processed to avoid data loss
            # This is safer than assuming processed and skipping files
            return False

    def mark_processed(self, filename: str, survey_type: str):
        """
        Mark CSV file as processed with new schema: file, processed_date
        
        Args:
            filename: CSV filename
            survey_type: "BRAND_TRACKER" or "CUSTOM"
        """
        try:
            # Create identifier from filename
            file_identifier = self._create_file_identifier(filename, survey_type)
            table_id = self.brand_table_id if survey_type == "BRAND_TRACKER" else self.custom_table_id
            
            # Row data with file identifier and processed date
            row_data = {
                'file': file_identifier,
                'processed_date': datetime.now().strftime('%Y-%m-%d')
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
            
            logger.info(f"✅ Marked {survey_type} file as processed: {file_identifier}")
                
        except Exception as e:
            logger.error(f"Error marking file as processed: {e}")
            logger.error(f"Failed to process: {filename} -> {self._create_file_identifier(filename, survey_type)}")