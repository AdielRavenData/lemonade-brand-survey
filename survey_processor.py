"""
Survey Processor for Cloud Run
Handles ZIP file processing and BigQuery data upload
Uses the proven logic from production_ready_processor.py and custom_data_cleaner.py
"""

import pandas as pd
import zipfile
import logging
import tempfile
import psutil
import os
from pathlib import Path
from datetime import datetime
from google.cloud import bigquery
import re

from custom_data_cleaner import CustomSurveyDataCleaner
from survey_tracker import SurveyTracker
from cloud_utils.bigquery_client import BigqueryClient

logger = logging.getLogger(__name__)

class SurveyProcessor:
    """
    Cloud-ready survey processor that handles single ZIP files
    and uploads results to BigQuery tables
    """
    
    def __init__(self, project_id, brand_dataset, custom_dataset):
        """Initialize the processor"""
        self.project_id = project_id
        self.brand_dataset = brand_dataset
        self.custom_dataset = custom_dataset
        
        # Initialize BigqueryClient (Raven Utils version)
        # Using minimal configuration for append-only operations
        self.bq_client = BigqueryClient(creds=None, config={})
        self.client = self.bq_client.client  # Keep backwards compatibility
        
        # Initialize tracker and cleaner
        self.tracker = SurveyTracker(project_id)
        self.cleaner = CustomSurveyDataCleaner()
        
        # Load DMA mapping (embedded to avoid file dependencies)
        self.dma_lookup = self._create_dma_mapping()
        
        # PRODUCTION COLUMN MAPPING (verified across ALL 90 ZIPs)
        self.brand_tracker_columns = {
            'q1': 'Q[1] - CHOOSE_MULTIPLE - Which of the following insurance companies have you heard of?',
            'q2': 'Q[2] - CHOOSE_MULTIPLE - Which of the following insurance companies would you consider purchasing?',
            'q3': 'Q[3] - CHOOSE_ONE - Which of the following insurance companies are you most likely to purchase in the next 3 months?'
        }
        
        self.custom_survey_columns = {
            'q1': 'Q[1] - OPEN_ENDED - When you think of an online insurance brand, what is the first brand that comes to mind? (please only indicate one brand)',
            'q2': 'Q[2] - OPEN_ENDED - What other online insurance brands do you know?'
        }
        
        logger.info("🚀 Survey processor initialized for Cloud Run")

    def _get_table_schema(self, table_name):
        """Get explicit BigQuery schema for tables to prevent autodetect conflicts"""
        if table_name == 'brand_responses':
            return [
                bigquery.SchemaField("age", "STRING"),
                bigquery.SchemaField("gender", "STRING"),
                bigquery.SchemaField("geo", "STRING"),
                bigquery.SchemaField("client_type", "STRING"),
                bigquery.SchemaField("recorded_timestamp", "STRING"),
                bigquery.SchemaField("session_weight", "FLOAT"),
                bigquery.SchemaField("survey_date", "STRING"),  # Force STRING to match existing table
                bigquery.SchemaField("processed_date", "STRING"),  # Force STRING to match existing table
                bigquery.SchemaField("q1_answer", "STRING"),
                bigquery.SchemaField("q2_answer", "STRING"),
                bigquery.SchemaField("q3_answer", "STRING"),
                bigquery.SchemaField("study_number", "STRING"),
                bigquery.SchemaField("Group_type", "STRING"),
                bigquery.SchemaField("Group", "STRING"),
            ]
        elif table_name == 'custom_responses':
            return [
                bigquery.SchemaField("age", "STRING"),
                bigquery.SchemaField("gender", "STRING"),
                bigquery.SchemaField("geo", "STRING"),
                bigquery.SchemaField("client_type", "STRING"),
                bigquery.SchemaField("recorded_timestamp", "STRING"),
                bigquery.SchemaField("session_weight", "FLOAT"),
                bigquery.SchemaField("survey_date", "STRING"),  # Force STRING to match existing table
                bigquery.SchemaField("processed_date", "STRING"),  # Force STRING to match existing table
                bigquery.SchemaField("q1_answer", "STRING"),
                bigquery.SchemaField("q2_answer", "STRING"),
                bigquery.SchemaField("q1_cleaned", "STRING"),
                bigquery.SchemaField("q2_cleaned", "STRING"),
                bigquery.SchemaField("study_number", "STRING"),
                bigquery.SchemaField("Group_type", "STRING"),
                bigquery.SchemaField("Group", "STRING"),
            ]
        return None

    def _create_dma_mapping(self):
        """Create DMA mapping (embedded data from complete dma-type-num.csv)"""
        dma_data = [
            ["Austin, TX", "TEST", 1],
            ["Phoenix et al, AZ", "TEST", 1],
            ["Denver, CO", "TEST", 1],
            ["Portland, OR", "TEST", 1],
            ["San Antonio, TX", "TEST", 1],
            ["Nashville, TN", "CONTROL", 1],
            ["Cleveland et al, OH", "CONTROL", 1],
            ["Waco-Temple-Bryan, TX", "CONTROL", 1],
            ["Cincinnati, OH", "CONTROL", "both"],
            ["Tucson(Sierra Vista), AZ", "CONTROL", 1],
            ["Colorado Sprgs et al, CO", "CONTROL", 1],
            ["Knoxville, TN", "CONTROL", 1],
            ["Memphis, TN", "CONTROL", 1],
            ["Eugene, OR", "CONTROL", 1],
            ["El Paso et al, TX-NM", "CONTROL", 1],
            ["Spokane, WA", "CONTROL", 1],
            ["Tyler-Longview et al, TX", "CONTROL", 1],
            ["Dayton, OH", "CONTROL", 1],
            ["Lubbock, TX", "CONTROL", 1],
            ["Chattanooga, TN", "CONTROL", 1],
            ["Toledo, OH", "CONTROL", 1],
            ["Yakima et al, WA", "CONTROL", "both"],
            ["Tri-Cities, TN-VA", "CONTROL", 1],
            ["Rockford, IL", "CONTROL", 1],
            ["Youngstown, OH", "CONTROL", "both"],
            ["Amarillo, TX", "CONTROL", "both"],
            ["Beaumont-Port Arthur, TX", "CONTROL", 1],
            ["Abilene-Sweetwater, TX", "CONTROL", 1],
            ["Sherman-Ada, TX-OK", "CONTROL", 1],
            ["Wichita Fls et al, TX-OK", "CONTROL", 1],
            ["San Angelo, TX", "CONTROL", "both"],
            ["Corpus Christi, TX", "CONTROL", 1],
            ["Grand Junction et al, CO", "CONTROL", "both"],
            ["Laredo, TX", "CONTROL", 1],
            ["Yuma-El Centro, AZ-CA", "CONTROL", "both"],
            ["Jackson, TN", "CONTROL", 1],
            ["Victoria, TX", "CONTROL", 1],
            ["Wheeling et al, WV-OH", "CONTROL", 1],
            ["Lima, OH", "CONTROL", 1],
            ["Zanesville, OH", "CONTROL", "both"],
            ["Chicago, IL", "TEST", 2],
            ["Champaign et al, IL", "CONTROL", 2],
            ["Peoria-Bloomington, IL", "CONTROL", 2],
            ["Davenport et al, IA-IL", "CONTROL", 2],
            ["Minot et al, ND", "CONTROL", 2]
        ]
        
        dma_lookup = {}
        for dma, group_type, group in dma_data:
            dma_lookup[dma] = {
                'Group_type': group_type,
                'Group': str(group)
            }
        
        return dma_lookup

    def extract_dma_from_filename(self, filename):
        """Extract DMA from filename with pattern matching"""
        # Minimal manual mapping for truncated/problematic filenames only
        manual_mapping = {
            '[Lemonade] MMM _ Brand Tracker - Colorado Springs,.zip': 'Colorado Sprgs et al, CO',
            '[Lemonade] MMM _ Brand Tracker - Corpus Christi, T.zip': 'Corpus Christi, TX',
            '[Lemonade] MMM _ Brand Tracker - Grand Junction, C.zip': 'Grand Junction et al, CO',
            '[Lemonade] MMM _ Brand Tracker - Tucson(Sierra Vis.zip': 'Tucson(Sierra Vista), AZ',
            '[Lemonade] MMM _ Brand Tracker - Wichita Falls, TX.zip': 'Wichita Fls et al, TX-OK',
            '[Lemonade] MMM - Colorado Springs, CO (Control).zip': 'Colorado Sprgs et al, CO',
            '[Lemonade] MMM - Wichita Falls, TX-OK (Control).zip': 'Wichita Fls et al, TX-OK'
        }
        
        # Check minimal manual mapping first
        if filename in manual_mapping:
            return manual_mapping[filename]
        
        patterns = [
            r'([A-Za-z\s\-()]+),\s*([A-Z]{2}(?:-[A-Z]{2})?)',  # "City, TX" or "City, TX-OK"
            r'([A-Za-z\s\-()]+),\s*([A-Z]{2})',                # "City, TX"
            r'- ([A-Za-z\s\-()]+), ([A-Z]{2})',                # "- City, TX"
            r'- ([A-Za-z\s\-()]+)-([A-Za-z\s]+)',              # "- City-State"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                city_part, state_part = match.groups()
                city_clean = city_part.strip().lower()
                
                # Handle common truncations
                truncation_fixes = {
                    'wate': 'water',
                    ' art': ' arthur',
                    'contr': 'control',
                    'sweetwate': 'sweetwater'
                }
                
                for old, new in truncation_fixes.items():
                    if city_clean.endswith(old):
                        city_clean = city_clean.replace(old, new)
                
                # Smart matching with DMA mapping
                for dma_key in self.dma_lookup.keys():
                    if ',' in dma_key:
                        dma_city = dma_key.split(',')[0].strip().lower()
                        # Exact match or partial match
                        if (city_clean == dma_city or 
                            city_clean in dma_city or 
                            dma_city in city_clean or
                            any(word in dma_city for word in city_clean.split('-') if len(word) > 3)):
                            return dma_key
        
        logger.warning(f"Could not extract DMA from: {filename}")
        return None

    def extract_survey_date_from_filename(self, filename):
        """Extract survey date from filename timestamp"""
        try:
            # Look for pattern like: 2025-03-17T19:31:41.076Z
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})T', filename)
            if date_match:
                return date_match.group(1)
            else:
                logger.warning(f"⚠️ Could not extract survey date from filename: {filename}")
                return None
        except Exception as e:
            logger.error(f"❌ Error extracting survey date from {filename}: {e}")
            return None

    def extract_study_id_from_csv_name(self, csv_name):
        """Extract study ID from CSV filename"""
        match = re.search(r'\[Study\s+(\d+)\]', csv_name)
        if match:
            return f"id_{match.group(1)}"
        return f"id_{hash(csv_name) % 10000000000000000000}"

    def split_and_explode(self, df, columns, delimiter=';'):
        """Split and explode multiple choice answers"""
        df_copy = df.copy()
        
        for col in columns:
            if col in df_copy.columns:
                # Handle NaN values
                df_copy[col] = df_copy[col].fillna('')
                df_copy[col] = df_copy[col].astype(str)
                
                # Split and explode
                df_copy[col] = df_copy[col].str.split(delimiter)
                df_copy = df_copy.explode(col)
                
                # Clean up
                df_copy[col] = df_copy[col].str.strip()
                df_copy = df_copy[df_copy[col] != '']
                df_copy = df_copy[df_copy[col] != 'nan']
        
        return df_copy.reset_index(drop=True)

    def clean_brand_name(self, brand):
        """Clean brand names to standardized format"""
        if pd.isna(brand) or brand == '' or brand == 'nan':
            return brand
        
        brand = str(brand).strip()
        
        # Standard brand name mapping
        brand_mapping = {
            'allstate': 'Allstate',
            'geico': 'Geico', 
            'progressive': 'Progressive',
            'state farm': 'State Farm',
            'liberty mutual': 'Liberty Mutual',
            'farmers': 'Farmers',
            'usaa': 'USAA',
            'nationwide': 'Nationwide',
            'american family': 'American Family',
            'the general': 'The General',
            'esurance': 'Esurance',
            'travelers': 'Travelers',
            'safeco': 'Safeco',
            'hartford': 'Hartford'
        }
        
        brand_lower = brand.lower()
        for key, value in brand_mapping.items():
            if key in brand_lower:
                return value
        
        return brand

    def process_brand_tracker_csv(self, df, dma, dma_info, study_id, csv_filename):
        """Process brand tracker CSV with both survey_date and processed_date"""
        brand_responses = []
        
        # Extract survey date from filename
        survey_date = self.extract_survey_date_from_filename(csv_filename)
        if not survey_date:
            survey_date = "Unknown"
        
        # Use today's date as processed_date (as string to match BigQuery schema)
        processed_date = datetime.now().date().isoformat()
        
        for _, row in df.iterrows():
            brand_row = {
                'age': row.get('Age', ''),
                'gender': row.get('Gender', ''),
                'geo': dma,
                'client_type': row.get('Client Type', ''),
                'recorded_timestamp': row.get('Recorded Timestamp', ''),
                'session_weight': float(row.get('Session Weight', 1.0)),
                'survey_date': survey_date,
                'processed_date': processed_date,
                'q1_answer': row.get(self.brand_tracker_columns['q1'], ''),
                'q2_answer': row.get(self.brand_tracker_columns['q2'], ''),
                'q3_answer': row.get(self.brand_tracker_columns['q3'], ''),
                'study_number': study_id,
                'Group_type': dma_info['Group_type'],
                'Group': dma_info['Group']
            }
            brand_responses.append(brand_row)
        
        return pd.DataFrame(brand_responses)

    def create_brand_question_tables(self, brand_df):
        """Create brand awareness, consideration, intent tables"""
        if brand_df is None or brand_df.empty:
            logger.warning("❌ Brand DF is None or empty for question tables")
            return {}
        
        logger.info(f"🔍 Creating question tables from {len(brand_df)} brand responses")
        logger.info(f"🔍 Available columns: {list(brand_df.columns)}")
        
        question_mapping = {'q1': 'awareness', 'q2': 'consideration', 'q3': 'intent'}
        question_tables = {}
        
        for q_num, table_name in question_mapping.items():
            answer_col = f'{q_num}_answer'
            if answer_col not in brand_df.columns:
                logger.warning(f"❌ Column {answer_col} not found in brand data")
                continue
                
            # Filter data with valid answers
            question_data = brand_df[
                brand_df[answer_col].notna() & 
                (brand_df[answer_col] != '') & 
                (brand_df[answer_col] != 'nan')
            ].copy()
            
            if question_data.empty:
                logger.warning(f"❌ No valid data for {table_name} (column: {answer_col})")
                continue
            else:
                logger.info(f"✅ Found {len(question_data)} valid responses for {table_name}")
            
            # Explode multiple choices (Q1 and Q2), Q3 is single choice
            if q_num in ['q1', 'q2']:
                question_data = self.split_and_explode(question_data, [answer_col], delimiter=';')
            
            # Clean brand names
            question_data[answer_col] = question_data[answer_col].apply(self.clean_brand_name)
            
            # Add survey_dates
            question_data['survey_dates'] = question_data['survey_date'].astype(str)
            
            # Group and count - PRODUCTION OPTIMIZED
            group_columns = [
                'age', 'gender', 'geo', 'client_type', 'session_weight',
                'survey_dates', 'survey_date', 'processed_date', 'study_number', answer_col, 'Group_type', 'Group'
            ]
            
            # Efficient counting
            question_grouped = question_data.groupby(group_columns).size().reset_index(name='count_response')
            
            # Calculate weighted response (keep as FLOAT to match BigQuery schema)
            question_grouped['Weighted_Response'] = (
                question_grouped['session_weight'] * question_grouped['count_response']
            ).astype(float)
            
            question_tables[table_name] = question_grouped
        
        return question_tables

    def process_custom_survey_csv(self, df, dma, dma_info, study_id, csv_filename):
        """Process custom survey CSV with both survey_date and processed_date"""
        custom_responses = []
        
        # Extract survey date from filename
        survey_date = self.extract_survey_date_from_filename(csv_filename)
        if not survey_date:
            survey_date = "Unknown"
        
        # Use today's date as processed_date (as string to match BigQuery schema)
        processed_date = datetime.now().date().isoformat()
        
        for _, row in df.iterrows():
            q1_raw = row.get(self.custom_survey_columns['q1'], '')
            q2_raw = row.get(self.custom_survey_columns['q2'], '')
            
            custom_row = {
                'age': row.get('Age', ''),
                'gender': row.get('Gender', ''),
                'geo': dma,
                'client_type': row.get('Client Type', ''),
                'recorded_timestamp': row.get('Recorded Timestamp', ''),
                'session_weight': float(row.get('Session Weight', 1.0)),
                'survey_date': survey_date,
                'processed_date': processed_date,
                'q1_answer': q1_raw,
                'q2_answer': q2_raw,
                'q1_cleaned': str(self.cleaner.clean_brand_response(q1_raw) or ''),
                'q2_cleaned': ', '.join([str(b) for b in self.cleaner.split_multiple_brands(q2_raw) if b]),
                'study_number': study_id,
                'Group_type': dma_info['Group_type'],
                'Group': dma_info['Group']
            }
            custom_responses.append(custom_row)
        
        return pd.DataFrame(custom_responses)

    def create_custom_question_tables(self, custom_df):
        """Create custom top_of_mind and knowledge tables"""
        if custom_df is None or custom_df.empty:
            return {}
        
        custom_tables = {}
        
        # Top of mind (Q1)
        if 'q1_cleaned' in custom_df.columns:
            q1_data = custom_df[
                custom_df['q1_cleaned'].notna() & 
                (custom_df['q1_cleaned'] != '') & 
                (custom_df['q1_cleaned'] != 'UNMAPPED_RESPONSE')
            ].copy()
            
            if not q1_data.empty:
                q1_data['survey_dates'] = q1_data['survey_date'].astype(str)
                
                group_columns = [
                    'age', 'gender', 'geo', 'client_type', 'session_weight',
                    'survey_dates', 'survey_date', 'processed_date', 'study_number', 'q1_cleaned', 'Group_type', 'Group'
                ]
                
                q1_grouped = q1_data.groupby(group_columns).size().reset_index(name='count_response')
                q1_grouped['Weighted_Response'] = (q1_grouped['session_weight'] * q1_grouped['count_response']).astype(float)
                
                custom_tables['top_of_mind'] = q1_grouped
        
        # Knowledge (Q2)
        if 'q2_cleaned' in custom_df.columns:
            q2_data = custom_df[
                custom_df['q2_cleaned'].notna() & 
                (custom_df['q2_cleaned'] != '') & 
                (custom_df['q2_cleaned'] != 'UNMAPPED_RESPONSE')
            ].copy()
            
            if not q2_data.empty:
                # Split and explode Q2 (multiple brands)
                q2_data['q2_individual'] = q2_data['q2_cleaned'].str.split(',')
                q2_exploded = q2_data.explode('q2_individual')
                q2_exploded['q2_individual'] = q2_exploded['q2_individual'].str.strip()
                q2_exploded = q2_exploded[
                    (q2_exploded['q2_individual'] != '') & 
                    (q2_exploded['q2_individual'] != 'UNMAPPED_RESPONSE')
                ]
                
                if not q2_exploded.empty:
                    q2_exploded['survey_dates'] = q2_exploded['survey_date'].astype(str)
                    
                    group_columns = [
                        'age', 'gender', 'geo', 'client_type', 'session_weight',
                        'survey_dates', 'survey_date', 'processed_date', 'study_number', 'q2_individual', 'Group_type', 'Group'
                    ]
                    
                    q2_grouped = q2_exploded.groupby(group_columns).size().reset_index(name='count_response')
                    q2_grouped['Weighted_Response'] = (q2_grouped['session_weight'] * q2_grouped['count_response']).astype(float)
                    
                    custom_tables['knowledge'] = q2_grouped
        
        return custom_tables

    def upload_to_bigquery(self, df, table_name, dataset_id):
        """Upload DataFrame to BigQuery using BigqueryClient.load_table"""
        if df.empty:
            logger.warning(f"Empty DataFrame for table {table_name}")
            return 0
        
        table_id = f"{self.project_id}.{dataset_id}.{table_name}"
        
        try:
            # Create a copy to avoid modifying original DataFrame
            df_upload = df.copy()
            
            # Basic data type fixes for compatibility
            if 'Group' in df_upload.columns:
                df_upload['Group'] = df_upload['Group'].astype(str)
            
            if 'session_weight' in df_upload.columns:
                df_upload['session_weight'] = pd.to_numeric(df_upload['session_weight'], errors='coerce').fillna(1.0)
            
            if 'count_response' in df_upload.columns:
                df_upload['count_response'] = pd.to_numeric(df_upload['count_response'], errors='coerce').fillna(0).astype(int)
            
            if 'Weighted_Response' in df_upload.columns:
                df_upload['Weighted_Response'] = pd.to_numeric(df_upload['Weighted_Response'], errors='coerce').fillna(0.0).astype(float)
            
            # Convert datetime columns to string
            datetime_columns = df_upload.select_dtypes(include=['datetime64']).columns
            for col in datetime_columns:
                df_upload[col] = df_upload[col].astype(str)
            
            # Ensure date columns are strings
            date_columns = ['survey_date', 'processed_date', 'survey_dates']
            for col in date_columns:
                if col in df_upload.columns:
                    df_upload[col] = df_upload[col].astype(str)
                    # Clean up date format
                    df_upload[col] = df_upload[col].str.replace(r'\s.*', '', regex=True)
            
            logger.info(f"🔍 Uploading {len(df_upload)} rows to {table_name} using BigqueryClient")
            
            # Use BigqueryClient.load_table method
            self.bq_client.load_table(
                data=df_upload,
                table_id=table_id,
                detect_schema=True,  # Let BigQuery detect schema
                to_truncate=False    # Append mode
            )
            
            logger.info(f"✅ Successfully uploaded {len(df_upload)} rows to {table_name}")
            return len(df_upload)
            
        except Exception as e:
            logger.error(f"❌ Failed to upload to {table_name}: {e}")
            
            # Fallback: Try with essential columns only
            try:
                logger.warning(f"🔄 Trying fallback upload with essential columns for {table_name}")
                essential_columns = [
                    'age', 'gender', 'geo', 'client_type', 'session_weight', 
                    'study_number', 'survey_date', 'processed_date', 'Group_type', 'Group'
                ]
                available_columns = [col for col in essential_columns if col in df.columns]
                
                if available_columns:
                    minimal_df = df[available_columns].copy()
                    if 'Group' in minimal_df.columns:
                        minimal_df['Group'] = minimal_df['Group'].astype(str)
                    
                    self.bq_client.load_table(
                        data=minimal_df,
                        table_id=table_id,
                        detect_schema=True,
                        to_truncate=False
                    )
                    
                    logger.info(f"✅ Fallback upload successful: {len(minimal_df)} rows to {table_name}")
                    return len(minimal_df)
            except Exception as fallback_error:
                logger.error(f"❌ Fallback upload also failed for {table_name}: {fallback_error}")
            
            raise

    def process_zip_with_individual_tracking(self, zip_path, original_filename, survey_type, tracker):
        """
        Process ZIP file with individual CSV file tracking
        Only processes CSV files that haven't been processed before
        
        Args:
            zip_path (Path): Path to ZIP file
            original_filename (str): Original filename for tracking
            survey_type (str): "BRAND_TRACKER" or "CUSTOM"
            tracker: SurveyTracker instance
        
        Returns:
            dict: Processing result with detailed stats
        """
        try:
            # Log initial memory usage
            process = psutil.Process(os.getpid())
            start_memory = process.memory_info().rss / 1024 / 1024
            logger.info(f"🧠 Starting memory usage: {start_memory:.1f} MB")
            logger.info(f"🔄 Processing {survey_type} ZIP with individual CSV tracking: {original_filename}")
            
            # Extract DMA from ZIP filename
            dma = self.extract_dma_from_filename(original_filename)
            if not dma:
                dma = "Unknown DMA"
                dma_info = {'Group_type': 'UNKNOWN', 'Group': 'UNKNOWN'}
            else:
                dma_info = self.dma_lookup.get(dma, {'Group_type': 'UNKNOWN', 'Group': 'UNKNOWN'})
            
            # Extract ZIP to temporary directory
            with tempfile.TemporaryDirectory() as temp_dir:
                extract_dir = Path(temp_dir) / "extract"
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                
                # Find CSV files
                csv_files = list(extract_dir.rglob("*.csv"))
                if not csv_files:
                    raise ValueError("No CSV files found in ZIP")
                
                logger.info(f"📄 Found {len(csv_files)} CSV files in ZIP")
                
                # Track processing stats
                csv_files_processed = []
                csv_files_skipped = []
                tables_updated = []
                records_added = {}
                
                for csv_file in csv_files:
                    csv_filename = csv_file.name
                    logger.info(f"🔍 Checking CSV: {csv_filename}")
                    
                    # Check if this specific CSV was already processed
                    if tracker.is_processed(csv_filename, survey_type):
                        logger.info(f"⏭️  Already processed: {csv_filename}")
                        csv_files_skipped.append(csv_filename)
                        continue
                    
                    # Process this CSV file
                    logger.info(f"🔄 Processing CSV: {csv_filename}")
                    try:
                        study_id = self.extract_study_id_from_csv_name(csv_filename)
                        df = pd.read_csv(csv_file)
                        logger.info(f"📊 Loaded CSV with {len(df)} rows, {df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
                        
                        if survey_type == "BRAND_TRACKER":
                            # Process brand tracker data
                            brand_data = self.process_brand_tracker_csv(df, dma, dma_info, study_id, csv_filename)
                            
                            if brand_data is not None and not brand_data.empty:
                                # Upload brand_responses
                                count = self.upload_to_bigquery(brand_data, 'brand_responses', self.brand_dataset)
                                if 'brand_responses' not in records_added:
                                    records_added['brand_responses'] = 0
                                    tables_updated.append('brand_responses')
                                records_added['brand_responses'] += count
                                
                                # Create and upload question tables
                                question_tables = self.create_brand_question_tables(brand_data)
                                for table_name, table_df in question_tables.items():
                                    if not table_df.empty:
                                        count = self.upload_to_bigquery(table_df, table_name, self.brand_dataset)
                                        if table_name not in records_added:
                                            records_added[table_name] = 0
                                            tables_updated.append(table_name)
                                        records_added[table_name] += count
                        
                        else:  # CUSTOM survey
                            # Process custom survey data
                            custom_data = self.process_custom_survey_csv(df, dma, dma_info, study_id, csv_filename)
                            
                            if custom_data is not None and not custom_data.empty:
                                # Upload custom_responses
                                count = self.upload_to_bigquery(custom_data, 'custom_responses', self.custom_dataset)
                                if 'custom_responses' not in records_added:
                                    records_added['custom_responses'] = 0
                                    tables_updated.append('custom_responses')
                                records_added['custom_responses'] += count
                                
                                # Create and upload custom question tables
                                custom_tables = self.create_custom_question_tables(custom_data)
                                for table_name, table_df in custom_tables.items():
                                    if not table_df.empty:
                                        count = self.upload_to_bigquery(table_df, table_name, self.custom_dataset)
                                        if table_name not in records_added:
                                            records_added[table_name] = 0
                                            tables_updated.append(table_name)
                                        records_added[table_name] += count
                        
                        # Mark this specific CSV as processed
                        # Calculate simple metrics for tracking
                        metrics = {
                            'group_type': dma_info['Group_type'],
                            'group_number': dma_info['Group']
                        }
                        
                        # Add question-specific response counts
                        if survey_type == "BRAND_TRACKER":
                            if 'brand_data' in locals() and not brand_data.empty:
                                metrics.update({
                                    'q1_response_count': len(brand_data[brand_data['q1_answer'].notna() & (brand_data['q1_answer'] != '')]),
                                    'q2_response_count': len(brand_data[brand_data['q2_answer'].notna() & (brand_data['q2_answer'] != '')]),
                                    'q3_response_count': len(brand_data[brand_data['q3_answer'].notna() & (brand_data['q3_answer'] != '')])
                                })
                            else:
                                metrics.update({'q1_response_count': 0, 'q2_response_count': 0, 'q3_response_count': 0})
                        else:
                            if 'custom_data' in locals() and not custom_data.empty:
                                metrics.update({
                                    'q1_response_count': len(custom_data[custom_data['q1_answer'].notna() & (custom_data['q1_answer'] != '')]),
                                    'q2_response_count': len(custom_data[custom_data['q2_answer'].notna() & (custom_data['q2_answer'] != '')])
                                })
                            else:
                                metrics.update({'q1_response_count': 0, 'q2_response_count': 0})
                        
                        tracker.mark_processed(
                            filename=csv_filename,
                            survey_type=survey_type
                        )
                        
                        csv_files_processed.append(csv_filename)
                        logger.info(f"✅ Successfully processed CSV: {csv_filename}")
                        
                        # Efficient memory cleanup after each file
                        import gc
                        del df
                        if survey_type == "BRAND_TRACKER" and 'brand_data' in locals():
                            del brand_data
                        elif survey_type == "CUSTOM" and 'custom_data' in locals():
                            del custom_data
                        gc.collect()
                        
                        # Log memory after cleanup
                        current_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
                        logger.info(f"🧹 Memory after cleanup: {current_memory:.1f} MB")
                    
                    except Exception as e:
                        logger.error(f"❌ Error processing CSV {csv_filename}: {e}")
                        continue
                
                # Remove duplicates from tables_updated
                tables_updated = list(set(tables_updated))
                
                # Log final memory usage
                end_memory = process.memory_info().rss / 1024 / 1024
                memory_diff = end_memory - start_memory
                logger.info(f"🧠 Final memory usage: {end_memory:.1f} MB (change: {memory_diff:+.1f} MB)")
                
                return {
                    "status": "success",
                    "csv_files_processed": csv_files_processed,
                    "csv_files_skipped": csv_files_skipped,
                    "tables_updated": tables_updated,
                    "records_added": records_added
                }
        
        except Exception as e:
            logger.error(f"❌ Error processing ZIP file: {e}", exc_info=True)
            return {"status": "error", "error": str(e)}

