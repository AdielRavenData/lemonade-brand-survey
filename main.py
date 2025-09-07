"""
Cloud Run Function for Lemonade Survey Processing
Triggered by Cloud Storage uploads, processes new ZIP files and appends to BigQuery tables
"""

import os
import logging
import tempfile
from pathlib import Path
from cloud_utils.CloudStorage_Client import CloudStorageClient
from google.cloud.exceptions import NotFound
from flask import Flask, request
import json
import time

from survey_processor import SurveyProcessor
from survey_tracker import SurveyTracker

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
PROJECT_ID = "lemonade-brand-sruvey-tracker"
BRAND_DATASET = "new_brand_survey"
CUSTOM_DATASET = "new_custom_brand_survey"

# Simple in-memory deduplication (resets when container restarts)
recent_files = {}

def process_uploaded_file(bucket_name, file_name):
    """
    Process a newly uploaded ZIP file
    
    Args:
        bucket_name (str): GCS bucket name
        file_name (str): ZIP file name
    
    Returns:
        dict: Processing result
    """
    logger.info(f"🔥 Processing upload: {bucket_name}/{file_name}")
    
    # Initialize components
    tracker = SurveyTracker(PROJECT_ID)
    processor = SurveyProcessor(PROJECT_ID, BRAND_DATASET, CUSTOM_DATASET)
    storage_client = CloudStorageClient(creds=None, headers_json=None)
    
    try:
        # Check if file is a ZIP
        if not file_name.endswith('.zip'):
            logger.info(f"⏭️  Skipping non-ZIP file: {file_name}")
            return {"status": "skipped", "reason": "not_zip_file"}
        
        # We'll check individual CSV files after extracting ZIP
        # Determine survey type from ZIP filename
        survey_type = "BRAND_TRACKER" if "Brand Tracker" in file_name else "CUSTOM"
        logger.info(f"🏷️  Detected survey type: {survey_type} for file: {file_name}")
        
        # Download file from GCS
        bucket = storage_client.client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        if not blob.exists():
            logger.error(f"❌ File not found: {bucket_name}/{file_name}")
            return {"status": "error", "reason": "file_not_found"}
        
        # Create temporary directory for processing
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            zip_path = temp_path / "survey.zip"
            
            # Download ZIP file
            logger.info(f"📥 Downloading {file_name}...")
            blob.download_to_filename(zip_path)
            
            # Process the ZIP file with individual CSV tracking
            logger.info(f"🔄 Processing survey data...")
            result = processor.process_zip_with_individual_tracking(
                zip_path=zip_path,
                original_filename=file_name,
                survey_type=survey_type,
                tracker=tracker
            )
            
            if result["status"] == "success":
                logger.info(f"✅ Successfully processed: {file_name}")
                logger.info(f"📊 CSV files processed: {result['csv_files_processed']}")
                logger.info(f"📊 CSV files skipped: {result['csv_files_skipped']}")
                logger.info(f"📊 Tables updated: {', '.join(result['tables_updated'])}")
                return {
                    "status": "success",
                    "survey_type": survey_type,
                    "csv_files_processed": result['csv_files_processed'],
                    "csv_files_skipped": result['csv_files_skipped'],
                    "tables_updated": result['tables_updated'],
                    "records_added": result.get('records_added', {})
                }
            else:
                logger.error(f"❌ Processing failed: {result.get('error', 'Unknown error')}")
                return {"status": "error", "reason": result.get('error', 'processing_failed')}
    
    except Exception as e:
        logger.error(f"❌ Unexpected error processing {file_name}: {str(e)}", exc_info=True)
        return {"status": "error", "reason": f"unexpected_error: {str(e)}"}

@app.route("/", methods=["POST"])
def handle_cloud_storage_trigger():
    """
    Handle Cloud Storage triggers via Eventarc (CloudEvents format)
    """
    try:
        # Parse CloudEvents format from Eventarc
        request_json = request.get_json()
        if not request_json:
            logger.error("❌ No JSON payload received")
            return "Bad Request: no JSON payload", 400
        
        # Extract file information from CloudEvents
        bucket_name = request_json.get("bucket")
        file_name = request_json.get("name")
        event_type = request_json.get("eventType", "unknown")
        
        if not bucket_name or not file_name:
            logger.error(f"❌ Missing bucket or file info: bucket={bucket_name}, file={file_name}")
            return "Bad Request: missing file information", 400
        
        # Log all event types to see what we're getting
        logger.info(f"📨 Received event type: {event_type}")
        
        # Simple deduplication: skip if we processed this file in the last 60 seconds
        file_key = f"{bucket_name}/{file_name}"
        current_time = time.time()
        
        if file_key in recent_files:
            time_diff = current_time - recent_files[file_key]
            if time_diff < 60:  # 60 seconds cooldown
                logger.info(f"⏭️ Skipping duplicate event for {file_name} (processed {time_diff:.1f}s ago)")
                return "OK: duplicate event ignored", 200
        
        # Record this processing attempt
        recent_files[file_key] = current_time
        
        logger.info(f"🎯 Processing Eventarc event: {event_type}")
        logger.info(f"📁 Bucket: {bucket_name}")
        logger.info(f"📄 File: {file_name}")
        
        # Process the uploaded file
        result = process_uploaded_file(bucket_name, file_name)
        
        # Return appropriate response
        if result["status"] == "success":
            return f"Successfully processed: {file_name}", 200
        elif result["status"] == "skipped":
            return f"Skipped: {file_name} - {result['reason']}", 200
        else:
            logger.error(f"❌ Processing failed: {result}")
            return f"Error processing: {file_name} - {result.get('reason', 'unknown')}", 500
    
    except Exception as e:
        logger.error(f"❌ Error handling request: {str(e)}", exc_info=True)
        return f"Internal Server Error: {str(e)}", 500

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    return "OK", 200

@app.route("/test", methods=["POST"])
def test_processing():
    """
    Test endpoint for manual file processing
    Expects JSON: {"bucket": "bucket-name", "file": "file-name.zip"}
    """
    try:
        data = request.get_json()
        if not data:
            return "Bad Request: JSON required", 400
        
        bucket_name = data.get("bucket")
        file_name = data.get("file")
        
        if not bucket_name or not file_name:
            return "Bad Request: bucket and file required", 400
        
        logger.info(f"🧪 Test processing: {bucket_name}/{file_name}")
        result = process_uploaded_file(bucket_name, file_name)
        
        return json.dumps(result, indent=2), 200
    
    except Exception as e:
        logger.error(f"❌ Test error: {str(e)}", exc_info=True)
        return f"Test Error: {str(e)}", 500

if __name__ == "__main__":
    # For Cloud Run
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
