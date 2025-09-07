"""
Slack Notification Service for Cloud Run
Sends notifications on processing success/failure
"""

import os
import logging
import requests
import json
from datetime import datetime
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)

class SlackNotifier:
    """
    Slack notification service for survey processing results
    """
    
    def __init__(self, webhook_url: Optional[str] = None):
        """
        Initialize Slack notifier with webhook URL
        
        Args:
            webhook_url: Slack webhook URL (can be set via environment variable SLACK_WEBHOOK_URL)
        """
        self.webhook_url = webhook_url or os.getenv('SLACK_WEBHOOK_URL')
        self.enabled = bool(self.webhook_url)
        
        if not self.enabled:
            logger.warning("⚠️ Slack notifications disabled - no webhook URL provided")
        else:
            logger.info("✅ Slack notifications enabled")
    
    def send_success_notification(self, 
                                file_name: str, 
                                survey_type: str,
                                csv_files_processed: List[str],
                                csv_files_skipped: List[str],
                                tables_updated: List[str],
                                records_added: Dict[str, int],
                                processing_time: Optional[float] = None) -> bool:
        """
        Send success notification to Slack
        
        Args:
            file_name: ZIP file that was processed
            survey_type: BRAND_TRACKER or CUSTOM
            csv_files_processed: List of CSV files processed
            csv_files_skipped: List of CSV files skipped
            tables_updated: List of BigQuery tables updated
            records_added: Dict of table -> record count
            processing_time: Processing time in seconds
            
        Returns:
            True if notification sent successfully, False otherwise
        """
        if not self.enabled:
            return False
        
        try:
            # Calculate totals
            total_processed = len(csv_files_processed)
            total_skipped = len(csv_files_skipped)
            total_records = sum(records_added.values()) if records_added else 0
            
            # Create rich message
            message = {
                "attachments": [
                    {
                        "color": "good",  # Green color
                        "title": "✅ Survey Processing Completed Successfully",
                        "fields": [
                            {
                                "title": "File",
                                "value": f"`{file_name}`",
                                "short": True
                            },
                            {
                                "title": "Survey Type",
                                "value": survey_type,
                                "short": True
                            },
                            {
                                "title": "CSV Files Processed",
                                "value": str(total_processed),
                                "short": True
                            },
                            {
                                "title": "CSV Files Skipped",
                                "value": str(total_skipped),
                                "short": True
                            },
                            {
                                "title": "Total Records Added",
                                "value": f"{total_records:,}",
                                "short": True
                            },
                            {
                                "title": "Tables Updated",
                                "value": ", ".join(tables_updated),
                                "short": True
                            }
                        ],
                        "footer": "Lemonade Survey Processor",
                        "ts": int(datetime.now().timestamp())
                    }
                ]
            }
            
            # Add processing time if available
            if processing_time:
                message["attachments"][0]["fields"].append({
                    "title": "Processing Time",
                    "value": f"{processing_time:.1f}s",
                    "short": True
                })
            
            # Add details about processed files
            if csv_files_processed:
                processed_list = "\n".join([f"• {f}" for f in csv_files_processed[:5]])
                if len(csv_files_processed) > 5:
                    processed_list += f"\n... and {len(csv_files_processed) - 5} more"
                
                message["attachments"][0]["fields"].append({
                    "title": "Files Processed",
                    "value": f"```{processed_list}```",
                    "short": False
                })
            
            return self._send_message(message)
            
        except Exception as e:
            logger.error(f"❌ Error sending success notification: {e}")
            return False
    
    def send_failure_notification(self, 
                                file_name: str, 
                                error: str,
                                survey_type: Optional[str] = None,
                                processing_time: Optional[float] = None) -> bool:
        """
        Send failure notification to Slack
        
        Args:
            file_name: ZIP file that failed to process
            error: Error message
            survey_type: BRAND_TRACKER or CUSTOM (if determined)
            processing_time: Processing time before failure
            
        Returns:
            True if notification sent successfully, False otherwise
        """
        if not self.enabled:
            return False
        
        try:
            message = {
                "attachments": [
                    {
                        "color": "danger",  # Red color
                        "title": "❌ Survey Processing Failed",
                        "fields": [
                            {
                                "title": "File",
                                "value": f"`{file_name}`",
                                "short": True
                            },
                            {
                                "title": "Error",
                                "value": f"```{error}```",
                                "short": False
                            }
                        ],
                        "footer": "Lemonade Survey Processor",
                        "ts": int(datetime.now().timestamp())
                    }
                ]
            }
            
            # Add survey type if available
            if survey_type:
                message["attachments"][0]["fields"].insert(1, {
                    "title": "Survey Type",
                    "value": survey_type,
                    "short": True
                })
            
            # Add processing time if available
            if processing_time:
                message["attachments"][0]["fields"].append({
                    "title": "Processing Time",
                    "value": f"{processing_time:.1f}s",
                    "short": True
                })
            
            return self._send_message(message)
            
        except Exception as e:
            logger.error(f"❌ Error sending failure notification: {e}")
            return False
    
    def send_skipped_notification(self, 
                                file_name: str, 
                                reason: str,
                                survey_type: Optional[str] = None) -> bool:
        """
        Send skipped file notification to Slack
        
        Args:
            file_name: ZIP file that was skipped
            reason: Reason for skipping
            survey_type: BRAND_TRACKER or CUSTOM (if determined)
            
        Returns:
            True if notification sent successfully, False otherwise
        """
        if not self.enabled:
            return False
        
        try:
            message = {
                "attachments": [
                    {
                        "color": "warning",  # Yellow color
                        "title": "⏭️ Survey Processing Skipped",
                        "fields": [
                            {
                                "title": "File",
                                "value": f"`{file_name}`",
                                "short": True
                            },
                            {
                                "title": "Reason",
                                "value": reason,
                                "short": True
                            }
                        ],
                        "footer": "Lemonade Survey Processor",
                        "ts": int(datetime.now().timestamp())
                    }
                ]
            }
            
            # Add survey type if available
            if survey_type:
                message["attachments"][0]["fields"].append({
                    "title": "Survey Type",
                    "value": survey_type,
                    "short": True
                })
            
            return self._send_message(message)
            
        except Exception as e:
            logger.error(f"❌ Error sending skipped notification: {e}")
            return False
    
    def _send_message(self, message: Dict) -> bool:
        """
        Send message to Slack webhook
        
        Args:
            message: Slack message payload
            
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            response = requests.post(
                self.webhook_url,
                json=message,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info("📤 Slack notification sent successfully")
                return True
            else:
                logger.error(f"❌ Slack notification failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error sending Slack message: {e}")
            return False
    
    def test_notification(self) -> bool:
        """
        Send a test notification to verify Slack integration
        
        Returns:
            True if test successful, False otherwise
        """
        if not self.enabled:
            logger.warning("⚠️ Slack notifications disabled - cannot send test")
            return False
        
        message = {
            "text": "🧪 Test notification from Lemonade Survey Processor",
            "attachments": [
                {
                    "color": "good",
                    "title": "✅ Slack Integration Test",
                    "text": "If you see this message, Slack notifications are working correctly!",
                    "footer": "Lemonade Survey Processor",
                    "ts": int(datetime.now().timestamp())
                }
            ]
        }
        
        success = self._send_message(message)
        if success:
            logger.info("✅ Slack test notification sent successfully")
        else:
            logger.error("❌ Slack test notification failed")
        
        return success
