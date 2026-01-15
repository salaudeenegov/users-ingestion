"""
API Client Module
Handles user ingestion via API
"""

import requests
import time
import json
import pandas as pd
from datetime import datetime


class APIClient:
    """
    API Client for uploading user data to DHIS2 ingestion endpoint
    """

    def __init__(self, api_url, tenant_id="bi", auth_token=None, update_url=None):
        """
        Initialize API client

        Args:
            api_url: API endpoint URL (for CREATE)
            tenant_id: Tenant ID
            auth_token: Authentication token
            update_url: Optional separate UPDATE endpoint (defaults to api_url)
        """
        self.api_url = api_url
        self.update_url = update_url or api_url  # Use same URL if not specified
        self.tenant_id = tenant_id
        self.auth_token = auth_token or "ee36fdd7-64e7-4583-9c16-998479ff53c0"

        # Default user info for request
        self.user_info = {
            "id": 97,
            "userName": "ab-prd",
            "salutation": None,
            "name": "System User",
            "gender": None,
            "mobileNumber": "9999999999",
            "emailId": None,
            "altContactNumber": None,
            "pan": None,
            "aadhaarNumber": None,
            "permanentAddress": None,
            "permanentCity": None,
            "permanentPinCode": None,
            "correspondenceAddress": None,
            "correspondenceCity": None,
            "correspondencePinCode": None,
            "alternatemobilenumber": None,
            "active": True,
            "locale": None,
            "type": "EMPLOYEE",
            "accountLocked": False,
            "accountLockedDate": 0,
            "fatherOrHusbandName": None,
            "relationship": None,
            "signature": None,
            "bloodGroup": None,
            "photo": None,
            "identificationMark": None,
            "createdBy": 23287,
            "lastModifiedBy": 23287,
            "tenantId": self.tenant_id,
            "roles": self._get_default_roles(),
            "uuid": f"{self.tenant_id}-prd",
            "createdDate": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            "lastModifiedDate": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            "dob": None,
            "pwdExpiryDate": None
        }

    def _get_default_roles(self):
        """Get default roles for the request"""
        return [
            {"code": "SUPERVISOR", "name": "Supervisor", "tenantId": self.tenant_id},
            {"code": "DISTRICT_SUPERVISOR", "name": "District Supervisor", "tenantId": self.tenant_id},
            {"code": "SYSTEM_ADMINISTRATOR", "name": "System Administrator", "tenantId": self.tenant_id},
            {"code": "SUPERUSER", "name": "Super User", "tenantId": self.tenant_id},
            {"code": "NATIONAL_SUPERVISOR", "name": "National Supervisor", "tenantId": self.tenant_id},
            {"code": "DISTRIBUTOR", "name": "Distributor", "tenantId": self.tenant_id},
            {"code": "WAREHOUSE_MANAGER", "name": "Warehouse Manager", "tenantId": self.tenant_id},
            {"code": "REGISTRAR", "name": "Registrar", "tenantId": self.tenant_id},
            {"code": "PROVINCIAL_SUPERVISOR", "name": "Provincial Supervisor", "tenantId": self.tenant_id}
        ]

    def _build_payload(self):
        """Build request payload"""
        return {
            "DHIS2IngestionRequest": json.dumps({
                "tenantId": self.tenant_id,
                "dataType": "Users",
                "requestInfo": {
                    "authToken": self.auth_token,
                    "userInfo": self.user_info
                }
            })
        }

    def _check_if_user_exists(self, response_text, status_code):
        """
        Check if the error indicates user already exists

        Args:
            response_text: API response text
            status_code: HTTP status code

        Returns:
            bool: True if user already exists
        """
        # Common patterns for "already exists" errors
        exists_patterns = [
            "already exists",
            "already exist",
            "duplicate",
            "user exists",
            "username already",
            "conflict",
        ]

        response_lower = response_text.lower()

        # Check status code 409 (Conflict) or patterns in message
        if status_code == 409:
            return True

        return any(pattern in response_lower for pattern in exists_patterns)

    def _upload_to_endpoint(self, file_path, endpoint_url, mode="CREATE"):
        """
        Internal method to upload file to specific endpoint

        Args:
            file_path: Path to file to upload
            endpoint_url: API endpoint URL
            mode: Operation mode (CREATE/UPDATE)

        Returns:
            dict: Response data with status, status_code, and message
        """
        payload = self._build_payload()
        headers = {'Accept': 'application/json'}

        try:
            with open(file_path, 'rb') as f:
                files = [('file', ('file', f, 'application/octet-stream'))]
                response = requests.post(
                    endpoint_url,
                    headers=headers,
                    data=payload,
                    files=files,
                    timeout=60
                )

            # Parse response
            result = {
                "status": "SUCCESS" if response.status_code == 200 else "ERROR",
                "status_code": response.status_code,
                "message": response.text,
                "mode": mode
            }

            return result

        except requests.exceptions.Timeout:
            return {
                "status": "ERROR",
                "status_code": 408,
                "message": "Request timeout",
                "mode": mode
            }
        except requests.exceptions.RequestException as e:
            return {
                "status": "ERROR",
                "status_code": 500,
                "message": str(e),
                "mode": mode
            }
        except Exception as e:
            return {
                "status": "ERROR",
                "status_code": 500,
                "message": f"Unexpected error: {str(e)}",
                "mode": mode
            }

    def upload_file(self, file_path, mode="AUTO"):
        """
        Upload a single file to API with flexible mode control

        Modes:
        - AUTO: Smart UPSERT - Try CREATE first, auto-retry UPDATE if exists
        - CREATE: Force CREATE only - Fail if user already exists
        - UPDATE: Force UPDATE only - Fail if user doesn't exist

        Args:
            file_path: Path to file to upload
            mode: Upload mode ("AUTO", "CREATE", "UPDATE")

        Returns:
            dict: Response data with status (CREATED/UPDATED/FAILED), status_code, and message
        """
        mode = mode.upper()

        if mode == "CREATE":
            # Force CREATE only
            result = self._upload_to_endpoint(file_path, self.api_url, mode="CREATE")
            if result["status"] == "SUCCESS":
                result["status"] = "CREATED"
                result["operation"] = "CREATE"
            else:
                result["status"] = "FAILED"
                result["operation"] = "CREATE_FAILED"

        elif mode == "UPDATE":
            # Force UPDATE only
            result = self._upload_to_endpoint(file_path, self.update_url, mode="UPDATE")
            if result["status"] == "SUCCESS":
                result["status"] = "UPDATED"
                result["operation"] = "UPDATE"
            else:
                result["status"] = "FAILED"
                result["operation"] = "UPDATE_FAILED"

        else:  # mode == "AUTO"
            # Smart UPSERT: Try CREATE first
            result = self._upload_to_endpoint(file_path, self.api_url, mode="CREATE")

            # If CREATE failed, check if user exists
            if result["status"] == "ERROR":
                if self._check_if_user_exists(result["message"], result["status_code"]):
                    # User exists - retry with UPDATE
                    result = self._upload_to_endpoint(file_path, self.update_url, mode="UPDATE")

                    if result["status"] == "SUCCESS":
                        result["status"] = "UPDATED"
                        result["operation"] = "UPDATE"
                    else:
                        result["status"] = "FAILED"
                        result["operation"] = "UPDATE_FAILED"
                else:
                    # Other error - mark as FAILED
                    result["status"] = "FAILED"
                    result["operation"] = "CREATE_FAILED"
            elif result["status"] == "SUCCESS":
                # CREATE succeeded
                result["status"] = "CREATED"
                result["operation"] = "CREATE"

        return result

    def process_validated_csv(self, validated_csv_path, output_path, delay=5):
        """
        Process validated CSV and upload to API, tracking responses per row

        Args:
            validated_csv_path: Path to validated CSV
            output_path: Path to save output CSV with API responses
            delay: Delay between requests in seconds

        Returns:
            dict: Summary of ingestion results
        """
        # Read validated CSV
        df = pd.read_csv(validated_csv_path)

        # Initialize API response columns
        df['api_status'] = ''
        df['api_status_code'] = ''
        df['api_message'] = ''

        success_count = 0
        error_count = 0
        skipped_count = 0

        # Only process rows with validation_status = 'CORRECT'
        for idx, row in df.iterrows():
            if row.get('validation_status') == 'CORRECT':
                print(f"Processing row {idx + 1}/{len(df)}: {row.get('username', 'N/A')}")

                # Create temp CSV for single row
                temp_file = f"temp_upload_{idx}.csv"
                single_row_df = pd.DataFrame([row])
                single_row_df.to_csv(temp_file, index=False)

                # Upload
                result = self.upload_file(temp_file)

                # Update DataFrame with API response
                df.at[idx, 'api_status'] = result['status']
                df.at[idx, 'api_status_code'] = result['status_code']
                df.at[idx, 'api_message'] = result['message']

                # Count based on new status values
                if result['status'] in ['CREATED', 'UPDATED']:
                    success_count += 1
                else:
                    error_count += 1

                # Clean up temp file
                import os
                if os.path.exists(temp_file):
                    os.remove(temp_file)

                # Delay between requests
                if delay > 0:
                    time.sleep(delay)
            else:
                # Skip rows with validation errors
                df.at[idx, 'api_status'] = 'SKIPPED'
                df.at[idx, 'api_status_code'] = 'N/A'
                df.at[idx, 'api_message'] = 'Validation failed'
                skipped_count += 1

        # Save output
        df.to_csv(output_path, index=False)

        summary = {
            "total_rows": len(df),
            "success": success_count,
            "error": error_count,
            "skipped": skipped_count,
            "output_file": output_path
        }

        return summary
