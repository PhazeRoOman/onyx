import logging
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any
from requests.auth import HTTPBasicAuth

from onyx.utils.logger import setup_logger

logger = setup_logger()


class BambooHRClientRequestFailedError(Exception):
    def __init__(self, message: str, status_code: int = 0):
        self.status_code = status_code
        super().__init__(message)


class BambooHRApiClient:
    """Client for accessing BambooHR API."""

    def __init__(self, subdomain: str, api_key: str):
        """Initialize the BambooHR API client.

        Args:
            subdomain: The BambooHR subdomain for your company
            api_key: The BambooHR API key
        """
        self.subdomain = subdomain
        self.api_key = api_key
        self.base_url = f"https://api.bamboohr.com/api/gateway.php/{self.subdomain}/v1"

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a GET request to the BambooHR API.

        Args:
            endpoint: API endpoint to call
            params: Query parameters

        Returns:
            Response JSON
        """
        url = f"{self.base_url}/{endpoint}"
        headers = {"Accept": "application/json"}
        
        try:
            response = requests.get(
                url,
                auth=HTTPBasicAuth(self.api_key, "x"),  # BambooHR uses API key as username and "x" as password
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            status_code = e.response.status_code if hasattr(e, 'response') else 0
            logger.error(f"Error making request to BambooHR (status={status_code}): {e}")
            raise BambooHRClientRequestFailedError(f"Failed to fetch data from BambooHR: {e}", status_code)

    def build_app_url(self, path: str) -> str:
        """Build a URL to access the BambooHR web application.

        Args:
            path: Path to append to the base URL

        Returns:
            Full URL
        """
        return f"https://{self.subdomain}.bamboohr.com{path}"