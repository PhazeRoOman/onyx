import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Generator

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.configs.constants import DocumentSource
from onyx.connectors.bamboohr.client import BambooHRApiClient, BambooHRClientRequestFailedError
from onyx.connectors.cross_connector_utils.miscellaneous_utils import time_str_to_utc
from onyx.connectors.exceptions import ConnectorValidationError
from onyx.connectors.exceptions import CredentialExpiredError
from onyx.connectors.exceptions import InsufficientPermissionsError
from onyx.connectors.interfaces import GenerateDocumentsOutput, SecondsSinceUnixEpoch
from onyx.connectors.interfaces import LoadConnector
from onyx.connectors.interfaces import PollConnector
from onyx.connectors.models import ConnectorMissingCredentialError
from onyx.connectors.models import Document
from onyx.connectors.models import TextSection
from onyx.utils.logger import setup_logger

logger = setup_logger()

# Fields available in the initial employee directory response
DIRECTORY_FIELDS = [
    "id",
    "firstName",
    "lastName",
    "jobTitle",
    "department",
    "location",
    "workPhone",
    "mobilePhone",
    "workEmail",
    "homeEmail",
]

# Additional fields to fetch per employee
EMPLOYEE_DETAIL_FIELDS = [
    "hireDate",
    "status",
    "lastChanged",
    "terminationDate",
]


class BambooHRConnector(LoadConnector, PollConnector):
    """Connector for BambooHR."""

    def __init__(
        self,
        batch_size: int = INDEX_BATCH_SIZE,
        departments: Optional[List[str]] = None,
        job_titles: Optional[List[str]] = None,
        employment_status: Optional[List[str]] = None,
        file_categories: Optional[List[str]] = None,
        time_off_types: Optional[List[str]] = None,
        include_files: bool = True,
        include_time_off: bool = True,
        hire_date_after: Optional[str] = None,
        updated_since: Optional[str] = None,
        indexing_scope: Optional[str] = "everything",
    ) -> None:
        """Initialize the BambooHR connector.

        Args:
            batch_size: Number of documents to index in one batch
            departments: Filter employees by departments
            job_titles: Filter employees by job titles
            employment_status: Filter employees by employment status
            file_categories: Filter documents by categories
            time_off_types: Filter time-off records by type
            include_files: Whether to include files in the indexing
            include_time_off: Whether to include time-off records in indexing
            hire_date_after: Only include employees hired after this date (YYYY-MM-DD)
            updated_since: Only include records updated after this date (YYYY-MM-DD)
            indexing_scope: Whether to index everything or use filters
        """
        self.batch_size = batch_size
        self.departments = departments or []
        self.job_titles = job_titles or []
        self.employment_status = employment_status or []
        self.file_categories = file_categories or []
        self.time_off_types = time_off_types or []
        self.include_files = include_files
        self.include_time_off = include_time_off
        self.hire_date_after = hire_date_after
        self.updated_since = updated_since
        self.indexing_scope = indexing_scope
        self.bamboohr_client: Optional[BambooHRApiClient] = None
        self.company_name: Optional[str] = None

    def load_credentials(self, credentials: Dict[str, Any]) -> Dict[str, Any] | None:
        """Load necessary credentials for BambooHR."""
        self.bamboohr_client = BambooHRApiClient(
            subdomain=credentials["bamboohr_subdomain"],
            api_key=credentials["bamboohr_api_token"],
        )
        
        # Fetch company name
        self.company_name = credentials["bamboohr_subdomain"]
        
        return None

    def _fetch_employee_details(self, employee_id: str) -> Dict[str, Any]:
        """Fetch additional details for an employee.

        Args:
            employee_id: The ID of the employee

        Returns:
            Dictionary with additional employee details
        """
        if not self.bamboohr_client:
            return {}

        try:
            # Fetch additional fields for this employee
            fields = ",".join(EMPLOYEE_DETAIL_FIELDS)
            response = self.bamboohr_client.get(f"employees/{employee_id}", {"fields": fields})
            return response
        except Exception as e:
            logger.warning(f"Error fetching details for employee {employee_id}: {e}")
            return {}

    def _employee_to_document(
        self,
        bamboohr_client: BambooHRApiClient, 
        employee: Dict[str, Any],
        details: Optional[Dict[str, Any]] = None
    ) -> Document:
        """Create a document from employee data.

        Args:
            bamboohr_client: The BambooHR API client
            employee: Employee data dictionary
            details: Optional additional employee details

        Returns:
            Document for indexing
        """
        employee_id = str(employee.get("id", ""))
        first_name = str(employee.get("firstName", ""))
        last_name = str(employee.get("lastName", ""))
        name = f"{first_name} {last_name}".strip()
        
        url = bamboohr_client.build_app_url(f"/employees/employee.php?id={employee_id}")
        
        # Format employee data as text
        text = f"Company: {self.company_name}\n"  # Add company name
        text += f"Employee: {name}\n"
        text += f"ID: {employee_id}\n"
        
        # Include standard fields from directory
        for field in ["jobTitle", "department", "location", "workPhone", "mobilePhone", 
                      "workEmail", "homeEmail"]:
            if field in employee and employee[field]:
                formatted_field = field.replace("_", " ").title()
                text += f"{formatted_field}: {employee[field]}\n"
        
        # Include additional details if available
        if details:
            for field in ["hireDate", "status"]:
                if field in details and details[field]:
                    formatted_field = field.replace("_", " ").title()
                    text += f"{formatted_field}: {details[field]}\n"
            
            # Add termination date if available
            if "terminationDate" in details and details["terminationDate"]:
                text += f"Termination Date: {details['terminationDate']}\n"
        
        # Determine the updated timestamp
        updated_at_str = None
        if details and "lastChanged" in details:
            updated_at_str = str(details["lastChanged"])
        
        # Prepare metadata from both sources with default empty strings to avoid None values
        metadata = {
            "type": "employee",
            "company": self.company_name,  # Add company name to metadata
            "employee_id": employee_id,
            "department": employee.get("department") or "",
            "job_title": employee.get("jobTitle") or "",
        }
        
        if details:
            metadata["status"] = details.get("status") or ""
            metadata["hire_date"] = details.get("hireDate") or ""
            metadata["termination_date"] = details.get("terminationDate") or ""
        else:
            metadata["status"] = ""
            metadata["hire_date"] = ""
            metadata["termination_date"] = ""
        
        return Document(
            id=f"bamboohr_employee_{employee_id}",
            sections=[TextSection(link=url, text=text)],
            source=DocumentSource.BAMBOOHR,
            semantic_identifier=f"Employee: {name} - {self.company_name}",  # Include company in identifier
            title=f"{name} - {self.company_name}",  # Include company in title
            doc_updated_at=time_str_to_utc(updated_at_str) if updated_at_str else None,
            metadata=metadata,
        )

    def _should_include_employee(self, employee: Dict[str, Any], details: Dict[str, Any]) -> bool:
        """Check if an employee should be included based on filters.

        Args:
            employee: Employee data dictionary
            details: Additional employee details

        Returns:
            True if the employee should be included, False otherwise
        """
        # Skip filter checking if indexing everything
        if self.indexing_scope == "everything":
            return True
            
        # Filter by department
        if self.departments and employee.get("department") not in self.departments:
            return False
            
        # Filter by job title
        if self.job_titles and employee.get("jobTitle") not in self.job_titles:
            return False
            
        # Filter by employment status (requires details)
        if self.employment_status and details.get("status") not in self.employment_status:
            return False
            
        # Filter by hire date (requires details)
        if self.hire_date_after and details.get("hireDate"):
            try:
                hire_date = datetime.strptime(details["hireDate"], "%Y-%m-%d").date()
                filter_date = datetime.strptime(self.hire_date_after, "%Y-%m-%d").date()
                if hire_date < filter_date:
                    return False
            except ValueError:
                # If date parsing fails, include the employee
                pass
                
        return True

    def _should_include_file(self, file_data: Dict[str, Any]) -> bool:
        """Check if a file should be included based on filters.

        Args:
            file_data: File metadata

        Returns:
            True if the file should be included, False otherwise
        """
        # Skip if not including files at all
        if not self.include_files:
            return False
            
        # Skip filter checking if indexing everything
        if self.indexing_scope == "everything":
            return True
            
        # Filter by file category
        if self.file_categories and file_data.get("category") not in self.file_categories:
            return False
                
        # Filter by updated date
        if self.updated_since and file_data.get("lastUpdated"):
            try:
                updated_date = time_str_to_utc(str(file_data["lastUpdated"]))
                filter_date = datetime.strptime(self.updated_since, "%Y-%m-%d").timestamp()
                if updated_date < filter_date:
                    return False
            except (ValueError, TypeError):
                # If date parsing fails, include the file
                pass
                
        return True
        
    def _should_include_time_off(self, time_off_data: Dict[str, Any]) -> bool:
        """Check if a time-off record should be included based on filters."""
        # Skip if not including time-off records at all
        if not self.include_time_off:
            return False
            
        # Skip filter checking if indexing everything
        if self.indexing_scope == "everything":
            return True
            
        # Filter by time-off type - handle both string and dict format
        if self.time_off_types:
            time_off_type = ""
            if isinstance(time_off_data.get("type"), dict):
                time_off_type = time_off_data.get("type", {}).get("name", "")
            else:
                time_off_type = str(time_off_data.get("type", ""))
                
            if time_off_type not in self.time_off_types:
                return False
                
        # Filter by updated date
        if self.updated_since:
            try:
                # Check if status is a dict containing lastChanged
                if isinstance(time_off_data.get("status"), dict) and "lastChanged" in time_off_data.get("status", {}):
                    updated_date = time_str_to_utc(str(time_off_data["status"]["lastChanged"]))
                elif time_off_data.get("lastModified"):
                    updated_date = time_str_to_utc(str(time_off_data["lastModified"]))
                else:
                    # If no update info, default to creation date
                    updated_date = time_str_to_utc(str(time_off_data.get("created")))
                    
                if updated_date:
                    filter_date = datetime.strptime(self.updated_since, "%Y-%m-%d").timestamp()
                    if updated_date < filter_date:
                        return False
            except (ValueError, TypeError):
                # If date parsing fails, include the record
                pass
                
        return True

    @staticmethod
    def _parse_employee_files_xml(xml_content: str, employee_id: str) -> List[Dict[str, Any]]:
        """Parse XML response for employee files.
        
        Args:
            xml_content: XML content returned by the API
            employee_id: Employee ID for reference
            
        Returns:
            List of file data dictionaries
        """
        try:
            root = ET.fromstring(xml_content)
            files = []
            
            # Process each category in the XML
            for category in root.findall('.//category'):
                category_id = category.get('id', '')
                category_name = category.findtext('name', '')
                
                # Process each file in the category
                for file_elem in category.findall('.//file'):
                    file_id = file_elem.get('id', '')
                    filename = file_elem.findtext('name', '')
                    created_date = file_elem.findtext('createdDate', '')
                    
                    file_data = {
                        'id': file_id,
                        'name': filename,
                        'category': category_name,
                        'categoryId': category_id,
                        'lastUpdated': created_date,  # Using createdDate as lastUpdated
                        'employeeId': employee_id,
                    }
                    files.append(file_data)
                    
            return files
        except ET.ParseError as e:
            logger.error(f"Error parsing XML for employee {employee_id} files: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error processing XML for employee {employee_id}: {e}")
            return []

    @staticmethod
    def _parse_company_files_xml(xml_content: str) -> List[Dict[str, Any]]:
        """Parse XML response for company files.
        
        Args:
            xml_content: XML content returned by the API
            
        Returns:
            List of file data dictionaries
        """
        try:
            root = ET.fromstring(xml_content)
            files = []
            
            # Process each category in the XML
            for category in root.findall('.//category'):
                category_id = category.get('id', '')
                category_name = category.findtext('name', '')
                
                # Process each file in the category
                for file_elem in category.findall('.//file'):
                    file_id = file_elem.get('id', '')
                    filename = file_elem.findtext('name', '')
                    created_date = file_elem.findtext('createdDate', '')
                    
                    file_data = {
                        'id': file_id,
                        'name': filename,
                        'category': category_name,
                        'categoryId': category_id,
                        'lastUpdated': created_date,  # Using createdDate as lastUpdated
                    }
                    files.append(file_data)
                    
            return files
        except ET.ParseError as e:
            logger.error(f"Error parsing XML for company files: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error processing XML for company files: {e}")
            return []

    def _file_to_document(
        self,
        bamboohr_client: BambooHRApiClient, 
        file_data: Dict[str, Any], 
        content: str, 
        file_type: str, 
        owner: Optional[str] = None
    ) -> Document:
        """Create a document from a file.

        Args:
            bamboohr_client: The BambooHR API client
            file_data: File metadata
            content: File content
            file_type: Type of file (employee or company)
            owner: Name of employee if relevant

        Returns:
            Document for indexing
        """
        file_id = str(file_data.get("id", ""))
        title = str(file_data.get("name", "Untitled"))
        
        # Build URL based on file type
        if file_type == "employee" and "employeeId" in file_data:
            url = bamboohr_client.build_app_url(f"/employees/employee.php?id={file_data['employeeId']}&tab=files")
        else:
            url = bamboohr_client.build_app_url("/files")
        
        text = f"Company: {self.company_name}\n"  # Add company name
        text += f"File: {title}\n"
        if owner:
            text += f"Owner: {owner}\n"
        if file_data.get("category"):
            text += f"Category: {file_data['category']}\n"
        text += f"\n{content}"
        
        # Ensure no None values in metadata
        metadata = {
            "type": "file",
            "company": self.company_name,  # Add company to metadata
            "file_type": file_type,
            "file_name": title,
            "category": file_data.get("category") or "",
            "owner": owner or "",
        }
        
        updated_at_str = str(file_data.get("lastUpdated")) if file_data.get("lastUpdated") else None
        
        file_title = f"{title} - {self.company_name}"  # Include company in title
        
        return Document(
            id=f"bamboohr_{file_type}_file_{file_id}",
            sections=[TextSection(link=url, text=text)],
            source=DocumentSource.BAMBOOHR,
            semantic_identifier=f"File: {file_title}",
            title=file_title,
            doc_updated_at=time_str_to_utc(updated_at_str) if updated_at_str else None,
            metadata=metadata,
        )

    def _time_off_to_document(
        self,
        bamboohr_client: BambooHRApiClient,
        time_off_data: Dict[str, Any],
        employee_name: Optional[str] = None
    ) -> Document:
        """Create a document from time-off data."""
        time_off_id = str(time_off_data.get("id", ""))
        employee_id = str(time_off_data.get("employeeId", ""))
        employee_name = employee_name or time_off_data.get("name", "")
        
        # Extract type information - could be string or dict
        time_off_type = ""
        if isinstance(time_off_data.get("type"), dict):
            time_off_type = time_off_data.get("type", {}).get("name", "")
        else:
            time_off_type = str(time_off_data.get("type", ""))
        
        # Build meaningful title
        title = f"{employee_name} - {time_off_type or 'Time Off'}"
        
        url = bamboohr_client.build_app_url(f"/employees/timeoff/?id={employee_id}")
        
        # Format time-off data as text
        text = f"Company: {self.company_name}\n"  # Add company name
        text += f"Time Off Request: {title}\n"
        text += f"Employee ID: {employee_id}\n"
        text += f"Employee: {employee_name}\n"
        
        # Add time off details
        text += f"Type: {time_off_type}\n"
        text += f"Start Date: {time_off_data.get('start', '')}\n"
        text += f"End Date: {time_off_data.get('end', '')}\n"
        
        # Extract amount information
        if isinstance(time_off_data.get("amount"), dict):
            amount_data = time_off_data.get("amount", {})
            amount = amount_data.get("amount", "")
            unit = amount_data.get("unit", "days")
            if amount:
                text += f"Amount: {amount} {unit}\n"
        
        # Extract status information
        status = ""
        if isinstance(time_off_data.get("status"), dict):
            status = time_off_data.get("status", {}).get("status", "")
        else:
            status = str(time_off_data.get("status", ""))
        
        if status:
            text += f"Status: {status}\n"
        
        # Extract notes information
        if isinstance(time_off_data.get("notes"), dict):
            notes_data = time_off_data.get("notes", {})
            
            employee_notes = notes_data.get("employee", "")
            if employee_notes:
                text += f"Employee Notes: {employee_notes}\n"
                
            manager_notes = notes_data.get("manager", "")
            if manager_notes:
                text += f"Manager Notes: {manager_notes}\n"
        elif time_off_data.get("notes"):
            text += f"Notes: {time_off_data.get('notes')}\n"
        
        metadata = {
            "type": "time_off",
            "company": self.company_name,  # Add company to metadata
            "employee_id": employee_id,
            "time_off_type": time_off_type,
            "status": status,
            "start_date": time_off_data.get("start") or "",
            "end_date": time_off_data.get("end") or "",
        }
        
        # Get updated timestamp
        updated_at_str = None
        if isinstance(time_off_data.get("status"), dict) and "lastChanged" in time_off_data.get("status", {}):
            updated_at_str = str(time_off_data["status"]["lastChanged"])
        else:
            updated_at_str = str(time_off_data.get("lastModified")) if time_off_data.get("lastModified") else None
        
        full_title = f"{title} - {self.company_name}"  # Include company in title
        
        return Document(
            id=f"bamboohr_time_off_{time_off_id}",
            sections=[TextSection(link=url, text=text)],
            source=DocumentSource.BAMBOOHR,
            semantic_identifier=f"Time Off: {title} - {self.company_name}",  # Include company in identifier
            title=full_title,
            doc_updated_at=time_str_to_utc(updated_at_str) if updated_at_str else None,
            metadata=metadata,
        )

    def _get_time_off_batch(
        self,
        start: Optional[SecondsSinceUnixEpoch] = None,
        end: Optional[SecondsSinceUnixEpoch] = None,
        employees: Optional[List[Dict[str, Any]]] = None
    ) -> List[Document]:
        """Get time-off data from BambooHR.

        Args:
            start: Start time for filtering
            end: End time for filtering
            employees: List of employee data for reference

        Returns:
            List of time-off documents
        """
        if not self.bamboohr_client or not self.include_time_off:
            return []
        
        logger.info("Fetching time-off data from BambooHR")
        documents = []
        
        try:
            # Create a mapping of employee IDs to names for easier lookup
            employee_map = {}
            if employees:
                for employee in employees:
                    if "id" in employee:
                        first_name = employee.get("firstName", "")
                        last_name = employee.get("lastName", "")
                        name = f"{first_name} {last_name}".strip()
                        employee_map[str(employee["id"])] = name
            
            # Construct date parameters for the API request
            params = {}
            
            # Use the current year as default if no start/end filters provided
            current_year = datetime.now().year
            default_start = f"{current_year - 1}-01-01"
            default_end = f"{current_year + 1}-12-31"
            
            if start:
                # Convert timestamp to date string
                start_date = datetime.fromtimestamp(start).strftime("%Y-%m-%d")
                params["start"] = start_date
            else:
                params["start"] = default_start
                
            if end:
                # Convert timestamp to date string
                end_date = datetime.fromtimestamp(end).strftime("%Y-%m-%d")
                params["end"] = end_date
            else:
                params["end"] = default_end
            
            # Fetch time-off records
            response = self.bamboohr_client.get("time_off/requests", params)
            
            # Handle both possible response formats from the API
            time_off_records = []
            if isinstance(response, list):
                # API returned a list of time-off records directly
                time_off_records = response
            elif isinstance(response, dict):
                # API returned a dict with a "requests" key containing the records
                time_off_records = response.get("requests", [])
            
            # Process each time-off record
            for record in time_off_records:
                # Apply filters
                if not self._should_include_time_off(record):
                    continue
                    
                # Get employee name if available
                employee_id = str(record.get("employeeId", ""))
                employee_name = employee_map.get(employee_id)
                
                # Create document
                document = self._time_off_to_document(
                    bamboohr_client=self.bamboohr_client,
                    time_off_data=record,
                    employee_name=employee_name
                )
                documents.append(document)
                
                # Avoid rate limiting
                time.sleep(0.1)
                
            return documents
            
        except Exception as e:
            logger.error(f"Error fetching time-off data: {e}")
            return []

    def _get_employees_batch(
        self,
        start: Optional[SecondsSinceUnixEpoch] = None,
        end: Optional[SecondsSinceUnixEpoch] = None,
    ) -> List[Document]:
        """Get employee data from BambooHR.

        Args:
            start: Start time for filtering
            end: End time for filtering

        Returns:
            List of employee documents
        """
        if not self.bamboohr_client:
            raise ConnectorMissingCredentialError("BambooHR")
            
        logger.info("Fetching employee data from BambooHR")
        
        params = {
            "fields": ",".join(DIRECTORY_FIELDS),
        }
        
        try:
            # First get the basic employee data from the directory
            response = self.bamboohr_client.get("employees/directory", params)
            employees = response.get("employees", [])
            
            documents = []
            
            # For each employee, get additional details if needed for filtering
            for employee in employees:
                # Fetch additional employee details
                employee_id = employee.get("id")
                if not employee_id:
                    continue
                    
                details = self._fetch_employee_details(employee_id)
                
                # Apply filters
                if not self._should_include_employee(employee, details):
                    continue
                
                # Apply time filtering for the "updated_since" parameter
                if start or self.updated_since:
                    filter_time = start  # This is already a timestamp (float)
                    if self.updated_since:
                        try:
                            # Convert string date to timestamp
                            updated_since_time = datetime.strptime(
                                self.updated_since, "%Y-%m-%d"
                            ).timestamp()
                            
                            # Use the later of the two timestamps
                            if filter_time is None or (updated_since_time > filter_time):
                                filter_time = updated_since_time
                        except ValueError:
                            # If date parsing fails, continue with the original filter_time
                            pass
                    
                    # Apply the filter if we have both a filter time and a last changed date
                    if filter_time is not None and details.get("lastChanged"):
                        try:
                            # Convert the lastChanged string to a timestamp
                            last_changed = time_str_to_utc(str(details["lastChanged"]))
                            
                            # Skip this employee if it was last changed before our filter time
                            if last_changed is not None and last_changed < filter_time:
                                continue
                        except (TypeError, ValueError):
                            # If conversion fails, include the employee
                            pass
                
                # Apply end time filtering if specified
                if end is not None and details.get("lastChanged"):
                    try:
                        # Convert the lastChanged string to a timestamp
                        last_changed = time_str_to_utc(str(details["lastChanged"]))
                        
                        # Skip if the last changed date is after our end time
                        if last_changed is not None and last_changed > end:
                            continue
                    except (TypeError, ValueError):
                        # If conversion fails, include the employee
                        pass
                
                # Create the employee document
                document = self._employee_to_document(
                    bamboohr_client=self.bamboohr_client,
                    employee=employee,
                    details=details
                )
                documents.append(document)
                
                # Avoid rate limiting
                time.sleep(0.1)
            
            return documents
        except Exception as e:
            logger.error(f"Error fetching employees: {e}")
            return []

    def _get_employee_files(
        self, 
        employee_id: str, 
        employee_name: str,
        start: Optional[SecondsSinceUnixEpoch] = None,
        end: Optional[SecondsSinceUnixEpoch] = None,
    ) -> List[Document]:
        """Get files associated with an employee.

        Args:
            employee_id: Employee ID
            employee_name: Employee name for reference
            start: Start time for filtering
            end: End time for filtering

        Returns:
            List of file documents
        """
        if not self.bamboohr_client or not self.include_files:
            return []
            
        documents = []
        
        try:
            # Make request and get XML response as string
            response = self.bamboohr_client.get(f"employees/{employee_id}/files/view", content_type="xml")
            
            # Parse XML response into file data list
            if isinstance(response, str):
                files = self._parse_employee_files_xml(response, employee_id)
            else:
                # Fallback if response isn't a string - try to get raw content from response object
                if hasattr(response, 'text'):
                    files = self._parse_employee_files_xml(response.text, employee_id)
                else:
                    logger.error(f"Unexpected response type for employee files: {type(response)}")
                    return []
            
            # Filter by date if needed
            if start is not None or end is not None:
                filtered_files = []
                for file_data in files:
                    include_file = True
                    
                    if "lastUpdated" in file_data:
                        try:
                            file_date = time_str_to_utc(str(file_data["lastUpdated"]))
                            
                            # Apply start date filter if provided
                            if start is not None and file_date is not None and file_date < start:
                                include_file = False
                            
                            # Apply end date filter if provided
                            if end is not None and file_date is not None and file_date > end:
                                include_file = False
                                
                        except (TypeError, ValueError):
                            # If date conversion fails, include the file
                            pass
                            
                    if include_file:
                        filtered_files.append(file_data)
                        
                files = filtered_files
            
            # Process each file
            for file_data in files:
                # Apply additional filtering
                if not self._should_include_file(file_data):
                    continue
                    
                try:
                    # Download file content
                    file_id = file_data.get("id")
                    url = f"files/{file_id}/download"
                    content_response = self.bamboohr_client.get(url)
                    
                    # Extract content as text if possible
                    content = ""
                    if isinstance(content_response, str):
                        content = content_response
                    elif isinstance(content_response, dict):
                        if "content" in content_response:
                            content = content_response["content"]
                        else:
                            content = str(content_response)
                    elif hasattr(content_response, "text"):
                        content = content_response.text
                    else:
                        content = str(content_response)
                    
                    # Create document
                    document = self._file_to_document(
                        bamboohr_client=self.bamboohr_client,
                        file_data=file_data,
                        content=content,
                        file_type="employee",
                        owner=employee_name
                    )
                    documents.append(document)
                    
                    # Avoid rate limiting
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.warning(f"Error processing file {file_data.get('id')} for employee {employee_id}: {e}")
                    continue
                    
        except Exception as e:
            logger.warning(f"Error fetching files for employee {employee_id}: {e}")
            
        return documents

    def _get_company_files_batch(
        self,
        start: Optional[SecondsSinceUnixEpoch] = None,
        end: Optional[SecondsSinceUnixEpoch] = None,
    ) -> List[Document]:
        """Get company-wide files from BambooHR.

        Args:
            start: Start time for filtering
            end: End time for filtering

        Returns:
            List of file documents
        """
        if not self.bamboohr_client or not self.include_files:
            return []
            
        documents = []
        
        try:
            # Make request and get XML response as string
            response = self.bamboohr_client.get("files/view", content_type="xml")
            
            # Parse XML response into file data list
            if isinstance(response, str):
                files = self._parse_company_files_xml(response)
            else:
                # Fallback if response isn't a string - try to get raw content from response object
                if hasattr(response, 'text'):
                    files = self._parse_company_files_xml(response.text)
                else:
                    logger.error(f"Unexpected response type for company files: {type(response)}")
                    return []
            
            # Filter by date if needed
            if start is not None or end is not None:
                filtered_files = []
                for file_data in files:
                    include_file = True
                    
                    if "lastUpdated" in file_data:
                        try:
                            file_date = time_str_to_utc(str(file_data["lastUpdated"]))
                            
                            # Apply start date filter if provided
                            if start is not None and file_date is not None and file_date < start:
                                include_file = False
                            
                            # Apply end date filter if provided
                            if end is not None and file_date is not None and file_date > end:
                                include_file = False
                                
                        except (TypeError, ValueError):
                            # If date conversion fails, include the file
                            pass
                            
                    if include_file:
                        filtered_files.append(file_data)
                        
                files = filtered_files
            
            # Process each file
            for file_data in files:
                # Apply additional filtering
                if not self._should_include_file(file_data):
                    continue
                    
                try:
                    # Download file content
                    file_id = file_data.get("id")
                    url = f"files/{file_id}/download"
                    content_response = self.bamboohr_client.get(url)
                    
                    # Extract content as text if possible
                    content = ""
                    if isinstance(content_response, str):
                        content = content_response
                    elif isinstance(content_response, dict):
                        if "content" in content_response:
                            content = content_response["content"]
                        else:
                            content = str(content_response)
                    elif hasattr(content_response, "text"):
                        content = content_response.text
                    else:
                        content = str(content_response)
                    
                    # Create document
                    document = self._file_to_document(
                        bamboohr_client=self.bamboohr_client,
                        file_data=file_data,
                        content=content,
                        file_type="company",
                    )
                    documents.append(document)
                    
                    # Avoid rate limiting
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.warning(f"Error processing company file {file_data.get('id')}: {e}")
                    continue
                    
        except Exception as e:
            logger.warning(f"Error fetching company files: {e}")
            
        return documents

    def load_from_state(self) -> GenerateDocumentsOutput:
        """Load all documents from BambooHR.

        Returns:
            Generator yielding batches of documents
        """
        if self.bamboohr_client is None:
            raise ConnectorMissingCredentialError("BambooHR")

        return self.poll_source(None, None)

    def poll_source(
        self, start: Optional[SecondsSinceUnixEpoch], end: Optional[SecondsSinceUnixEpoch]
    ) -> GenerateDocumentsOutput:
        """Poll BambooHR for new or updated documents.

        Args:
            start: Start time for filtering
            end: End time for filtering

        Returns:
            Generator yielding batches of documents
        """
        if self.bamboohr_client is None:
            raise ConnectorMissingCredentialError("BambooHR")

        # Get employee documents
        employee_docs = self._get_employees_batch(start, end)
        if employee_docs:
            # Process in batches
            for i in range(0, len(employee_docs), self.batch_size):
                batch = employee_docs[i:i+self.batch_size]
                yield batch
            
            # Process employee files if needed
            if self.include_files:
                for employee_doc in employee_docs:
                    employee_id = employee_doc.metadata.get("employee_id")
                    employee_name = employee_doc.title
                    
                    if employee_id:
                        file_docs = self._get_employee_files(employee_id, employee_name, start, end)
                        
                        # Process in batches
                        for i in range(0, len(file_docs), self.batch_size):
                            batch = file_docs[i:i+self.batch_size]
                            yield batch
                            
                        # Avoid rate limiting
                        time.sleep(0.2)

        # Get company files if needed
        if self.include_files:
            company_file_docs = self._get_company_files_batch(start, end)
            
            # Process in batches
            for i in range(0, len(company_file_docs), self.batch_size):
                batch = company_file_docs[i:i+self.batch_size]
                yield batch
                
        # Get time-off records if needed
        if self.include_time_off:
            # Fetch all employees for the name mapping
            try:
                employee_directory = self.bamboohr_client.get("employees/directory", {"fields": ",".join(DIRECTORY_FIELDS)})
                time_off_docs = self._get_time_off_batch(start, end, employees=employee_directory.get("employees", []))
            except Exception as e:
                logger.warning(f"Error fetching employee directory for time-off records: {e}")
                time_off_docs = self._get_time_off_batch(start, end)
            
            # Process in batches
            for i in range(0, len(time_off_docs), self.batch_size):
                batch = time_off_docs[i:i+self.batch_size]
                yield batch

    def validate_connector_settings(self) -> None:
        """
        Validate that the BambooHR credentials and connector settings are correct.
        Specifically checks that we can make an authenticated request to BambooHR.
        """
        if not self.bamboohr_client:
            raise ConnectorMissingCredentialError(
                "BambooHR credentials have not been loaded."
            )

        try:
            # Attempt to fetch a small amount of data to verify credentials
            _ = self.bamboohr_client.get("employees/directory", {"limit": "1"})

        except BambooHRClientRequestFailedError as e:
            # Check for HTTP status codes
            if e.status_code == 401:
                raise CredentialExpiredError(
                    "Your BambooHR credentials appear to be invalid or expired (HTTP 401)."
                ) from e
            elif e.status_code == 403:
                raise InsufficientPermissionsError(
                    "The configured BambooHR API key does not have sufficient permissions (HTTP 403)."
                ) from e
            else:
                raise ConnectorValidationError(
                    f"Unexpected BambooHR error (status={e.status_code}): {e}"
                ) from e

        except Exception as exc:
            raise ConnectorValidationError(
                f"Unexpected error while validating BambooHR connector settings: {exc}"
            ) from exc

    def get_connector_config(self) -> Dict[str, Any]:
        """Get connector configuration for frontend."""
        return {
            "indexing_scope": self.indexing_scope,
            "departments": self.departments,
            "job_titles": self.job_titles,
            "employment_status": self.employment_status,
            "file_categories": self.file_categories,
            "time_off_types": self.time_off_types,
            "include_files": self.include_files,
            "include_time_off": self.include_time_off,
            "hire_date_after": self.hire_date_after,
            "updated_since": self.updated_since,
        }