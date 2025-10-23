from .database_connector import DatabaseConnector
from datetime import datetime, timezone
from abc import ABC, abstractmethod
from typing import List, Optional
from dotenv import load_dotenv
from sqlalchemy import insert
from pathlib import Path
import requests
import os

load_dotenv()


class ExtractLoadPipeline(ABC, DatabaseConnector):
    """
    Abstract base class for unified Extract & Load (EL) pipelines from the FMP API.

    This class provides a reusable framework for all endpoint-specific extractors.
    It handles:
        - Building and sending HTTP requests to the Financial Modeling Prep API.
        - Logging extraction and load operations with timestamps and phases.
        - Inserting the full JSON API response into a specified raw table in PostgreSQL.
        - Managing database connections through the inherited DatabaseConnector.

    Child classes must implement the `run()` method to define their specific extraction and load logic.
    They should provide:
        - `source`: the logical name of the dataset (e.g., "dividends", "balance_sheet").
        - `table`: the SQLAlchemy Table object representing the target raw table.
    """

    api_key = os.getenv("FMP_API_KEY")
    base_url = "https://financialmodelingprep.com"
    api_version = "/stable"

    log_path = Path(__file__).resolve().parents[2] / "logs" / "ingestion.log"

    def __init__(self, source: str, table: str):
        super().__init__()
        self.source = source
        self.table = table  # sqlalchemy table  object passed by child classes
        self.session = requests.Session()

    def extract(self, endpoint: str, query: str = "") -> Optional[List[dict]]:
        """Extract data from Financial Modeling Prep API and return JSON response."""
        self._log("Starting extraction...", phase="Extract")

        if not self.api_key:
            error_msg = "FMP key not found in environment variables."
            self._log(error_msg)
            raise ValueError(error_msg)

        url = self._build_url(endpoint=endpoint, query=query)

        try:
            self._log(f"Sending request to {url}")
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            record_count = len(data) if isinstance(data, list) else 1
            self._log(f"Response received with status {response.status_code} - {record_count} records fetched")
            return data

        except requests.RequestException as exception:
            self._log(f"API request failed: {exception}")
            return None

    def load(self, raw_data: list[dict]) -> None:
        """Load the API response into the raw table"""
        self._log("Starting load...", phase="LOAD")

        row = {"json_list": raw_data, "source": self.source}
        stmt = insert(self.table)

        try:
            # write the data
            with self.engine.begin() as conn:
                conn.execute(stmt, row)
            self._log(f"Load done - 1 record inserted", phase="LOAD")

        except Exception as e:
            self._log(f"Failed to insert the data: {e}", phase="LOAD")

    @abstractmethod
    def run(self):
        """Must be implemented by child class: controls full extraction and load logic."""
        pass

    # ---------------------------------------------------------
    #                   Private helpers
    # ---------------------------------------------------------

    def _log(self, message: str, header: str, phase: str):
        """
        Write a formatted, timestamped log entry to the EL log file.

        Args:
            message (str): The message to log.
            header (str, optional): Optional section header displayed as a separator block.
            phase (str, optional): Current EL phase (EXTRACT or LOAD).
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        full_message = f"[{timestamp}] [{phase.upper()}] [{self.source.upper()}] {message}"

        with open(self.log_path, "a") as f:

            if header:
                f.write("\n" + "=" * 50 + "\n")
                f.write(header.center(50))
                f.write("\n" + "=" * 50 + "\n")

            f.write(full_message + "\n")

    def _build_url(self, endpoint: str, query: str) -> str:
        """
        Build the full Financial Modeling Prep API request URL.

        This method dynamically constructs the request URL based on the provided
        endpoint and optional query parameters. It appends the API key and handles
        both cases where a query string is provided or not.

        Examples:
            Without query parameters:
                https://financialmodelingprep.com/stable/dividends-calendar?apikey=api_token

            With query parameters:
                https://financialmodelingprep.com/stable/dividends?symbol=AAPL&apikey=api_token

        Returns:
            str: The fully constructed API URL ready for HTTP requests.
        """
        if not query:
            return f"{self.base_url}{self.api_version}{endpoint}?apikey={self.api_key}"

        return f"{self.base_url}{self.api_version}{endpoint}?{query}&apikey={self.api_key}"
