from el.el_job import ExtractLoadPipeline
from db.raw_schema import all_shares_float
from time import sleep


class SharesFloatELJob(ExtractLoadPipeline):
    """Extract and load all shares float data from /shares-float-all endpoint of FMP API with retry mechanism."""

    MAX_RETRIES = 3

    def __init__(self):
        super().__init__(source="FMP-All Shares Float", table=all_shares_float)
        self.page = 0
        self.limit = 5000

    def run(self) -> None:
        """Main execution method for extracting and loading shares float data"""

        self._log(header="EXTRACTING Start - endpoint: All Shares Float")
        self._log(message="Starting extraction...", phase="EXTRACT")

        data = self.extract(endpoint="/shares-float-all", query=f"page={self.page}&limit={self.limit}")
        if not data and self.status_code != 200:
            for attempt in range(1, self.MAX_RETRIES + 1):
                self._log(
                    message=f"Retry {attempt}/{self.MAX_RETRIES} - failed to extract All Shares Float. Retrying in {2**attempt}s...",
                    phase="EXTRACT",
                )
                sleep(2**attempt)
                data = self.extract(endpoint="/shares-float-all", query=f"page={self.page}&limit={self.limit}")
                if data:
                    break

        self._log(header="EXTRACTING End - endpoint: All Shares Float")

        self._log(header="LOADING Start - endpoint: All Shares Float")

        if data:
            self.load(data)
            self._log(message=f"Batch size: {len(data)} shares", phase="LOAD")
        else:
            self._log(message="No data collected.", phase="LOAD")

        self._log(header="LOADING End - endpoint: All Shares Float")
