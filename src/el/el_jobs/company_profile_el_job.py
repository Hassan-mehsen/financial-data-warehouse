from el.el_job import ExtractLoadPipeline
from db.raw_schema import company_profile
from time import sleep


class CompanyProfileELJob(ExtractLoadPipeline):
    """Extract and load company profiles from /profile endpoint of FMP API with retry mechanism."""

    MAX_RETRIES = 3

    def __init__(self):
        super().__init__(source="FMP-Company Profile", table=company_profile)

    def run(self) -> None:
        """Main execution method for extracting and loading company profiles data"""
        all_data = []

        self._log(header="EXTRACTING Start - endpoint: Company Profile")
        self._log(message="Starting batch extraction...", phase="EXTRACT")

        for symbol in self.available_companies:

            data = self.extract(endpoint="/profile", query=f"symbol={symbol}")

            if not data and self.status_code != 200:
                for attempt in range(1, self.MAX_RETRIES + 1):
                    self._log(
                        message=f"Retry {attempt}/{self.MAX_RETRIES} - failed to extract company profile for {symbol}. Retrying in {2**attempt}s...",
                        phase="EXTRACT",
                    )
                    sleep(2**attempt)
                    data = self.extract(endpoint="/profile", query=f"symbol={symbol}")
                    if data:
                        break
            if data:
                all_data.extend(data)

        self._log(header="EXTRACTING End - endpoint: Company Profile")

        self._log(header="LOADING Start - endpoint: Company Profile")

        if all_data:
            self.load(all_data)
            self._log(message=f"Batch size: {len(all_data)} companies profile.", phase="LOAD")
        else:
            self._log(message="No data collected.", phase="LOAD")

        self._log(header="LOADING End - endpoint: Company Profile")
