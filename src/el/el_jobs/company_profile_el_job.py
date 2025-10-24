from el.el_job import ExtractLoadPipeline
from db.raw_schema import company_profile
from time import sleep


class CompanyProfileELJob(ExtractLoadPipeline):
    """Extract and load company profiles from /profile endpoint of FMP API with retry mechanism."""

    MAX_RETRIES = 3

    def __init__(self):
        super().__init__(source="FMP - Company Profile", table=company_profile)

    def run(self):
        """Main execution method for extracting and loading company profiles."""
        all_data = []

        self._log(header="EXTRACTING Start - Company Profile")
        self._log("Starting batch extraction...", phase="EXTRACT")

        for symbol in self.available_companies:
            attempt = 1
            data = self.extract(endpoint="/profile", query=f"symbol={symbol}")

            if not data:
                while attempt <= self.MAX_RETRIES:
                    self._log(
                        f"Retry {attempt}/{self.MAX_RETRIES} - failed to extract company profile for {symbol}. Retrying in {2**attempt}s...",
                        phase="EXTRACT",
                    )
                    sleep(2**attempt)
                    data = self.extract(endpoint="/profile", query=f"symbol={symbol}")
                    if data:
                        break
                    attempt += 1

            if data:
                all_data.append(data)

        self._log(header="EXTRACTING End - Company Profile")

        if all_data:
            self._log(header="LOADING Start - Company Profile")
            self._log(f"Batch size: {len(all_data)} symbols collected. Starting load...", phase="LOAD")
            self.load(all_data)

        else:
            self._log("No data collected.", phase="LOAD")

        self._log(header="LOADING End - Company Profile")
