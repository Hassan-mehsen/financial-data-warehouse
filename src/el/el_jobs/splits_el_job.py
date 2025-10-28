from el.el_job import ExtractLoadPipeline
from db.raw_schema import splits
from time import sleep


class SplitsELJob(ExtractLoadPipeline):
    """Extract and load historical stock splits data from /spilts endpoint FMP API with retry mecanism"""

    MAX_RETRIES = 3

    def __init__(self):
        super().__init__(source="FMP-Splits", table=splits)

    def run(self) -> None:
        """Main execution method for extracting and loading companies splits data"""
        all_data = []

        self._log(header="EXTRACTING Start - endpoint: Splits Details")
        self._log(message="Starting batch extraction...", phase="EXTRACT")

        for symbol in self.available_companies:

            data = self.extract(endpoint="/splits", query=f"symbol={symbol}")

            if not data and self.status_code != 200:
                for attempt in range(1, self.MAX_RETRIES + 1):
                    self._log(
                        message=f"Retry {attempt}/{self.MAX_RETRIES} - failed to extract splits details for {symbol}. Retrying in {2**attempt}s...",
                        phase="EXTRACT",
                    )
                    sleep(2**attempt)
                    data = self.extract(endpoint="/splits", query=f"symbol={symbol}")
                    if data:
                        break
            if data:
                all_data.extend(data)

        self._log(header="EXTRACTING End - endpoint: Splits Details")

        self._log(header="LOADING Start - endpoint: Splits Details")

        if all_data:
            self.load(all_data)
            self._log(message=f"Batch size: {len(all_data)} splits", phase="LOAD")
        else:
            self._log(message="No data collected.", phase="LOAD")

        self._log(header="LOADING End - endpoint: Splits Details")
