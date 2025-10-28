from el.el_job import ExtractLoadPipeline
from db.raw_schema import dividends
from time import sleep


class DividendsELJob(ExtractLoadPipeline):
    """Extract and load company dividends data from /divindes and /dividends-calendar endpoints of FMP API with retry mecanism"""

    MAX_RETRIES = 3

    def __init__(self):
        super().__init__(source="FMP-Dividends", table=dividends)

    def run(self) -> None:
        """Main execution method for extracting and loading dividends data"""
        all_data = []

        self._log(header="EXTRACTING Start - endpoint: Dividends")
        self._log(message="Starting batch extraction...", phase="EXTRACT")

        # ---------------------          Dividends Company real time          ---------------------
        for symbol in self.available_companies:
            self._log(message="Extracting Dividends Company", phase="EXTRACT")
            data = self.extract(endpoint="/dividends", query=f"symbol={symbol}")

            if not data and self.status_code != 200:
                for attempt in range(1, self.MAX_RETRIES + 1):
                    self._log(
                        message=f"Retry {attempt}/{self.MAX_RETRIES} - failed to extract dividends for {symbol}. Retrying in {2**attempt}s...",
                        phase="EXTRACT",
                    )
                    sleep(2**attempt)
                    data = self.extract(endpoint="/dividends", query=f"symbol={symbol}")
                    if data:
                        break
            if data:
                all_data.extend(data)

        # ---------------------          Dividends history           ---------------------
        self._log(message="Extracting Dividends Calendar", phase="EXTRACT")
        data = self.extract(endpoint="/dividends-calendar")

        if not data and self.status_code != 200:
            for attempt in range(1, self.MAX_RETRIES + 1):
                self._log(
                    message=f"Retry {attempt}/{self.MAX_RETRIES} - failed to extract dividends calender. Retrying in {2**attempt}s...",
                    phase="EXTRACT",
                )
                sleep(2**attempt)
                data = self.extract(endpoint="/dividends-calendar")
                if data:
                    break
            if data:
                all_data.extend(data)

        self._log(header="EXTRACTING End - endpoint: Dividends")

        self._log(header="LOADING Start - endpoint: Dividends")

        if all_data:
            self.load(all_data)
            self._log(message=f"Batch size: {len(all_data)} dividends.", phase="LOAD")
        else:
            self._log(message="No data collected.", phase="LOAD")

        self._log(header="LOADING End - endpoint: Dividends")
