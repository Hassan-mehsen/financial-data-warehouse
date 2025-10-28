from el.el_job import ExtractLoadPipeline
from db.raw_schema import earnings
from time import sleep


class EarningsELJob(ExtractLoadPipeline):
    """Extract and load company earnings data from /earnings and /earnings-calendar endpoints of FMP API with retry mecanism."""

    MAX_RETRIES = 3

    def __init__(self):
        super().__init__(source="FMP-Earnings", table=earnings)

    def run(self) -> None:
        """Main execution method for extracting and loading earnings data"""
        all_data = []

        self._log(header="EXTRACTING Start - endpoint: Earnings")
        self._log(message="Starting batch extraction...", phase="EXTRACT")

        # ---------------------          Earnings Report real time          ---------------------
        for symbol in self.available_companies:
            self._log(message="Extracting Earnings Report", phase="EXTRACT")
            data = self.extract(endpoint="/earnings", query=f"symbol={symbol}")

            if not data and self.status_code != 200:
                for attempt in range(1, self.MAX_RETRIES + 1):
                    self._log(
                        message=f"Retry {attempt}/{self.MAX_RETRIES} - failed to extract earnings report for {symbol}. Retrying in {2**attempt}s...",
                        phase="EXTRACT",
                    )
                    sleep(2**attempt)
                    data = self.extract(endpoint="/earnings", query=f"symbol={symbol}")
                    if data:
                        break
            if data:
                all_data.extend(data)

        # ---------------------          Earnings Report history        ---------------------
        self._log(message="Extracting Earnings Calendar", phase="EXTRACT")
        data = self.extract(endpoint="/earnings-calendar")

        if not data and self.status_code != 200:
            for attempt in range(1, self.MAX_RETRIES + 1):
                self._log(
                    message=f"Retry {attempt}/{self.MAX_RETRIES} - failed to extract earnings calender. Retrying in {2**attempt}s...",
                    phase="EXTRACT",
                )
                sleep(2**attempt)
                data = self.extract(endpoint="/earnings-calendar")
                if data:
                    break
            if data:
                all_data.extend(data)

        self._log(header="EXTRACTING End - endpoint: Earnings")

        self._log(header="LOADING Start - endpoint: Earnings")

        if all_data:
            self.load(all_data)
            self._log(message=f"Batch size: {len(all_data)} earnings report.", phase="LOAD")
        else:
            self._log(message="No data collected.", phase="LOAD")

        self._log(header="LOADING End - endpoint: Earnings")
