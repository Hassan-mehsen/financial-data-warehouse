from el.el_job import ExtractLoadPipeline
from db.raw_schema import income_stmt
from time import sleep


class IncomeStatementELJob(ExtractLoadPipeline):
    """Extract and load companies income statement data from /income-statement endpoint of FMP API with retry mecanism"""

    period = ["Q1", "Q2", "Q3", "Q4"]
    MAX_RETRIES = 3

    def __init__(self):
        super().__init__(source="FMP-Income Statement", table=income_stmt)

    def run(self) -> None:
        """Main execution method for extracting and loading companies income statement data"""
        all_data = []

        self._log(header="EXTRACTING Start - endpoint: Income Statement")
        self._log(message="Starting batch extraction...", phase="EXTRACT")

        for symbol in self.available_companies:
            for period in self.period:
                data = self.extract(endpoint="/income-statement", query=f"symbol={symbol}&period={period}")

                if not data and self.status_code != 200:
                    for attempt in range(1, self.MAX_RETRIES + 1):
                        self._log(
                            message=f"Retry {attempt}/{self.MAX_RETRIES} - failed to extract income statement data for {symbol} in period {period}. Retrying in {2**attempt}s...",
                            phase="EXTRACT",
                        )
                        sleep(2**attempt)
                        data = self.extract(endpoint="/income-statement", query=f"symbol={symbol}&period={period}")
                        if data:
                            break
                if data:
                    all_data.extend(data)

        self._log(header="EXTRACTING End - endpoint: Income Statement")

        self._log(header="LOADING Start - endpoint: Income Statement")

        if all_data:
            self.load(all_data)
            self._log(message=f"Batch size: {len(all_data)} income statements.", phase="LOAD")
        else:
            self._log(message="No data collected.", phase="LOAD")

        self._log(header="LOADING End - endpoint: Income Statement")
