from el.el_job import ExtractLoadPipeline
from db.raw_schema import stock_batch_quotes
from time import sleep


class StockQuoteELJob(ExtractLoadPipeline):
    """Extract and load stock quotes from /quote endpoint of FMP API with retry mechanism"""

    MAX_RETRIES = 3

    def __init__(self):
        super().__init__(source="FMP-Stock Quote", table=stock_batch_quotes)

    def run(self) -> None:
        """Main execution method for extracting and loading stock quote data"""
        all_data = []

        self._log(header="EXTRACTING Start - endpoint: Stock Quote")
        self._log(message="Starting batch extraction...", phase="EXTRACT")

        for symbol in self.available_companies:

            data = self.extract(endpoint="/quote", query=f"symbol={symbol}")

            if not data and self.status_code != 200:
                for attempt in range(1, self.MAX_RETRIES + 1):
                    self._log(
                        message=f"Retry {attempt}/{self.MAX_RETRIES} - failed to extract quotes. Retrying in {2**attempt}s...",
                        phase="EXTRACT",
                    )
                    sleep(2**attempt)
                    data = self.extract(endpoint="/quote", query=f"symbol={symbol}")
                    if data:
                        break
            if data:
                all_data.extend(data)

        self._log(header="EXTRACTING End - endpoint: Stock Quote")

        self._log(header="LOADING Start - endpoint: Stock Quote")

        if all_data:
            self.load(all_data)
            self._log(message=f"Batch size: {len(all_data)} quotes.", phase="LOAD")
        else:
            self._log(message="No data collected.", phase="LOAD")

        self._log(header="LOADING End - endpoint: Stock Quote")
