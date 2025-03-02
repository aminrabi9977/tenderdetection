from abc import ABC, abstractmethod
import pandas as pd


class BaseScraper(ABC):
    def __init__(self, base_url: str):
        self.base_url = base_url

    @abstractmethod
    async def init_browser(self):
        pass

    @abstractmethod
    async def close_browser(self):
        pass

    @abstractmethod
    async def extract_table_data(self):
        pass

    @abstractmethod
    async def check_next_page(self) -> bool:
        pass

    @abstractmethod
    async def scrape_data(self) -> pd.DataFrame:
        pass