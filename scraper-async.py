import asyncio
import logging
import pdb
from typing import List

import asyncpg
import aiohttp
from bs4 import BeautifulSoup

BASE_URL = "https://www.goodreads.com"


class WebScraper(object):
    def __init__(self, urls):
        self.urls = urls
        # Global Place To Store The Data:
        self.all_data = []
        self.master_dict = {}
        self.db_conn = None
        # Run The Scraper:
        asyncio.run(self.main())

    async def fetch(self, session, url):
        try:
            async with session.get(url) as response:
                # 1. Extracting the Text:
                text = await response.text()
                # 2. Extracting the  Tag:
                loop = asyncio.get_event_loop()
                urls_to_scrape = await loop.run_in_executor(None, self.extract_title_tag, text)
                await self.add_urls_to_db(urls_to_scrape)
                return urls_to_scrape
        except Exception as e:
            print(str(e))

    async def add_urls_to_db(self, urls_to_add: List[str]):
        db_conn = await asyncpg.connect(user='postgres', password='password',
                                        database='quotes', host='localhost')
        try:
            await db_conn.copy_records_to_table("quote_urls", records=urls_to_add)
        except Exception as e:
            logging.exception(f"Error adding records to DB: {e}")

    def extract_title_tag(self, text):
        try:
            soup = BeautifulSoup(text, 'html.parser')
            urls = soup.find_all("a", class_="smallText")
            return [tuple([f"{BASE_URL}{url['href']}"]) for url in urls]
        except Exception as e:
            print(str(e))

    async def main(self):
        tasks = []
        headers = {
            "user-agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"}
        async with aiohttp.ClientSession(headers=headers) as session:
            for url in self.urls:
                tasks.append(self.fetch(session, url))

            htmls = await asyncio.gather(*tasks)

            for html in htmls:
                self.all_data.extend(html)


urls = [f"https://www.goodreads.com/quotes?page={i}" for i in range(1, 101)]
scraper = WebScraper(urls=urls)
print(f"# of urls collected: {len(scraper.all_data)}")
print()
