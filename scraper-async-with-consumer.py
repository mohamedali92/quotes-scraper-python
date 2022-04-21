import asyncio
import dataclasses
import datetime
import logging
import pdb
from typing import List

import asyncpg
import aiohttp
from bs4 import BeautifulSoup

BASE_URL = "https://www.goodreads.com"


@dataclasses.dataclass
class Quote:
    quote_url: str
    quote_text: str
    author: str
    tags: List[str]
    tags_links: List[str]
    other_quotes_by_author_url: str
    likes: int
    created_at: datetime.datetime = datetime.datetime.now()


class WebScraper(object):
    def __init__(self, urls):
        self.urls = urls
        # Global Place To Store The Data:
        self.all_data = []
        self.master_dict = {}
        self.db_conn = None
        # Run The Scraper:
        asyncio.run(self.main())

    async def fetch(self, name, session, url, queue):
        print(f"Fetcher {name} starting ...")
        try:
            async with session.get(url) as response:
                # 1. Extracting the Text:
                text = await response.text()
                # 2. Extracting the  Tag:
                loop = asyncio.get_event_loop()
                urls_to_scrape = await loop.run_in_executor(None, self.extract_title_tag, name, text)
                for url_to_scrape in urls_to_scrape:
                    await queue.put(url_to_scrape)
                await queue.put(None)
                #await self.add_urls_to_db(urls_to_scrape)
                print(f"Fetcher {name} ending ...")
                return urls_to_scrape
        except Exception as e:
            print(str(e))

    async def get_asyncpg_connection(self):
        connection = await asyncpg.connect(user='postgres', password='password',
                              database='quotes', host='localhost')
        return connection

    # async def add_urls_to_db(self, urls_to_add: List[str]):
    #     db_conn = self.get_asyncpg_connection()
    #     try:
    #         await db_conn.copy_records_to_table("quote_urls", records=urls_to_add)
    #     except Exception as e:
    #         logging.exception(f"Error adding records to DB: {e}")
    async def consumer(self, name, queue):
        print(f"Consumer {name} starting ...")
        while True:
            try:
                item = await queue.get()
                if not item:
                    break
                await self.add_url_to_scrape_to_db(name, item)
                # Make request to get quote info
                quote_details = await self.get_quote_details(name, item)
                # Persist to db
                await self.add_quote_to_db(name, quote_details)
                await self.add_tags_links_to_db(name, quote_details.tags_links)
                await self.add_urls_for_other_quotes_by_author_to_db(name, quote_details.other_quotes_by_author_url)
                print(f"Consumer {name} ending ...")
            except Exception as e:
                logging.exception(f"Error in consumer: {e}")


    async def add_urls_for_other_quotes_by_author_to_db(self, name, url: str):
        print(f"add_urls_for_other_quotes_by_author_to_db {name} starting ...")
        try:
            connection = await self.get_asyncpg_connection()
            query = """
                    INSERT INTO urls_for_other_quotes_by_author(other_quotes_by_author_url)
                    VALUES ($1)
                    ON CONFLICT DO NOTHING
            """
            await connection.execute(query, url)
            print(f"add_urls_for_other_quotes_by_author_to_db {name} ending ...")

        except Exception as e:
            logging.exception(f"Error adding urls for other quotes by author to DB: {e}")

    async def add_tags_links_to_db(self, name: str, tags_links: List[str]):
        print(f"add_tags_links_to_db {name} starting ...")
        connection = await self.get_asyncpg_connection()
        query = """
                CREATE TEMPORARY TABLE _data(
                url text
            )
        """
        await connection.execute(query)
        try:
            tag_links_tuples = [tuple([tl]) for tl in tags_links]
            await connection.copy_records_to_table("_data", records=tag_links_tuples)
            q = """
                INSERT INTO tag_links(tag_url)
                SELECT * FROM _data
                ON CONFLICT (tag_url)
                DO NOTHING
            """
            await connection.execute(q)
            print(f"add_tags_links_to_db {name} ending ...")
        except Exception as e:
            logging.exception(f"Error adding tags links records to DB: {e}")


    async def add_quote_to_db(self, name: str, quote: Quote):
        print(f"add_quote_to_db {name} starting ...")
        try:
            connection = await self.get_asyncpg_connection()
            query = """
                    INSERT INTO quotes(quote_url, quote_text, author, tags, likes, tags_links)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT DO NOTHING
            """
            await connection.execute(query, quote.quote_url, quote.quote_text, quote.author, quote.tags, quote.likes, quote.tags_links)
            print(f"add_quote_to_db {name} ending ...")
        except Exception as e:
            logging.exception(f"Error adding quote object to db, error: {e}")

    async def get_quote_details(self, name, url):
        print(f"get_quote_details {name} starting ...")
        headers = {
            "user-agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url) as response:
                text = await response.text()
                #quote = await self.extract_quote_details_from_html(name, url, text)
                loop = asyncio.get_event_loop()
                quote_details = await loop.run_in_executor(None, self.extract_quote_details_from_html, name, url, text)

        print(f"get_quote_details {name} ending ...")
        return quote_details

    def extract_quote_details_from_html(self, name, quote_url, response):
        print(f"extract_quote_details_from_html {name} starting ...")
        try:
            soup = BeautifulSoup(response, 'html.parser')

            quote_text = soup.find("h1", class_="quoteText").get_text(strip=True)
            author = soup.find("span", class_="authorOrTitle").get_text(strip=True).replace(",", "")
            likes_str = soup.find("span", class_="uitext smallText").get_text(strip=True).split(" likes")[0]
            other_quotes_by_author_relative_url = soup.find("a", class_="actionLink")["href"]
            other_quotes_by_author_absolute_url = f"{BASE_URL}{other_quotes_by_author_relative_url}"
            try:
                likes = int(likes_str)
            except Exception as e:
                likes = 0
                logging.exception(f"Failed to convert the number of likes string to an int: {e}")

            try:
                tags_html = soup.find("div", class_="greyText smallText left").find_all("a")
                tags = [tag.get_text() for tag in tags_html]
                tags_links = [f"{BASE_URL}{tag_html['href']}" for tag_html in tags_html]
            except Exception as e:
                logging.exception(f"Failed to extract the tags: {e}, url: {quote_url}")
                tags = []
                tags_links = []

            quote = Quote(author=author,
                          quote_url=quote_url,
                          quote_text=quote_text,
                          likes=likes,
                          tags=tags,
                          tags_links=tags_links,
                          other_quotes_by_author_url=other_quotes_by_author_absolute_url)
            return quote
            print(f"extract_quote_details_from_html {name} ending ...")
        except Exception as e:
            logging.exception(f"Error extracting quote details from the html: {e}, url: {quote_url}")

    async def add_url_to_scrape_to_db(self, name, url):
        print(f"add_url_to_scrape_to_db {name} starting ...")
        connection = await self.get_asyncpg_connection()
        query = """
                INSERT INTO quote_urls(quote_url)
                VALUES ($1)
                ON CONFLICT DO NOTHING
        """
        await connection.execute(query, url)
        print(f"add_url_to_scrape_to_db {name} ending ...")

    def extract_title_tag(self, name, text):
        print(f"extract_title_tag {name} starting ...")
        try:
            soup = BeautifulSoup(text, 'html.parser')
            urls = soup.find_all("a", class_="smallText")
            print(f"extract_title_tag {name} ending ...")
            return [f"{BASE_URL}{url['href']}" for url in urls]
        except Exception as e:
            print(str(e))

    async def main(self):
        queue = asyncio.Queue(500)
        tasks = []
        headers = {
            "user-agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"}
        async with aiohttp.ClientSession(headers=headers) as session:
            for index, url in enumerate(self.urls):
                tasks.append(self.fetch(f"url_fetcher{index}", session, url, queue))
                tasks.append(self.consumer(f"consumer{index}", queue))

            await asyncio.gather(*tasks)

            # for html in htmls:
            #     self.all_data.extend(html)


urls = [f"https://www.goodreads.com/quotes?page={i}" for i in range(1, 3)]
scraper = WebScraper(urls=urls)
print(f"# of urls collected: {len(scraper.all_data)}")
print()
