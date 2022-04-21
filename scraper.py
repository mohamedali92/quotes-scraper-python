import bs4
import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool

BASE_URL = "https://www.goodreads.com"
BASE_QUOTES_URL = "https://www.goodreads.com/quotes?page="
QUOTE_URLS_TO_SCRAPE = []
urls = [f"https://www.goodreads.com/quotes?page={i}" for i in range(1, 20)]

def scrape_quote(url):
    res = requests.get(url)
    soup = BeautifulSoup(res.content, "html.parser")

    # quote_text =
    # author =
    # tags =


def get_quote_urls(base_url):
    res = requests.get(base_url)
    soup = BeautifulSoup(res.content, "html.parser")
    urls = soup.find_all("a", class_="smallText")
    for url in urls:
        quote_url = f"{BASE_URL}{url['href']}"
        QUOTE_URLS_TO_SCRAPE.append(quote_url)


if __name__ == '__main__':
    for url in urls:
        get_quote_urls(url)

    print(f"# of urls collected: {len(QUOTE_URLS_TO_SCRAPE)}")