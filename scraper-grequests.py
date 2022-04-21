import grequests
from bs4 import BeautifulSoup

BASE_URL = "https://www.goodreads.com"
urls = [f"https://www.goodreads.com/quotes?page={i}" for i in range(1, 20)]
QUOTE_URLS_TO_SCRAPE = []

reqs = (grequests.get(url) for url in urls)
resps = grequests.imap(reqs, grequests.Pool())


for r in resps:
    soup = BeautifulSoup(r.text, "html.parser")
    urls = soup.find_all("a", class_="smallText")
    for url in urls:
        quote_url = f"{BASE_URL}{url['href']}"
        QUOTE_URLS_TO_SCRAPE.append(quote_url)

print(f"# of urls collected: {len(QUOTE_URLS_TO_SCRAPE)}")