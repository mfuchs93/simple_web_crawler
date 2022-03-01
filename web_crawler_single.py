from time import sleep
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests as requests
from bs4 import BeautifulSoup


class MyWebCrawler:
    def __init__(self) -> None:
        super().__init__()
        self.robots = {}
        self.urls = []
        self.data = []
        self.user_agent = "MyWebCrawlingBot2407"
        self.url_storage = []
        self.start_url = ""

    def get_robot(self, start_url: str) -> None:
        robot = RobotFileParser()
        robot.set_url(start_url + "/robots.txt")
        robot.read()
        self.robots[start_url] = robot

    def crawl(self, start_url: str) -> None:
        parsed_url = urlparse(start_url)
        base_url = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_url)
        self.start_url = base_url
        self.urls.append(base_url)
        self.get_robot(base_url)
        if self.robots[base_url].site_maps():
            self.append_urls(self.robots[base_url].site_maps(), base_url)
        self.start_crawling()

    def start_crawling(self):
        headers = {'User-Agent': self.user_agent}
        delay = self.robots[self.start_url].crawl_delay(self.user_agent)
        request_rate = self.robots[self.start_url].request_rate(self.user_agent)
        url = self.urls.pop(0)
        request = requests.get(url, headers=headers)
        self.url_storage.append(url)
        self.parse(request.text, url)
        while self.urls:
            if delay:
                sleep(delay)
            elif request_rate:
                sleep(request_rate.seconds / request_rate.requests)
            else:
                sleep(2)
            url = self.urls.pop(0)
            parsed_url = urlparse(url)
            base_url = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_url)
            if base_url not in self.robots:
                self.get_robot(base_url)
            print(f"parsing {url}, queue size {len(self.urls)}, already parsed {len(self.url_storage)}")
            request = requests.get(url, headers=headers)
            self.url_storage.append(url)
            self.parse(request.text, base_url)

    def parse(self, html_file: str, base_url: str) -> None:
        soup = BeautifulSoup(html_file, 'html.parser')
        self.data.append(soup.get_text())
        self.append_urls([url.get('href') for url in soup.find_all('a')], base_url)

    def append_urls(self, urls: list, base_url: str) -> None:
        url = ""
        for path in [url for url in urls if url]:
            if path.startswith("http"):
                url = path
            else:
                url = base_url + path
            if url not in self.url_storage and url not in self.urls \
                    and self.robots[base_url].can_fetch(self.user_agent, url):
                self.urls.append(url)


if __name__ == '__main__':
    mwc = MyWebCrawler()
    mwc.crawl("https://www.goodreads.com/quotes")
