import multiprocessing
from http.client import RemoteDisconnected
from time import sleep
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
import validators
from bs4 import BeautifulSoup


def get_domain(start_url):
    parsed_url = urlparse(start_url)
    base_url = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_url)
    return base_url


def get_robot(start_url: str) -> RobotFileParser:
    robot = RobotFileParser()
    robot.set_url(start_url + "/robots.txt")
    robot.read()
    return robot


def start_crawling(pid, q, start_url, agent, robot, base_url_store):
    data = []
    urls = []
    url_storage = set([])
    urls.append(start_url)

    headers = {'User-Agent': agent}
    delay = robot.crawl_delay(agent)
    request_rate = robot.request_rate(agent)
    while urls:
        if delay:
            sleep(delay)
        elif request_rate:
            sleep(request_rate.seconds / request_rate.requests)
        else:
            sleep(2)
        current_url = urls.pop(0)
        print(f"process\t{pid} parsing {current_url}\t queue size {len(urls)}\t already parsed {len(url_storage)}")
        request = requests.get(current_url, headers=headers)
        url_storage.add(current_url)
        parsed_data, new_urls = parse(request.text)
        data = data + parsed_data
        urls_to_append = [url for url in append_urls(q, new_urls, start_url, url_storage, robot, base_url_store) if
                          url not in urls]
        urls = urls + urls_to_append


def parse(html_file: str) -> (list, list):
    data = []
    soup = BeautifulSoup(html_file, 'html.parser')
    data.append(soup.get_text())
    return data, [url.get('href') for url in soup.find_all('a')]


def append_urls(q, new_urls: list, base_url: str, url_storage: set, robot: RobotFileParser, base_url_store) -> set:
    urls_to_crawl = set([])
    for path in [url for url in new_urls if url]:
        if path.startswith("http"):
            url = path
            url_root = get_domain(url)
            if url_root not in base_url_store:
                q.put(url_root)
                # print(f"found new domain {url_root}")
        else:
            url = base_url + path
            if url not in url_storage and robot.can_fetch("MyWebCrawlingBot2407", url):
                urls_to_crawl.add(url)
    return urls_to_crawl


if __name__ == '__main__':
    user_agent = "MyWebCrawlingBot2407"
    multiprocessing.set_start_method('spawn', True)
    manager = multiprocessing.Manager()
    base_url_store = manager.list()
    domain_queue = manager.Queue()
    robots = manager.dict()
    start_url = "http://quotes.toscrape.com/"
    domain_queue.put(start_url)
    pool = multiprocessing.Pool(processes=4)
    pid = 0
    while True:
        if not domain_queue.empty():
            url = domain_queue.get()
            base_url = get_domain(url)
            if validators.url(base_url):
                base_url_store.append(base_url)
                try:
                    robot = get_robot(base_url)
                except requests.exceptions.SSLError:
                    continue
                except URLError:
                    continue
                except RemoteDisconnected:
                    continue
                pool.apply_async(start_crawling, (pid, domain_queue, base_url, user_agent, robot, base_url_store))
                pid += 1
