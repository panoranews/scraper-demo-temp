from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict
import requests
from bs4 import BeautifulSoup
import json


@dataclass
class SiteConfig:
    base_url: str
    extension: str
    link_selector: str
    title_selector: str
    body_selector: str


@dataclass
class PageTask:
    site: SiteConfig
    url: str
    html: bytes = b""


def fetch_html(url: str) -> bytes:
    print(f"Fetching: {url}")
    res = requests.get(url, timeout=15)
    res.raise_for_status()
    print(f"Finished fetching: {url}")
    return res.content


def extract_links(html: bytes, link_selector: str, base_url: str) -> List[str]:
    print("Extracting links...")
    soup = BeautifulSoup(html, "html.parser")
    tags = soup.select(link_selector)
    return [base_url + str(tag.get("href")) for tag in tags]


def parse_post_html(
    html: bytes,
    title_selector: str,
    body_selector: str,
    url: str
) -> Dict[str, str]:
    print("Parsing post...")
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.select_one(title_selector)
    if not title_tag:
        raise ValueError("Failed to extract title - CSS selector likely wrong")

    body_parts = [p.get_text(strip=True) for p in soup.select(body_selector)]

    return {
        "title": title_tag.get_text(strip=True),
        "body": "\n\n".join(body_parts),
        "url": url
    }


def main():
    SITE_COUNT = 1

    site_list = [
        SiteConfig(
            base_url="https://formulanews.ge",
            extension="/Category/all",
            link_selector="div.main__new__slider__desc > a",
            title_selector="h1.news__inner__desc__title",
            body_selector="section.article-content > p"
        )
        for _ in range(SITE_COUNT)
    ]

    list_page_tasks: List[PageTask] = [
        PageTask(site=s, url=s.base_url + s.extension)
        for s in site_list
    ]

    del site_list

    with ThreadPoolExecutor(max_workers=50) as pool:
        future_to_task = {pool.submit(fetch_html, t.url): t for t in list_page_tasks}
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            task.html = future.result()

    post_tasks: List[PageTask] = []

    for task in list_page_tasks:
        links = extract_links(task.html, task.site.link_selector, task.site.base_url)
        for link in links:
            post_tasks.append(PageTask(task.site, link))

    del list_page_tasks

    with ThreadPoolExecutor(max_workers=50) as pool:
        future_to_task = {pool.submit(fetch_html, t.url): t for t in post_tasks}
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            task.html = future.result()

    parsed_posts: List[Dict] = []

    for task in post_tasks:
        post = parse_post_html(
            task.html,
            task.site.title_selector,
            task.site.body_selector,
            task.url
        )
        parsed_posts.append(post)

    del post_tasks

    with open("output/result.json", "w", encoding="utf-8") as f:
        json.dump(parsed_posts, f, indent=4, ensure_ascii=False)

    print("DONE.")


if __name__ == "__main__":
    main()
