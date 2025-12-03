import json
import asyncio
from dataclasses import dataclass
from typing import List, Dict

import aiohttp
from bs4 import BeautifulSoup


@dataclass
class SiteConfig:
    """Mock Site class mimicking database Object"""
    base_url: str
    extension: str
    link_selector: str
    title_selector: str
    body_selector: str


@dataclass
class PageTask:
    """Task class for interconnecting scraping pipeline components"""
    site: SiteConfig
    url: str
    html: bytes = b""

    async def assign_html(self, session: aiohttp.ClientSession):
        self.html = await fetch_html(session, self.url)


async def fetch_html(session: aiohttp.ClientSession, url: str) -> bytes:
    """Function which fetches html from web"""
    print(f"Fetching: {url}")
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.content.read()


async def assign_html_to_tasks(tasks: List[PageTask]):
    """Function which dynamicaly assigns html to tasks"""
    async with aiohttp.ClientSession() as session:
        coros = [task.assign_html(session) for task in tasks]
        await asyncio.gather(*coros)


def extract_links(html: bytes, link_selector: str, base_url: str) -> List[str]:
    """Function which extracts links from html"""
    print("Extracting links...")
    soup = BeautifulSoup(html, "html.parser")
    tags = soup.select(link_selector)
    return [base_url + str(tag.get("href")) for tag in tags]


def generate_post_tasks(tasks: List[PageTask]):
    """Function which generates child tasks based on links"""
    post_tasks: List[PageTask] = []
    for task in tasks:
        links = extract_links(task.html, task.site.link_selector, task.site.base_url)
        for link in links:
            post_tasks.append(PageTask(task.site, link))
    return post_tasks


def parse_post_html(
    html: bytes,
    title_selector: str,
    body_selector: str,
    url: str
) -> Dict[str, str]:
    """Function for parsing a post"""
    print("Parsing post...")
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.select_one(title_selector)
    if not title_tag:
        raise ValueError("Failed to extract title - CSS selector likely wrong")
        # Probably send email

    body_parts = [p.get_text(strip=True) for p in soup.select(body_selector)]

    return {
        "title": title_tag.get_text(strip=True),
        "body": "\n\n".join(body_parts),
        "url": url
    }


def parse_all_posts(tasks: List[PageTask]):
    """Function for parsing multiple posts based on task list"""
    parsed_posts: List[Dict] = []
    for task in tasks:
        post = parse_post_html(
            task.html,
            task.site.title_selector,
            task.site.body_selector,
            task.url
        )
        parsed_posts.append(post)
    return parsed_posts


def list_all_sites(site_count: int):
    """Mock function mimicking database call"""
    return [
        SiteConfig(
            base_url="https://formulanews.ge",
            extension="/Category/all",
            link_selector="div.main__new__slider__desc > a",
            title_selector="h1.news__inner__desc__title",
            body_selector="section.article-content > p"
        )
        for _ in range(site_count)
    ]


async def main():
    SITE_COUNT = 1

    site_list = list_all_sites(SITE_COUNT)

    list_page_tasks: List[PageTask] = [
        PageTask(site=site, url=site.base_url + site.extension)
        for site in site_list
    ]

    del site_list

    await assign_html_to_tasks(list_page_tasks)

    post_tasks = generate_post_tasks(list_page_tasks)

    del list_page_tasks

    await assign_html_to_tasks(post_tasks)

    parsed_posts = parse_all_posts(post_tasks)

    del post_tasks

    with open("output/result.json", "w", encoding="utf-8") as f:
        json.dump(parsed_posts, f, indent=4, ensure_ascii=False)

    print("DONE.")


if __name__ == "__main__":
    asyncio.run(main())
