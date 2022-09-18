import asyncio
import aiohttp
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

UA = UserAgent()
HEADERS = {
    "User-Agent": UA.random,
}

BASE_URL = "https://gall.dcinside.com"
GALLERIES = [
    "stockus",
    "neostock",
    "tenbagger",
    "chstock",
    "immovables",
    "kospi",
    "dow100",
    "jaetae",
    "of",
    "bitcoins_new1",
]


class DCInsideCrawler:
    def __init__(self, standard: int):
        print("Init Crawler..!")
        self.session = aiohttp.ClientSession(headers=HEADERS)
        self.standard = standard

    async def close(self):
        await self.session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *err):
        await self.close()

    @staticmethod
    def announcement_checker(posts: list):
        print("Removing Announcement..!")
        posts = [
            post
            for post in posts
            if post.select("td.gall_subject")[0].text != "공지"
        ]
        return posts

    def top_post_checker(self, posts: list):
        print("Checking Top Posts..")
        posts_href = [
            post.select("a")[0].get("href")
            for post in posts
            if int(post.select("td.gall_recommend")[0].text) > self.standard
        ]
        return posts_href

    async def fetch_posts(self, posts_href):
        if posts_href == None:
            yield None
        url = BASE_URL + posts_href
        async with self.session.get(url) as res:
            html = await res.text()
            soup = BeautifulSoup(html, "lxml")
        try:
            content = soup.select_one("div.write_div").text
        except AttributeError:
            yield print("AttributeError")
        content = content.replace("\n", "").replace("  - dc official App", "")
        post = {}
        post["title"] = soup.select_one("span.title_subject").text
        post["author"] = soup.select_one("span.nickname").get("title")
        post["reg_ts"] = soup.select_one("span.gall_date").get("title")
        post["content"] = content
        post["agree"] = soup.select_one("div.up_num_box > p").text
        post["disagree"] = soup.select_one("div.down_num_box > p").text
        print(post)
        yield (post)

    async def fetch_top_posts_href(self, gallery, page):
        print(f"Now Crawling {gallery} gallery's page #{page}")
        url = (
            BASE_URL + "/mgallery/board/lists/?id=" + gallery + f"&page={page}"
        )
        async with self.session.get(url, headers=HEADERS) as res:
            html = await res.text()
            soup = BeautifulSoup(html, "lxml")
        post_list = soup.select("tr.ub-content.us-post")

        if page == 1:
            post_list = self.announcement_checker(post_list)
        posts_href = self.top_post_checker(post_list)
        if len(posts_href) == 0:
            return print("No top post in this page")
        print(f"Fetching top posts href in this page!")
        for href in posts_href:
            await self.fetch_posts(href)

    async def main(self):
        coros = [
            self.fetch_top_posts_href(gallery, page=i + 1)
            for gallery in GALLERIES
            for i in range(30)
        ]
        await asyncio.gather(*coros)
