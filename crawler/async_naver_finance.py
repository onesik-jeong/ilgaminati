import asyncio
import json
import re
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

import aiohttp
import pandas as pd
import requests
from bs4 import BeautifulSoup
from db_manager import send_to_ilgaminati
from fake_useragent import UserAgent

BASE_URL = "https://finance.naver.com"
UA = UserAgent()
HEADERS = {
    "User-Agent": UA.random,
}


class Crawler:
    def __init__(self):
        self.trend_stock_df = self.get_search_trend_stock()

    @staticmethod
    def get_search_trend_stock(n: int = 30):
        """
        네이버 검생상위 n개 종목과 그 코드를 df로 출력
        :param n: 가져올 상위 종목의 갯수 (0 < n <= 30)
        :return:
        """
        if (n > 30) or (n < 0):
            raise Exception("n must be between 0 and 30")

        url = BASE_URL + "/sise/lastsearch2.naver"
        r = requests.get(url, headers=HEADERS)
        html = r.text
        soup = BeautifulSoup(html, "html.parser")
        li = soup.find_all("a", "tltle")

        symbols = []
        codes = []
        for stock in li:
            symbol = stock.text
            code = re.search(r"(?<==).*", stock["href"])[0]

            symbols.append(symbol)
            codes.append(code)

        df = pd.DataFrame(zip(symbols, codes), columns=["Symbol", "Code"])
        return df

    @staticmethod
    async def fetch_comments_by_post(session, nid):
        comment_url = (
            "https://apis.naver.com/commentBox/cbox/"
            "web_naver_list_jsonp.json?ticket=finance"
            "&templateId=default&pool=cbox12&lang=ko&"
            f"country=KR&objectId={nid}"
        )
        headers = {
            "User-Agent": UA.random,
            "referer": "https://finance.naver.com/item/board_read.naver?"
            f"code=112040&nid={nid}&st=&sw=&page=1",
        }
        async with session.get(comment_url, headers=headers) as response:
            html = await response.text()
            comments_list = re.findall(
                r'(?<={"commentList":).*(?=,"pageModel")', html
            )[0]
            comments_list = json.loads(comments_list)

            comments = []
            keys = [
                "contents",
                "replyAllCount",
                "userName",
                "modTime",
                "sympathyCount",
                "antipathyCount",
            ]
            for comment in comments_list:
                comments.append({x: comment[x] for x in keys})
            return comments

    async def fetch_by_post(self, session, top_post, symbol):
        """
        post의 내용을 크롤링하는 메소드
        comment의 경우 일시적으로 가져오지 않는다.
        :param session:
        :param top_post:
        :param symbol:
        :return:
        """
        href = top_post.get("href")
        async with session.get(BASE_URL + href, headers=HEADERS) as response:
            html = await response.text()
            content_soup = BeautifulSoup(html, "lxml")

            author = content_soup.select_one(
                "th > span.gray03 > strong"
            ).text.replace(" ", "")
            date = content_soup.select_one("tr > th.gray03.p9.tah").text

            post_info = content_soup.select_one("tr > th:nth-of-type(2)")
            post_info = post_info.getText(",", strip=True).split(",")

            content = content_soup.select_one("#body")
            content = content.getText().replace("\xa0\r", "\n")
            content = content.replace("\r", "\n")
            # nid = int(re.search(r'(?<=nid=)[0-9]+', href)[0])
            # comments = self.fetch_comments_by_post(nid) #비동기로 바꿔줘야함

            post = {}
            post["title"] = top_post.get("title")
            post["author"] = author
            post["content"] = content
            post["stock_name"] = symbol
            post["likes"] = post_info[3]
            post["dislikes"] = post_info[5]
            post["views"] = post_info[1]
            post["reg_ts"] = date.replace(".", "-") + ":00"
            # post['nid'] = nid
            # post['comments'] = comments
            return send_to_ilgaminati(Post=post)

    async def fetch_by_page(
        self, session, symbol: str, code: str, page: int, standard
    ):
        """
        한 게시판 페이지 내의 글들을 크롤링하는 메소드
        :param code: 종목코드
        :param page: 페이지 번호
        :return: 한 페이지의 게시글들을 dict of list 로 반환.
        """
        url = BASE_URL + "/item/board.naver?code=" + code + "&page=%d" % page
        async with session.get(url, headers=HEADERS) as response:
            html = await response.text()
            soup = BeautifulSoup(html, "lxml")

            title_list = soup.select("td.title > a")
            agree_list = soup.select("td:nth-child(5) > strong")
            idx_list = [
                idx
                for idx, agree in enumerate(agree_list)
                if int(agree.text) >= standard
            ]
            top_post_list = [title_list[x] for x in idx_list]

            async with aiohttp.ClientSession() as session:
                await asyncio.gather(
                    *[
                        self.fetch_by_post(session, top_post, symbol)
                        for top_post in top_post_list
                    ]
                )


async def main():
    c = Crawler()
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            *[
                c.fetch_by_page(
                    session,
                    row["Symbol"],
                    row["Code"],
                    page=i + 1,
                    standard=20,
                )
                for _, row in c.trend_stock_df.iterrows()
                for i in range(30)
            ]
        )
