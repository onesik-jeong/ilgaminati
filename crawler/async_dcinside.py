import asyncio
import aiohttp
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

UA = UserAgent()
HEADERS = {
    'User-Agent': UA.random,
}

BASE_URL = 'https://gall.dcinside.com'
GALLERIES = ['stockus', 'neostock', 'tenbagger', 'chstock',
             'immovables', 'kospi', 'dow100', 'jaetae', 'of', 'bitcoins_new1']


class Crawler:
    def __init__(self, standard: int):
        self.standard = standard
        return None

    @staticmethod
    def announcement_checker(posts: list):
        posts = [post for post in posts 
                 if post.select('td.gall_subject')[0].text != '공지']
        return posts

    def top_post_checker(self, posts: list):
        posts_href = [post.select('a')[0].get('href') for post in posts
                 if int(post.select('td.gall_recommend')[0].text) \
                 > self.standard]
        return posts_href

    async def fetch_posts(self, session, post_href: list):
        url = BASE_URL + post_href
        async with session.get(url, headers=HEADERS) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'lxml')

            content = soup.select_one('div.write_div').text
            content = content.replace('\n', '') \
                             .replace('  - dc official App', '')

            post = {}
            post['title'] = soup.select_one('span.title_subject').text
            post['author'] = soup.select_one('span.nickname').get('title')
            post['reg_ts'] = soup.select_one('span.gall_date').get('title')
            post['content'] = content
            post['agree'] = soup.select_one('div.up_num_box > p').text
            post['disagree'] = soup.select_one('div.down_num_box > p').text
            print(post)

    async def fetch_page(self, session, gallery, page):
        url = BASE_URL + '/mgallery/board/lists/?id=' + \
              gallery + f'&page={page}'
        async with session.get(url, headers=HEADERS) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'lxml')
            post_list = soup.select('tr.ub-content.us-post')
            
            if page == 1:
                post_list = self.announcement_checker(post_list)
            posts_href = self.top_post_checker(post_list)

            async with aiohttp.ClientSession() as session:
                await asyncio.gather(
                    *[self.fetch_posts(session, href) for href in posts_href]
                )    

async def main():
    standard = 10
    c = Crawler(standard)
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            *[c.fetch_page(session, gallery, page=i+1)
              for gallery in GALLERIES
              for i in range(30) ]
        )
