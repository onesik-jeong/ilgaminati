import re
import time
import json

import pandas as pd
import requests
import multiprocessing
from bs4 import BeautifulSoup
from multiprocessing import Pool
from fake_useragent import UserAgent


BASE_URL = 'https://finance.naver.com'
UA = UserAgent()
HEADERS = {
    'User-Agent': UA.random,
}


class Crawler:
    def __init__(self, n_process):
        self.n_process = n_process
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
        soup = BeautifulSoup(html, 'html.parser')
        li = soup.find_all('a', 'tltle')

        symbols = []
        codes = []
        for stock in li:
            symbol = stock.text
            code = re.search(r'(?<==).*', stock['href'])[0]

            symbols.append(symbol)
            codes.append(code)

        df = pd.DataFrame(zip(symbols, codes), columns=["Symbol", "Code"])
        return df

    def fetch_by_page(self, code: str, page: int):
        """
        한 게시판 페이지 내의 글들을 크롤링하는 메소드
        :param code: 종목코드
        :param page: 페이지 번호
        :return: 한 페이지의 게시글들을 dict of list 로 반환.
        """

        url = BASE_URL + '/item/board.naver?code=' + code + '&page=%d' % page
        req = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(req.text, 'lxml')

        title_list = soup.select('td.title > a')
        agree_list = soup.select('td:nth-child(5) > strong')
        idx_list = [idx for idx, agree in enumerate(agree_list) if int(agree.text) >= 10]
        top_post_list = [title_list[x] for x in idx_list]

        def fetch_comments_by_post(nid):
            comment_url = "https://apis.naver.com/commentBox/cbox/" \
                          "web_naver_list_jsonp.json?ticket=finance" \
                          "&templateId=default&pool=cbox12&lang=ko&"\
                          f"country=KR&objectId={nid}"
            headers = {
                'User-Agent': UA.random,
                'referer': 'https://finance.naver.com/item/board_read.naver?'\
                           f'code=112040&nid={nid}&st=&sw=&page=1'
            }
            r = requests.get(comment_url, headers=headers)
            comments_list = re.findall(r'(?<={"commentList":).*(?=,"pageModel")', r.text)[0]
            comments_list = json.loads(comments_list)

            comments = []
            keys = ['contents', 'replyAllCount', 'userName', 'modTime', 'sympathyCount', 'antipathyCount']
            for comment in comments_list:
                comments.append({x: comment[x] for x in keys})
            return comments

        def fetch_by_post(top_post):
            href = top_post.get('href')
            r = requests.get(BASE_URL + href)
            content_soup = BeautifulSoup(r.text, 'lxml')

            date = content_soup.select_one('tr > th.gray03.p9.tah').text

            post_info = content_soup.select_one('tr > th:nth-of-type(2)')
            post_info = post_info.getText(',', strip=True).split(',')

            content = content_soup.select_one('#body')
            content = content.getText().replace(u'\xa0\r', '\n')
            content = content.replace('\r', '\n')
            nid = int(re.search(r'(?<=nid=)[0-9]+', href)[0])
            comments = fetch_comments_by_post(nid)

            posts = {}
            posts['title'] = top_post.get('title')
            posts['nid'] = nid
            posts['date'] = date
            posts['view'] = post_info[1]
            posts['agree'] = post_info[3]
            posts['disagree'] = post_info[5]
            posts['opinion'] = post_info[7]
            posts['content'] = content
            posts['comments'] = comments
            return posts

        pool = multiprocessing.pool.ThreadPool(10)
        posts = [pool.apply_async(fetch_by_post, args={top_post: top_post})
                 for top_post in top_post_list]
        pool.close()
        pool.join()
        posts = [post.get() for post in posts]

        # list of dict -> dict of list
        posts = {k: [dic[k] for dic in posts] for k in posts[0]}
        return posts

    def fetch_by_code(self, code, datum_point=10):
        """
        Multiprocessing을 사용하여 한 종목 토론실 글을 모두 크롤링하는 메소드
        :param code: 종목코드
        :param datum_point: 베스트 글 선정 기준
        :return: DB 저장 형식의 pd.DataFrame()
        """
        req = requests.get(BASE_URL + '/item/board.nhn?code=' + code)
        page_soup = BeautifulSoup(req.text, 'lxml')
        total_page_num = page_soup.select_one('tr > td.pgRR > a')
        if total_page_num is not None:
            total_page_num = total_page_num.get('href').split('=')[-1]
            total_page_num = int(total_page_num)
        else:
            total_page_num = 1

        print('total_pages={}'.format(total_page_num), end=' ', flush=True)
        pool = Pool(self.n_process)
        m = multiprocessing.Manager()
        event = m.Event()

        posts_list = [pool.apply_async(self.fetch_by_page, args=(code, i, event))
                      for i in range(1, total_page_num + 1)]
        pool.close()
        pool.join()
        posts_list = [res.get() for res in posts_list]

        df = pd.concat(list(map(pd.DataFrame, posts_list)))
        df.date = pd.to_datetime(df.date)
        df.sort_values(by='nid', inplace=True)
        df.set_index('nid', inplace=True)
        df['opinion'].replace('의견없음', 0, inplace=True)

        print('\r' + code + ': Done.', end=' ')
        return df

    def fetch_all(self):
        """
        모든 종목 토론실 게시글을 크롤링하는 메소드.
        :output: data/*.db
        """
        for i, code in enumerate(sorted(self.stock_df['종목코드'])):
            print(code, end=': ')
            if self.is_up_to_date(code):
                print('\r' + code + ': Already up-to-date')
                continue
            try:
                t = time.time()
                df = self.fetch_by_code(code)
                print('({:.2f}sec)                 '.format(time.time() - t))
                self.db.write(code, df)
                del df
            except:
                print('Failed:{}'.format(code))
                continue
