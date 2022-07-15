import re
import time

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


# TODO
# 의논 할 것
# - 날짜를 보는 것보다 페이지 갯수로 제한하는 것이 좋을까?
#   (사람의 행동이 날짜보다 페이지 넘기는 것에 제한되니까)
# - AWS람다에서 멀티스레드 멀티프로세싱 적용 어떻게하지?


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

        def fetch_by_post(title_atag):
            r = requests.get(BASE_URL + title_atag.get('href'))
            content_soup = BeautifulSoup(r.text, 'lxml')

            date = content_soup.select_one('tr > th.gray03.p9.tah').text

            post_info = content_soup.select_one('tr > th:nth-of-type(2)')
            post_info = post_info.getText(',', strip=True).split(',')

            content = content_soup.select_one('#body')
            content = content.getText().replace(u'\xa0\r', '\n')
            content = content.replace('\r', '\n')

            href = title_atag.get('href')

            posts = {}
            posts['title'] = title_atag.get('title')
            posts['nid'] = int(re.search('(?<=nid=)[0-9]+', href)[0])
            posts['date'] = date
            posts['view'] = post_info[1]
            posts['agree'] = post_info[3]
            posts['disagree'] = post_info[5]
            posts['opinion'] = post_info[7]
            posts['content'] = content
            return posts

        pool = multiprocessing.pool.ThreadPool(10)
        posts = [pool.apply_async(fetch_by_post, args={title_atag: title_atag})
                 for title_atag in title_atags]
        pool.close()
        pool.join()
        posts = [post.get() for post in posts]

        # list of dict -> dict of list
        posts = {k: [dic[k] for dic in posts] for k in posts[0]}

        db_latest_nid = self.db.latest_nid.get(code, 0)
        # 최신글 부터 DB에 저장된 날짜까지 다 크롤링 한 경우, 중단!
        # 단, 아래 코드가 정상적으로 작동하려면
        # min(all fetched posts' nid) <= min(this page's posts' nid) 이어야 함.
        if min(posts['nid']) < db_latest_nid:
            event.set()

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

    def fetch_daily_top_posts(self, code, datum_point=10, total_page=30):
        """

        :param code:
        :param datum_point:
        :param total_page:
        :return:
        """
        for i in range(1, total_page + 1):
            req = requests.get(BASE_URL + '/item/board.naver?code=' + code)
        soup = BeautifulSoup(req.text, 'html.parser')

        pool = multiprocessing.Pool(self.n_process)
        m = multiprocessing.Manager()
        event = m.Event()


    def is_up_to_date(self, code):
        """
        종목 토톤실의 가장 최근 글의 날짜와 DB에 저장된 가장 최근 글의 날짜를 비교하여,
        DB가 최신인지 아닌지 여부를 반환함.
        (nid를 비교하는 것이 더욱 정확하지만 date로 비교해도 문제는 없다.)
        주의!: 최신글이 답변글인 경우, 글이 게시판 최상단에 위치하지 않아 True가 반환된다.
        """
        req = requests.get(BASE_URL + '/item/board.nhn?code=' + code)
        page_soup = BeautifulSoup(req.text, 'lxml')
        web_latest_date = page_soup.select_one('tbody > tr:nth-of-type(3) > td:nth-of-type(1) > span')
        web_latest_date = pd.to_datetime(web_latest_date.text)

        db_latest_date = self.db.latest_date.get(code, 0)
        if db_latest_date == 0:
            return False
        elif db_latest_date < web_latest_date:
            return False
        else:
            return True

    def fetch_one(self, code):
        print(code, end=' ')
        if self.is_up_to_date(code):
            print('\r'+code+': Already up-to-date')
        else:
            t = time.time()
            df = self.fetch_by_code(code)
            print('({:.2f}sec)                 '.format(time.time() - t))
            self.db.write(code, df)
            del df

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
