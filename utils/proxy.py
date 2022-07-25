from http_request_randomizer.requests.proxy.requestProxy import RequestProxy
from fake_headers import Headers
import requests

class RANDOM_PROXY:
    def __init__(self):
        self.proxies = 


    def proxy_create(self):
        """
        무작위로 프록시를 생성해서 가져오는 코드
        """
        self.req_proxy = RequestProxy()
        proxy = self.test_proxy() # 잘 작동되는 프록시 선별
        return proxy

    def test_proxy(self):
    	"""
        가져온 프록시중에서 실제로 작동되는 프록시만 하나씩 가져오는 코드
        test_url : 자신의 IP를 확인하는 코드. 여기서 변경된 IP가 나오면 성공적으로 우회가된것
        """
        
        test_url = 'http://ipv4.icanhazip.com' 
        while True: # 제대로된 프록시가 나올때까지 무한반복 
            requests = self.req_proxy.generate_proxied_request(test_url)

            if requests is not None:
                print("\t Response: ip={0}".format(u''.join(requests.text).encode('utf-8')))
                proxy = self.req_proxy.current_proxy
                break

            else:
                continue
                
        return proxy # 잘작동된 proxy를 뽑아준다. 
        
    def crawling(self):
       header = Headers(
                    browser="chrome",  # Generate only Chrome UA
                    os="win",  # Generate ony Windows platform
                    headers=True  # generate misc headers
                )
       self.headers = header.generate() # 랜덤 유저 에이전트를 생성해주는 함수.
       _url = 원하는 주소 
       
       self.proxies = {} # request.get 인자에 넣어줄 딕셔너리 생성 
       self.proxies['http'] = 'http://%s' % self.proxy
       
       html = self.session.get(_url, headers=self.headers,proxies=self.proxies).content
       # get 인자에 프록시와 헤더를 넣어주면 끝.