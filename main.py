# import asyncio
# from crawler.async_naver_finance import Crawler
# from crawler.async_naver_finance import main

# if __name__ == '__main__':
#     asyncio.run(main())


import asyncio
from crawler.async_dcinside import Crawler
from crawler.async_dcinside import main

if __name__ == '__main__':
    asyncio.run(main())