import asyncio
from crawler.naver_finance_async import nf_main
from crawler.dcinside_async_v1 import dc_main


if __name__ == "__main__":
    asyncio.run(nf_main())
    asyncio.run(dc_main())
