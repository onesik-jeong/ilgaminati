from collections import namedtuple

import requests

Post = namedtuple("Post", "title, content, stock_name, author, reg_ts, views, likes, dislikes")

def send_to_gamigool(post: Post):
    return requests.post("https://hxx059yi92.execute-api.ap-northeast-2.amazonaws.com/api/crawling/posts", json={
        "title": post.title,
        "author": post.author,
        "content": post.content,
        "stock_name": post.stock_name,
        "likes": int(post.likes),
        "dislikes": int(post.dislikes),
        "views": int(post.views),
        "reg_ts": post.reg_ts
    })
