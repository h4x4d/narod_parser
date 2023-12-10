import fake_useragent

from constants import BINARY_FILES

headers_ = {
    'Accept-Language': 'ru,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
}
ua = fake_useragent.UserAgent()


def get_headers():
    headers = headers_.copy()
    headers['User-Agent'] = ua.random
    return headers


def check_binary(url: str):
    return any(url.endswith(i) for i in BINARY_FILES)