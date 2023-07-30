# **********************************************************************
# Generic Web Functions
# **********************************************************************

# Python imports
from functools import reduce
from itertools import chain, zip_longest
import re

# HTML Tools imports
import requests
import urllib
import html2text
from bs4 import BeautifulSoup
from urllib.parse import urlparse, quote, unquote
from selenium import webdriver

CHROME_OPTIONS = webdriver.ChromeOptions()


# --------------------------------------------------------------------
# Standard Request Headers
# --------------------------------------------------------------------

REQUEST_HEADERS = {'User-Agent': 'Chrome/83.0.4103.97',
                   'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                   'Accept-Encoding': 'none',
                   'Accept-Language': 'en-US,en;q=0.8',
                   'Connection': 'keep-alive'}

DATA = {'tx_howsite_json_list[page]': '1',
        'tx_howsite_json_list[limit]': '12',
        'tx_howsite_json_list[lang]': '',
        'tx_howsite_json_list[rent]': '',
        'tx_howsite_json_list[area]': '',
        'tx_howsite_json_list[rooms]': 'egal',
        'tx_howsite_json_list[wbs]': 'all-offers'}

REQUEST_HEADERS_2 = {'User-Agent': 'Popular browser\'s user-agent',
                     'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                     'Accept-Encoding': 'none',
                     'Accept-Language': 'en-US,en;q=0.8',
                     'Connection': 'keep-alive'}

# --------------------------------------------------------------------
# URL Response and Data
# --------------------------------------------------------------------

def get_url_response(url, headers=REQUEST_HEADERS_2, params={}):
    'Returns a response object or None'
    
    try:
        return requests.get(url, headers=headers, params=params)
    except Exception:
        return None

# --------------------------------------------------------------------


def get_url_data(url, params={}):
    'Returns the response object content or None'
    
    response = get_url_response(url, params)
    if response is None:
        return None
    else:
        return str(response.content)

def get_chrome_page_source(url, options=CHROME_OPTIONS):
    browser = webdriver.Chrome(options=options)
    browser.get(url)
    return browser.page_source


def get_chrome_content(url):
    page_source = get_chrome_page_source(url)
    return page_source

                       
# --------------------------------------------------------------------

def get_url_content(url):

    req = urllib.request.Request(url)
    req.add_header('User-Agent', 'Chrome/83.0.4103.97')
    header2 = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
    req.add_header('Accept', header2)
    req.add_header('Accept-Language', 'en-US,en;q=0.5')

    content = urllib.request.urlopen(req).read().decode('utf-8')
    
    return content

# --------------------------------------------------------------------

def parse_page_source(url, page_source):
    htmlcontent = BeautifulSoup(page_source, "html.parser")
    text = htmlcontent.get_text()
    title = htmlcontent.find("h1")
    title = title.string.replace('\n', '').strip() if title is not None else ''
    subtitles = htmlcontent.find_all("h2", {"class": re.compile('article')})
    subtitles = [s.string for s in subtitles if s is not None]
    subtitles = [s.replace('\n', '').strip() for s in subtitles]
    passages = [p.get_text for p in htmlcontent.find_all("p", class_=None)]

    return {'url': url, 'text': text, 'title': title, 'subtitles': subtitles,
            'passages': passages}

# --------------------------------------------------------------------

def parse_url_content(url):
    # page_source = get_url_content(url)
    page_source = get_chrome_page_source(url)
    return parse_page_source(url, page_source)


# ----------------------------------------------------------------------
# Convert HTML to Plain Text
# ----------------------------------------------------------------------

def scan_html(html):
    'Converts an html string to regular text with links ignored.'
    
    h2t = html2text.HTML2Text()
    h2t.ignore_links = True
    text = h2t.handle(html)
    return text


#------------------------------------------------------------------------

def get_url_text(url):
    'Returns the text of the page with HTML removed.'
    
    responce = get_url_response(url)
    if response is not None:
        html = response.text
        return scan_html(html)
    else:
        return None


# **********************************************************************
# End of File
# **********************************************************************
