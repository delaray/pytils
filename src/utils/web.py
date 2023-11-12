# **********************************************************************
# Generic Web Functions
# **********************************************************************

# Part 1: Requests
# Part 2: Scraping

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

from utils.nlp import tokenize_text


# --------------------------------------------------------------------

CHROME_OPTIONS = None

# --------------------------------------------------------------------
# Standard Request Headers
# --------------------------------------------------------------------

REQUEST_HEADERS = {'User-Agent': 'Chrome/83.0.4103.97',
                   'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                   'Accept-Encoding': 'none',
                   'Accept-Language': 'en-US,en;q=0.8',
                   'Connection': 'keep-alive'}


# --------------------------------------------------------------------
# URL Response and Data
# --------------------------------------------------------------------

def get_url_response(url, headers=REQUEST_HEADERS, params={}):
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

    
def initialize_chrome_options():
    global CHROME_OPTIONS
    CHROME_OPTIONS = webdriver.ChromeOptions()
    return CHROME_OPTIONS


def get_chrome_page_source(url, options=CHROME_OPTIONS):
    if options is None:
        options = initialize_chrome_options()
    browser = webdriver.Chrome(options=options)
    browser.get(url)
    return browser.page_source


def get_chrome_content(url):
    page_source = get_chrome_page_source(url)
    return page_source

                       
# --------------------------------------------------------------------

H1 =  'text/html,application/xhtml+xml,application/xml;'
H2 =  'q=0.9,image/avif,image/webp,*/*;q=0.8'
HEADER2 = H1 + H2

def get_url_content(url):

    req = urllib.request.Request(url)
    req.add_header('User-Agent', 'Chrome/83.0.4103.97')
    req.add_header('Accept', H2)
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
    subtitle = [s.string for s in subtitles if s is not None]
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


# ******************************************************************
# Part 2: Generic Web Scraping Tools
# ******************************************************************

def contains_stop_word(link, stop_words):
    result = False
    for x in stop_words:
        if x in link['href']:
            result = True
    return result


# -------------------------------------------------------------------
# URL Predicates
# -------------------------------------------------------------------

def full_url_p(url):
    return ('http://' in url) or ('https://' in url)


def make_full_url(url, full_url):
    urlcomps = urlparse(full_url)
    return urlcomps.scheme + '://' + urlcomps.netloc + url


def same_domain_p(url1, url2):
    comps1 = urlparse(url1)
    comps2 = urlparse(url2)
    return comps1.netloc == comps2.netloc


def make_domain_url(url):
    comps = urlparse(url)
    return comps.scheme + '://' + comps.netloc + '/'


def internal_link_p(url, site_url):
    return same_domain_p(url, site_url)

# -------------------------------------------------------------------
# URL Extraction.
# -------------------------------------------------------------------

def pure_url(url):
    'Removes the parameters of url'
    return url[:url.index("?")] if "?" in url else url

# -------------------------------------------------------------------

def extract_title(url, response=None):
    response = get_url_response(url) if response is None else response
    if response is not None:
        soup = BeautifulSoup(response.content, 'lxml')
        title = soup.find("h1")
        title = title.string.replace('\n', '').strip() if title is not None else ''
        return title
    else:
        return None
    
# -------------------------------------------------------------------

def extract_urls (url, filter='', stop_words=[]):
    response = get_url_response(url)
    urls = []
    if response is not None:
        soup = BeautifulSoup(response.content, 'lxml')
        for link in soup.find_all('a', href=True):
            if filter in link['href'] and not contains_stop_word (link, stop_words):
                # print(unquote(link['href']))
                urls.append((unquote(link['href'])))
        
    urls = [url for url in urls if url.startswith('http') is True]
    
    return urls


# -------------------------------------------------------------------

def extract_url_entries (url, filter='', stop_words=[]):
    response = get_url_response(url)
    urls = []
    if response is not None:
        soup = BeautifulSoup(response.content, 'lxml')
        for link in soup.find_all('a', href=True):
            if filter in link['href'] and not contains_stop_word (link, stop_words):
                urls.append([(unquote(link['href'])), link.get_text()])
        
    urls = [url for url in urls if url[0].startswith('http') is True]
    
    return urls

# -------------------------------------------------------------------

def extract_full_urls(url, root_url, filter='', stop_words=[]):
    urls = extract_urls(url, filter, stop_words)
    full_urls = []
    for u in urls:
        if not full_url_p(u):
            full_url = make_full_url(u, root_url)
        else:
            full_url = u
        full_urls.append(full_url)
    return full_urls
 
# -------------------------------------------------------------------

def extract_internal_urls(url, root_url, filter='', stop_words=[]):
    urls = extract_full_urls(url, root_url, filter, stop_words)
    result = []
    for u in urls:
        if internal_link_p(u, root_url) == True:
            result.append(u)
    return(result)

# -------------------------------------------------------------------

# Defined for convenience. Tokenizes then the reasembles.

def clean_sentence(s):
    tokens = tokenize_text(s)
    return ' '.join(tokens)

# -------------------------------------------------------------------

def extract_text(url):
    content = get_url_data(url)
    text = []
    if content is not None:
        soup = BeautifulSoup(content, 'lxml')
        for x in soup.find_all('p'):
            text.append(x.get_text())
        text = [unquote(x)  for x in text if len(x) > 2]
    return text

# -------------------------------------------------------------------

CHROME_OPTIONS
def extract_clean_text (url):
    text = extract_text(url)
    clean = [clean_sentence(x) for x in text]
    filtered = [x for x in clean if len(x.split(' ')) > 1]
    return filtered

# **********************************************************************
# End of File
# **********************************************************************
