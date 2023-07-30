import re
from bs4 import BeautifulSoup
from selenium import webdriver

options = webdriver.ChromeOptions()

class Edito:
    def __init__(self, url):
        self.url = url
        browser = webdriver.Chrome(options=options)
        browser.get(url)
        self.htmlContent = BeautifulSoup(browser.page_source, "html.parser")
        title = self.htmlContent.find("h1")
        self.title = title.string if title is not None else ''
        subTitles = self.htmlContent.find_all("h2", {"class": re.compile('article')})
        self.subTitles = [s.string for s in subTitles if s is not None]
        articlePassages = self.htmlContent.find_all("p", class_=None)
        self.articlePassages = list(map(lambda aPassage: aPassage.get_text(), articlePassages))

    def printEdito(self):
        print(f'Title: {self.title}\n Subtitles: {self.subTitles}\n\n Passages: {self.articlePassages}')

    def getHtmlStrippedContent(self):
        return self.htmlContent.get_text()

    def getHtmlContentPassages(self):
        return list(filter(None, self.htmlContent.get_text().split('\n')))
