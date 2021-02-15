from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from bs4 import BeautifulSoup
from langdetect import detect
import re
from ..updated_kw import translated_kw_dicts
from scrapy.crawler import CrawlerRunner, CrawlerProcess
from twisted.internet import reactor
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from ..domain_format import parse_url


tc_list = []
privacy_list = []
refund_list = []
delivery_list = []
delivery_list_url = []
phone_number_list = []
emails_list = []
pay_logos_list = []


def tc_find(language, url, soup):
    """
    Loop through tc keywords to find if their present in URL
    or h1, h2, h3, span, li <tags> in URL
    :return: kw found in URL or site text
    """
    if len(tc_list) < 1:
        if url not in tc_list:
            for single_term in translated_kw_dicts['tc_kw'][language]:
                if single_term in url:
                    tc_list.append(url)
                    return single_term
                else:
                    for text in soup.find_all(["h1", "h2", "h3", "li"], text=True):
                        if text.find('a') is None:
                            if single_term in text.string:
                                tc_list.append(url)
                                return single_term


def refund_find(language, url, soup):
    """
    Loop through refund keywords to find if their present in URL
    or h1, h2, h3, span, li <tags> in URL
    :return: kw found in URL or site text
    """
    if len(refund_list) < 1:
        if url not in refund_list:
            for single_term in translated_kw_dicts['refund_kw'][language]:
                if single_term in url:
                    refund_list.append(url)
                    return single_term
                else:
                    for text in soup.find_all(["h1", "h2", "h3", "li"], text=True):
                        if text.find('a') is None:
                            if single_term in text.string:
                                refund_list.append(url)
                                return single_term


def delivery_find(language, url, soup):
    """
    Loop through delivery keywords to find if their present in URL
    or h1, h2, h3, li <tags> in URL.
    :return: kw found in URL or site text
    """
    if len(delivery_list) < 1:  # leave this so that the function isn't executed per each response
        if url not in delivery_list:
            for single_term in translated_kw_dicts['delivery_kw'][language]:
                if single_term in url:
                    delivery_list.append(url)
                    return single_term
                else:
                    for text in soup.find_all(["h1", "h2", "h3", "li"], text=True):
                        if text.find('a') is None:
                            if single_term in text.string:
                                delivery_list.append(url)
                                return single_term


def privacy_find(language, url, soup):
    """
    Loop through privacy keywords to find if their present in URL
    or h1, h2, h3, span, li <tags> in URL
    :return: URL of site with privacy policy
    """
    if len(privacy_list) < 1:
        if url not in privacy_list:
            for single_term in translated_kw_dicts['privacy_kw'][language]:
                if single_term in url:
                    privacy_list.append(url)
                    return single_term
                else:  # TODO remove else and create a new loop so that first loop checks all kw against url (prioritisation of url)
                    for text in soup.find_all(["h1", "h2", "h3", "li"], text=True):
                        if text.find('a') is None:
                            if single_term in text.string.lower():
                                privacy_list.append(url)
                                return url


def phone_find(soup_text_list):
    """
    changed from soup_text to soup_text_list to avoid text  being merged
    from tables and different paragraphs. Then loop through each separate
    text string to find phone number
    """
    if len(phone_number_list) < 1:
        for single_text in soup_text_list:
            number = re.search('[\d+(][\d\s)-][\d\s)-][\d\s)-][\d\s)-][\d\s)-][\d\s)-][\d\s-][\d\s-]?[\d\s-]?[\d\s-]?[\d\s-]?[\d\s-][\d][\d]', single_text, re.DOTALL)
            if number:
                if re.search(r'[\s+-]', number.group()):
                    search_result = re.sub('[a-zA-Z\s,.]', '', number.group())
                    if len(search_result) > 7 and search_result not in phone_number_list:
                        phone_number_list.append(search_result)
                        return search_result


def email_find(soup_text_list):  # TODO remove script tags
    if len(emails_list) < 1:
        for single_text in soup_text_list:
            emails = re.findall(r'[\w.-]+@[\w.-]+\.\w+', single_text)
            for single_email in emails:
                if single_email in emails_list:
                    pass
                else:
                    emails_list.append(single_email)
                    return single_email


def logo_find(language, soup, url):  # TODO ielikt, ka meklē tikai attēlus no footer?
    """
    Check if footer section contains Visa/ MC logo. If yes, then append
    pay_logos_list. Intended to be used max 5 times
    :params language, soup:
    """
    if len(pay_logos_list) < 1:
        image_tags = soup.find_all(['img', 'svg'])
        for single_tag in image_tags:
            for kw in translated_kw_dicts['pay_logos_kw'][language]:
                if kw in f"{single_tag}".lower():
                    pay_logos_list.append(single_tag)
                    print(single_tag)
                    return single_tag, url


class ContentSpider(CrawlSpider):

    def __init__(self, user_input=None, merchant_id=None, *args, **kwargs):
        super(ContentSpider, self).__init__(*args, **kwargs)
        self.website = 'http://' + parse_url(user_input)
        self.start_urls = [self.website]
        self.allowed_domains = [parse_url(user_input)]  # TODO formatēt bez www
        self.merchant_id = merchant_id

    name = 'content_spider'
    custom_settings = {
        'ROBOTSTXT_OBEY': True,
        'CLOSESPIDER_PAGECOUNT': 200
    }

    rules = (
        Rule(LinkExtractor(), callback='parse_item', follow=True),
    )

    def parse_item(self, response):
        url = response.url
        soup = BeautifulSoup(response.text, 'lxml')
        for s in soup(['script', 'style', 'meta']):  # removing script, style and meta tags from soup
            s.decompose()
        soup_text = soup.get_text(strip=True)
        soup_text_list = soup.find_all(text=True)
        language = detect(soup_text)  # TODO maybe detect language for soup_text_list?

        for single_word in translated_kw_dicts['restricted_kw'][language]:
            if single_word in soup_text_list:
                yield {
                    "words": (response.url, single_word)
                }

        for link in soup.find_all('a', href=re.compile('http')):
            yield {
                "href": link.get('href')
            }

        tc = tc_find(language, url, soup)
        if tc:
            yield {
                "tc": response.url
            }

        privacy = privacy_find(language, url, soup)
        if privacy:
            yield {
                "privacy": response.url
            }

        refund = refund_find(language, url, soup)
        if refund:
            yield {
                "refund": response.url
            }

        delivery = delivery_find(language, url, soup)
        if delivery:
            yield {
                "delivery": response.url
            }

        phone = phone_find(soup_text_list)
        if phone:
            yield {
                "phone": phone
            }

        email = email_find(soup_text_list)
        if email:
            yield {
                "email": email
            }

        logo = logo_find(language, soup, response.url)
        if logo:
            yield {
                "logos": response.url
            }


def start_crawling(user_input, merchant_id):
    configure_logging()
    settings = get_project_settings()
    runner = CrawlerRunner(settings)

    runner.crawl(ContentSpider, user_input=user_input, merchant_id=merchant_id)

    reactor.run(installSignalHandlers=False)
