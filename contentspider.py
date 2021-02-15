from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import CrawlerRunner, CrawlerProcess
from twisted.internet import reactor
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings


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
        print('parsed item')


def start_crawling(user_input, merchant_id):
    configure_logging()
    settings = get_project_settings()
    runner = CrawlerRunner(settings)

    runner.crawl(ContentSpider, user_input=user_input, merchant_id=merchant_id)

    reactor.run(installSignalHandlers=False)
