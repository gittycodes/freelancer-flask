# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.utils.log import configure_logging
import json
import logging
from app import Requisites, Merchant, OutboundLinks, Words, db
from helper_functions import column_count
from urllib.parse import urlparse
from dateutil.relativedelta import relativedelta
from datetime import date, datetime


class NewPipeline(object):

    def __init__(self, website, merchant_id):
        self.website = website
        self.merchant_id = merchant_id

        self.outgoing_links = set()

    @classmethod
    def from_crawler(cls, crawler):
        website = getattr(crawler.spider, 'website')
        merchant_id = getattr(crawler.spider, 'merchant_id')
        return cls(website, merchant_id)

    def open_spider(self, spider):
        logging.warning("SPIDER OPENED FROM PIPELINE")

    def close_spider(self, spider):
        """
        Use the self.outgoing_links set and iterate over. Add values to OutboundLinks table.

        Find first merchant from merchants table with corresponding merchant_id and
        find first requisite record with corresponding merchant_id. Use column_count
        function at end of crawling in order to calculate number of populated requisites.
        This amount will be added to /home merchants table.

        :param spider:
        :return:
        """

        # check if outgoing link exists. If no, then add a new entry
        for url in self.outgoing_links:
            existing_outgoing_link = OutboundLinks.query.filter_by(merchant_id=self.merchant_id, outbound_link=url).first()
            if existing_outgoing_link:
                pass
            else:
                outgoing_link = OutboundLinks(merchant_id=self.merchant_id, outbound_link=url)
                db.session.add(outgoing_link)

        # find merchant and its requisites table. If requisite table exists pass
        # requisite table to column_count function for requisite completion calculation
        # if requisites table does not exist (probably because no item was found during
        # scraping that leads to process_item() not being executed) then create table and calculate
        merchant = Merchant.query.filter_by(id=self.merchant_id).order_by(Merchant.id).first()
        requisite = Requisites.query.filter_by(merchant_id=self.merchant_id).order_by(Requisites.id).first()
        if requisite:
            try:
                merchant.requisites_completion = column_count(requisite)
            except AttributeError:
                merchant.requisites_completion = 0
        else:
            requisite = Requisites(website=self.website, merchant_id=self.merchant_id)
            db.session.add(requisite)
            try:
                merchant.requisites_completion = column_count(requisite)
            except AttributeError:
                merchant.requisites_completion = 0

        # update merchant.next_scan based on merchant.recurrence value
        today = date.today()
        if merchant.recurrence == 'Weekly':
            next_scan = today + relativedelta(weeks=1)
            merchant.next_scan = next_scan
        elif merchant.recurrence == 'Monthly':
            next_scan = today + relativedelta(month=1)
            merchant.next_scan = next_scan

        merchant.scan_status = True

        db.session.commit()
        print(f'===== outgoing links - {self.outgoing_links}')
        logging.warning("SPIDER CLOSED FROM PIPELINE")

    def process_item(self, item, spider):

        # check if item is an outgoing link. If is, add to set later set is added to OutboundLinks table
        if 'href' in item:
            self.outgoing_links.add(urlparse(item['href']).netloc)
            return item

        # check if item is restricted word. If is, add to Words table
        if 'words' in item:
            url, word = item['words']
            words = Words(merchant_id=self.merchant_id, words=word, url=url)
            db.session.add(words)
            db.session.commit()
            return item

        # search for first entry by merchant_id. Order by Requisite.id in case there are multiple
        # entries of the same merchant_id in requisites
        requisite = Requisites.query.filter_by(merchant_id=self.merchant_id).order_by(Requisites.id).first()

        # if merchant already has requisites record then update values of existing requisite record
        if requisite:  # TODO this will override any requisites saved by user. Maybe better not to update?
            for key in item:
                exec(f"requisite.{key} = item['{key}']")
                db.session.commit()

        # if merchant has no requisites record then create a new record and update fields
        else:
            requisite = Requisites(website=self.website, merchant_id=self.merchant_id)
            db.session.add(requisite)
            for key in item:
                exec(f"requisite.{key} = item['{key}']")
                db.session.commit()
        return item
