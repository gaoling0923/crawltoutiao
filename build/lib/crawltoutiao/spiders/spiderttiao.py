# -*- coding: utf-8 -*-
import json

import scrapy

from crawltoutiao.items import CrawltoutiaoItem, detailItem


class SpiderttiaoSpider(scrapy.Spider):
    name = 'spiderttiao'
    allowed_domains = ['www.toutiao.com']
    # start_urls = ['http://www.toutiao.com/']
    'https://www.toutiao.com/search_content/?offset=0&format=json&keyword=%E7%BA%A2%E6%97%97%E8%BD%BF%E8%BD%A6&autoload=true&count=20&cur_tab=1'
    start_urls = ['https://www.toutiao.com/search_content/?offset={offset}&format=json&keyword=%E7%BA%A2%E6%97%97%E8%BD%BF%E8%BD%A6&autoload=true&count=20&cur_tab=1']

    def start_requests(self):
        for url in self.start_urls:

            ln_sep = 0
            while ln_sep <= 180:
                print('count:', ln_sep)

                yield scrapy.Request(url=url.format(offset=ln_sep, count=20), callback=self.parse)
                ln_sep = ln_sep + 20

    def parse(self, response):
        #article_url
        response_text = json.loads(response.text)["data"]
        baseurl = 'www.toutiao.com'
        for text in response_text:
            # while text:
            # json.
            item=CrawltoutiaoItem()
            item['text']=text
            # if text.has_key['source_url'] :
            #     print('text=====', text['source_url'])
            # if 'source_url' in text['text'].keys():
            #     print('text=====',text['source_url'])
            yield item
            for t in text:
                if t=='source_url':
                    # text['text']
                    next = text['source_url']
                    groupid = text['tag_id']
                    itemid = text['item_id']


                    print('tag_id', groupid)
                    # nextUrl ="http://%s/api/comment/list/?group_id=%s&item_id=%s&offset=%s&count=%s" %(baseurl,groupid,itemid,0,200)

                    nextUrl = "http://%s/api/comment/list/?group_id=%s&item_id=%s&offset ={offset}&count=%s" % (
                        baseurl, groupid, itemid, 200)

                    # if json.loads(response.text)["has_more"] == 1:
                    ln_sep2 = 0
                    while ln_sep2 <= 180:
                        print('count:', ln_sep2)

                        yield scrapy.Request(url=nextUrl.format(offset=ln_sep2, count=20), callback=self.savedetail)
                        ln_sep2 = ln_sep2 + 20


            # detailurl=text['source_url']
            # print('text', text['source_url']);

            # url=self.baseurl+detailurl
            #
            # if json.loads(response.text)["has_more"] == 1:
            #     offset = json.loads(response.text)["offset"]
            #     yield scrapy.Request(url=self.start_urls.format(offset=offset, count=120), callback=self.pic_detail)

        # self.getInfo()
    def savedetail(self,response):
            # http: // www.toutiao.com / api / comment / list /?group_id = 6464798681328517646 & item_id = 6464798681328517646 & offset = 0 & count = 900

            print('ttttttttt')
            response_text = json.loads(response.text)["data"]
            print(response_text)

            for text in response_text:
                item2 = detailItem()
                item2['textdetail'] = response_text['comments']
                yield item2

            # if response_text["has_more"] == 1:
            #     print('uuuuuu')
            #     offset = json.loads(response.text)["offset"]
            #     yield scrapy.Request(url=self.start_urls.format(offset=offset, count=20), callback=self.pic_detail)

            # for text in response_text:
                # self.table2.save(text)
            #     item2 = detailItem()
            #     item2['textdetail'] = text
            #     yield item2