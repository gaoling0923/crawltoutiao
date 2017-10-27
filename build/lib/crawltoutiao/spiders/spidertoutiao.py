# -*- coding: utf-8 -*-
import json

import datetime
import scrapy
import time
from scrapy.conf import settings

from crawltoutiao.items import CrawltoutiaoItem, detailItem, TouTiaoItem, TTDetailItem


class SpiderttiaoSpider(scrapy.Spider):
    name = 'spidertoutiao'
    allowed_domains = ['www.toutiao.com']
    # start_urls = ['http://www.toutiao.com/']
    # 'https://www.toutiao.com/search_content/?offset=0&format=json&keyword=%E7%BA%A2%E6%97%97%E8%BD%BF%E8%BD%A6&autoload=true&count=20&cur_tab=1'
    # start_urls = ['https://www.toutiao.com/search_content/?offset={offset}&format=json&keyword=%E7%BA%A2%E6%97%97%E8%BD%BF%E8%BD%A6&autoload=true&count=20&cur_tab=1']
    start_urls = ['https://www.toutiao.com/search_content/?offset={offset}&format=json&keyword={keywords}&autoload=true&count=20&cur_tab=1']


    def __init__(self,**kwargs):
        self.item_startdate= settings['ITEM_STARTDATE']
        self.count=0
        self.keywords=['一汽 红旗','一汽 徐留平','一汽 自主',
                    '一汽 情怀',
                    '一汽 复兴',
                    '一汽 改革',
                    '一汽 共和国长子',
                    '一汽 解放']

        # self.keywords=['一汽 奔腾']


    def start_requests(self):



        for url in self.start_urls:

            for key in self.keywords:
                self._wait()
                self.count=self.count+1

                url2=url.replace('{keywords}',key)
                print('keywordscrawl====',key)
                print('count1:', self.count)

                ln_sep = 0
                while ln_sep <= 180:
                    self._wait()
                    print('count2:', ln_sep)

                    request= scrapy.Request(url=url2.format(offset=ln_sep, count=20), callback=self.parse)
                    request.meta['keywords']=key
                    yield  request
                    ln_sep = ln_sep + 20


    def parse(self, response):
        #article_url
        key= response.meta['keywords']
        response_text = json.loads(response.text)["data"]
        baseurl = 'www.toutiao.com'
        for text in response_text:
            # while text:
            # json.
            ttitem=TouTiaoItem()

            media_creator_id = ''
            play_effective_count = ''
            media_name = ''
            repin_count = ''
            ban_comment = ''
            show_play_effective_count = ''
            single_mode = ''
            abstract = ''
            display_title = ''
            media_avatar_url = ''
            cdatetime = ''
            article_type = ''
            more_mode = ''
            create_time = ''
            has_m3u8_video = ''
            keywords = ''
            video_duration = ''
            has_mp4_video = ''
            favorite_count = ''
            # aggr_type = ''
            comments_count = ''
            article_sub_type = ''
            bury_count = ''
            title = ''
            has_video = ''
            share_url = ''
            id = ''
            source = ''
            comment_count = ''
            article_url = ''
            image_url = ''
            middle_mode = ''
            large_mode = ''
            item_source_url = ''
            media_url = ''
            display_time = ''
            publish_time = ''
            go_detail_count = ''
            image_list = ''
            gallary_image_count = ''
            item_seo_url = ''
            video_duration_str = ''
            source_url = ''
            tag_id = ''
            item_id = ''
            natant_level = ''
            seo_url = ''
            display_url = ''
            url = ''
            level = ''
            digg_count = ''
            behot_time = ''
            # image_detail = ''
            tag = ''
            has_gallery = ''
            has_image = ''
            group_id = ''
            middle_image = ''
            print('type(title)', type(title))
            for t in text:
                if t == 'media_creator_id':
                    media_creator_id = text['media_creator_id'],
                if t == 'play_effective_count':
                    play_effective_count = text['play_effective_count'],
                if t == 'media_name':
                    media_name = text['media_name'],
                if t == 'repin_count':
                    repin_count = text['repin_count'],
                if t == 'ban_comment':
                    ban_comment = text['ban_comment'],
                if t == 'show_play_effective_count':
                    show_play_effective_count = text['show_play_effective_count'],
                if t == 'single_mode':
                    single_mode = text['single_mode'],
                if t == 'abstract':
                    abstract = text['abstract'],
                if t == 'display_title':
                    display_title = text['display_title'],
                if t == 'media_avatar_url':
                    media_avatar_url = text['media_avatar_url'],
                if t == 'cdatetime':
                    cdatetime = text['datetime'],
                if t == 'article_type':
                    article_type = text['article_type'],
                if t == 'more_mode':
                    more_mode = text['more_mode'],
                if t == 'create_time':
                    create_time = text['create_time'],
                if t == 'has_m3u8_video':
                    has_m3u8_video = text['has_m3u8_video'],
                if t == 'keywords':
                    keywords = text['keywords'],
                if t == 'video_duration':
                    video_duration = text['video_duration'],
                if t == 'has_mp4_video':
                    has_mp4_video = text['has_mp4_video'],
                if t == 'favorite_count':
                    favorite_count = text['favorite_count'],
                # if t == 'aggr_type':
                #     aggr_type = text['aggr_type'],
                if t == 'comments_count':
                    comments_count = text['comments_count'],
                if t == 'article_sub_type':
                    article_sub_type = text['article_sub_type'],
                if t == 'bury_count':
                    bury_count = text['bury_count'],
                if t == 'title':
                    title = text['title'],
                if t == 'has_video':
                    has_video = text['has_video'],
                if t == 'share_url':
                    share_url = text['share_url'],
                if t == 'id':
                    id = text['id'],
                if t == 'source':
                    source = text['source'],
                if t == 'comment_count':
                    comment_count = text['comment_count'],
                if t == 'article_url':
                    article_url = text['article_url'],
                if t == 'image_url':
                    image_url = text['image_url'],
                if t == 'middle_mode':
                    middle_mode = text['middle_mode'],
                if t == 'large_mode':
                    large_mode = text['large_mode'],
                if t == 'item_source_url':
                    item_source_url = text['item_source_url'],
                if t == 'media_url':
                    media_url = text['media_url'],
                if t == 'display_time':
                    display_time = text['display_time'],
                if t == 'publish_time':
                    publish_time = text['publish_time'],
                if t == 'go_detail_count':
                    go_detail_count = text['go_detail_count'],
                if t == 'image_list':
                    image_list = text['image_list'],
                if t == 'gallary_image_count':
                    gallary_image_count = text['gallary_image_count'],
                if t == 'item_seo_url':
                    item_seo_url = text['item_seo_url'],
                if t == 'video_duration_str':
                    video_duration_str = text['video_duration_str'],
                if t == 'source_url':
                    source_url = text['source_url'],
                if t == 'tag_id':
                    tag_id = text['tag_id'],
                if t == 'item_id':
                    item_id = text['item_id'],
                if t == 'natant_level':
                    natant_level = text['natant_level'],
                if t == 'seo_url':
                    seo_url = text['seo_url'],
                if t == 'display_url':
                    display_url = text['display_url'],
                if t == 'url':
                    url = text['url'],
                if t == 'level':
                    level = text['level'],
                if t == 'digg_count':
                    digg_count = text['digg_count'],
                if t == 'behot_time':
                    behot_time = text['behot_time'],
                # if t == 'image_detail':
                #     image_detail = text['image_detail'],
                if t == 'tag':
                    tag = text['tag'],
                if t == 'has_gallery':
                    has_gallery = text['has_gallery'],
                if t == 'has_image':
                    has_image = text['has_image'],
                if t == 'group_id':
                    group_id = text['group_id'],
                if t == 'middle_image':
                    middle_image = text['middle_image']
            # media_creator_id = text['media_creator_id'],
            print('type(str(title))',type(str(title)))
            ttitem['media_creator_id'] = str(media_creator_id[0]) if media_creator_id else ''
            ttitem['play_effective_count'] = str(play_effective_count[0]) if play_effective_count else ''
            ttitem['media_name'] = str(media_name[0]) if media_name else ''
            ttitem['repin_count'] = str(repin_count[0]) if repin_count else ''
            ttitem['ban_comment'] = str(ban_comment[0]) if ban_comment else ''
            ttitem['show_play_effective_count'] = str(show_play_effective_count[0]) if show_play_effective_count else ''
            ttitem['single_mode'] = str(single_mode[0]) if single_mode else ''
            ttitem['abstract'] = str(abstract[0]) if abstract else ''
            ttitem['display_title'] = str(display_title[0]) if display_title else ''
            ttitem['media_avatar_url'] = str(media_avatar_url[0]) if media_avatar_url else ''
            ttitem['datetime'] = str(cdatetime[0]) if cdatetime else ''
            ttitem['article_type'] = str(article_type[0]) if article_type else ''
            ttitem['more_mode'] = str(more_mode[0]) if more_mode else ''
            ttitem['create_time'] = str(create_time[0]) if create_time else ''
            ttitem['has_m3u8_video'] = str(has_m3u8_video[0]) if has_m3u8_video else ''
            ttitem['keywords'] = str(keywords[0]) if keywords else ''
            ttitem['video_duration'] = str(video_duration[0]) if video_duration else ''
            ttitem['has_mp4_video'] = str(has_mp4_video[0]) if has_mp4_video else ''
            ttitem['favorite_count'] = str(favorite_count[0]) if favorite_count else ''
            # ttitem['aggr_type'] = aggr_type[0] if aggr_type else ''
            ttitem['comments_count'] = str(comments_count[0]) if comments_count else ''
            ttitem['article_sub_type'] = str(article_sub_type[0]) if article_sub_type else ''
            ttitem['bury_count'] = str(bury_count[0]) if bury_count else ''
            ttitem['title'] = str(title[0]) if title else ''
            ttitem['has_video'] = str(has_video[0]) if has_video else ''
            ttitem['share_url'] = str(share_url[0]) if share_url else ''
            ttitem['id'] = str(id[0]) if id else ''
            ttitem['source'] = str(source[0]) if source else ''
            ttitem['comment_count'] = str(comment_count[0]) if comment_count else ''
            ttitem['article_url'] = str(article_url[0]) if article_url else ''
            ttitem['image_url'] = str(image_url[0]) if image_url else ''
            ttitem['middle_mode'] = str(middle_mode[0]) if middle_mode else ''
            ttitem['large_mode'] = str(large_mode[0]) if large_mode else ''
            ttitem['item_source_url'] = str(item_source_url[0]) if item_source_url else ''
            ttitem['media_url'] = str(media_url[0]) if media_url else ''
            ttitem['display_time'] = str(display_time[0]) if display_time else ''
            ttitem['publish_time'] = str(publish_time[0]) if publish_time else ''
            ttitem['go_detail_count'] = str(go_detail_count[0]) if go_detail_count else ''
            ttitem['image_list'] = str(image_list[0]) if image_list else ''
            ttitem['gallary_image_count'] = str(gallary_image_count[0]) if gallary_image_count else ''
            ttitem['item_seo_url'] = str(item_seo_url[0]) if item_seo_url else ''
            ttitem['video_duration_str'] = str(video_duration_str[0]) if video_duration_str else ''
            ttitem['source_url'] = str(source_url[0]) if source_url else ''
            ttitem['tag_id'] = str(tag_id[0]) if tag_id else ''
            ttitem['item_id'] = str(item_id[0]) if item_id else ''
            ttitem['natant_level'] = str(natant_level[0]) if natant_level else ''
            ttitem['seo_url'] = str(seo_url[0]) if seo_url else ''
            ttitem['display_url'] = str(display_url[0]) if display_url else ''
            ttitem['url'] = str(url[0]) if url else ''
            ttitem['level'] = str(level[0]) if level else ''
            ttitem['digg_count'] = str(digg_count[0]) if digg_count else ''
            ttitem['behot_time'] = str(behot_time[0]) if behot_time else ''
            # ttitem['image_detail'] = image_detail[0] if image_detail else ''
            ttitem['tag'] = str(tag[0]) if tag else ''
            ttitem['has_gallery'] = str(has_gallery[0]) if has_gallery else ''
            ttitem['has_image'] = str(has_image[0]) if has_image else ''
            ttitem['group_id'] = str(group_id[0]) if group_id else ''
            ttitem['middle_image'] = str(middle_image) if middle_image else ''

            url=response.url
            ttitem['fromurl'] = url
            timeStamp = time.time()
            timeArray = time.localtime(timeStamp)
            print('timeArray==',timeArray)
            nowTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

            ttitem['crawldate'] = nowTime
            ttitem['kwords'] = key

            strtime2 = '20170923000000'
            # strtimeArray = time.localtime(strtime)
            # vartime = time.strftime("%Y-%m-%d %H:%M:%S", strtimeArray)

            # create_time = time.localtime(create_time[0])
            print('create_time=',create_time)
            create_time = datetime.datetime.fromtimestamp(create_time[0])
            vartime = datetime.datetime.strptime(self.item_startdate,"%Y-%m-%d %H:%M:%S")
            # createtime = time.strftime("%Y-%m-%d %H:%M:%S", create_time)

            # varnowTime = time.strptime("%Y-%m-%d", timeArray)

            # varnowTime = time.strptime(nowTime, '%Y%m%d%H%M%S')
            # vartime = time.strptime(strtime2, '%Y%m%d%H%M%S')


            d1=datetime.datetime.now()
            d2 = d1 - datetime.timedelta(days=1)

            print('d2.date()',d2.date())

            print('createtime==', create_time.date())
            print('d2==', d2.timetuple())


            # if create_time.date() >= d2.date():
            if create_time.date() >= vartime.date():
            #     yield item
                yield ttitem

            # yield ttitem

            #循环取列
            next = ''
            ngroupid = ''
            nitemid = ''
            for t in text:
                # print('text---t=',t)

                if t == 'source_url':
                    # text['text']
                    next = text['source_url']
                if t == 'tag_id':
                    ngroupid = text['tag_id']
                if t == 'item_id':
                    nitemid = text['item_id']
                    # print('tag_id', groupid)
            if ngroupid !='' and nitemid !='':
                nextUrl = "http://%s/api/comment/list/?group_id=%s&item_id=%s&offset ={offset}&count=%s" % (
                    baseurl, ngroupid, nitemid, 200)

                ln_sep2 = 0
                while ln_sep2 <= 180:
                    self._wait()
                    print('count:', ln_sep2)

                    request = scrapy.Request(url=nextUrl.format(offset=ln_sep2, count=20), callback=self.savedetail)
                    request.meta['keywords'] = key
                    request.meta['group_id'] = group_id[0]
                    yield request
                    ln_sep2 = ln_sep2 + 20

    def savedetail(self,response):
            # http: // www.toutiao.com / api / comment / list /?group_id = 6464798681328517646 & item_id = 6464798681328517646 & offset = 0 & count = 900

            # print('ttttttttt')
            key = response.meta['keywords']
            group_id = response.meta['group_id']
            response_text = json.loads(response.text)["data"]
            if response_text is None:
                return
            # print('response_text=',response_text)

            # for rtext in response_text:
            ditem = TTDetailItem()
            # item2['textdetail'] = response_text['comments']
            # yield item2
            # for text in response_text:
            comments=response_text['comments']
            print('comments==',comments)
            if comments :

                    # if response_text["has_more"] == 1:
                #     print('uuuuuu')
                #     offset = json.loads(response.text)["offset"]
                #     yield scrapy.Request(url=self.start_urls.format(offset=offset, count=20), callback=self.pic_detail)

                # for text in response_text:
                    # self.table2.save(text)
                #     item2 = detailItem()
                #     item2['textdetail'] = text
                #     yield item2
                text = ''
                digg_count = ''
                reply_data = ''
                reply_list = ''
                reply_count = ''
                create_time = ''
                # user = ''
                avatar_url = ''
                user_id = ''
                name = ''
                dongtai_id = ''
                user_digg = ''
                id = ''

                for ts in comments:
                    print('cotttt',ts)

                    for t in ts:
                        # print('cot', t)
                        if t=='text':
                            text = ts['text']
                        if t == 'digg_count':
                            digg_count = ts['digg_count']
                        if t == 'reply_data':
                            reply_data = ts['reply_data']
                            reply_list = reply_data['reply_list']
                        if t == 'reply_count':
                            reply_count = ts['reply_count']
                        if t == 'create_time':
                            create_time = ts['create_time']
                        if t == 'user':
                            user = ts['user']
                            for u in user:
                                avatar_url = user['avatar_url']
                                user_id = user['user_id']
                                name = user['name']
                        if t == 'dongtai_id':
                            dongtai_id = ts['dongtai_id']
                        if t == 'user_digg':
                            user_digg = ts['user_digg']
                        if t == 'id':
                            id = ts['id']
                ditem['text'] = text if text else ''
                ditem['digg_count'] = str(digg_count) if digg_count else ''
                # ditem['reply_data'] = reply_data if reply_data else ''
                ditem['reply_list'] = str(reply_list[0]) if reply_list else ''
                ditem['reply_count'] = str(reply_count) if reply_count else ''
                ditem['create_time'] = str(create_time) if create_time else ''
                # ditem['user'] = user if user else ''
                ditem['avatar_url'] = str(avatar_url[0]) if avatar_url else ''
                ditem['user_id'] = str(user_id) if user_id else ''
                ditem['name'] = str(name) if name else ''
                ditem['dongtai_id'] = str(dongtai_id) if dongtai_id else ''
                ditem['user_digg'] = str(user_digg[0]) if user_digg else ''
                ditem['id'] = str(id) if id else ''

                url=response.url
                ditem['fromurl'] = url

                timeStamp = time.time()
                timeArray = time.localtime(timeStamp)
                nowTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
                ditem['crawldate'] = nowTime

                ditem['kwords'] = key
                ditem['group_id'] = str(group_id)

                strtime2 = '20170923000000'

                # create_time = time.localtime(create_time)
                # createtime = time.strftime("%Y-%m-%d %H:%M:%S", create_time)

                # vartime = time.strptime(strtime2, '%Y%m%d%H%M%S')

                create_time = datetime.datetime.fromtimestamp(create_time)
                vartime = datetime.datetime.strptime(self.item_startdate,"%Y-%m-%d %H:%M:%S")

                print('ddcreatetime==', create_time)
                print('vartime==', vartime)
                if create_time.date() >= vartime.date():
                    #     yield item
                    yield ditem
                # yield  ditem

    def _wait(self):
        for i in range(0, 3):
            print('.' * (i % 3 + 1))
            time.sleep(0.3)


