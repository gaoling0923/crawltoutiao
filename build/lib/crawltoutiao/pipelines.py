# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from _md5 import md5

import happybase
import pymongo
from scrapy.conf import settings

from crawltoutiao.items import detailItem, CrawltoutiaoItem, TouTiaoItem, TTDetailItem



class HBasePipeline(object):
    def __init__(self):
        host = settings['HBASE_HOST']
        self.table_name1 = settings['HBASE_TABLE1']
        self.table_name2 = settings['HBASE_TABLE2']
        port = settings['HBASE_PORT']
        self.connection = happybase.Connection(host)


    def process_item(self, item, spider):
        # cl = dict(item)

        if isinstance(item, TouTiaoItem):
            # self.table.put('text', cl)
            table = self.connection.table(self.table_name1)
            print('进入pipline')
            media_creator_id = item['media_creator_id']
            play_effective_count = item['play_effective_count']
            media_name = item['media_name']
            repin_count = item['repin_count']
            ban_comment = item['ban_comment']
            show_play_effective_count = item['show_play_effective_count']
            single_mode = item['single_mode']
            abstract = item['abstract']
            display_title = item['display_title']
            media_avatar_url = item['media_avatar_url']
            datetime = item['datetime']
            article_type = item['article_type']
            more_mode = item['more_mode']
            create_time = item['create_time']
            has_m3u8_video = item['has_m3u8_video']
            keywords = item['keywords']
            video_duration = item['video_duration']
            has_mp4_video = item['has_mp4_video']
            favorite_count = item['favorite_count']
            # aggr_type = item['aggr_type']
            comments_count = item['comments_count']
            article_sub_type = item['article_sub_type']
            bury_count = item['bury_count']
            title = item['title']
            has_video = item['has_video']
            share_url = item['share_url']
            id = item['id']
            source = item['source']
            comment_count = item['comment_count']
            article_url = item['article_url']
            image_url = item['image_url']
            middle_mode = item['middle_mode']
            large_mode = item['large_mode']
            item_source_url = item['item_source_url']
            media_url = item['media_url']
            display_time = item['display_time']
            publish_time = item['publish_time']
            go_detail_count = item['go_detail_count']
            image_list = item['image_list']
            gallary_image_count = item['gallary_image_count']
            item_seo_url = item['item_seo_url']
            video_duration_str = item['video_duration_str']
            source_url = item['source_url']
            tag_id = item['tag_id']
            item_id = item['item_id']
            natant_level = item['natant_level']
            seo_url = item['seo_url']
            display_url = item['display_url']
            url = item['url']
            level = item['level']
            digg_count = item['digg_count']
            behot_time = item['behot_time']
            # image_detail = item['image_detail']
            tag = item['tag']
            has_gallery = item['has_gallery']
            has_image = item['has_image']
            group_id = item['group_id']
            middle_image = item['middle_image']
            fromurl = item['fromurl']
            crawldate = item['crawldate']
            kwords = item['kwords']

            table.put(md5(title.encode('utf-8') + group_id.encode('utf-8')).hexdigest(), {'cf1:media_creator_id':media_creator_id,
                                                                                        'cf1:play_effective_count':play_effective_count,
                                                                                        'cf1:media_name':media_name,
                                                                                        'cf1:repin_count':repin_count,
                                                                                        'cf1:ban_comment':ban_comment,
                                                                                        'cf1:show_play_effective_count':show_play_effective_count,
                                                                                        'cf1:single_mode':single_mode,
                                                                                        'cf1:abstract':abstract,
                                                                                        'cf1:display_title':display_title,
                                                                                        'cf1:media_avatar_url':media_avatar_url,
                                                                                        'cf1:datetime':datetime,
                                                                                        'cf1:article_type':article_type,
                                                                                        'cf1:more_mode':more_mode,
                                                                                        'cf1:create_time':create_time,
                                                                                        'cf1:has_m3u8_video':has_m3u8_video,
                                                                                        'cf1:keywords':keywords,
                                                                                        'cf1:video_duration':video_duration,
                                                                                        'cf1:has_mp4_video':has_mp4_video,
                                                                                        'cf1:favorite_count':favorite_count,
                                                                                        # 'cf1:aggr_type':aggr_type,
                                                                                        'cf1:comments_count':comments_count,
                                                                                        'cf1:article_sub_type':article_sub_type,
                                                                                        'cf1:bury_count':bury_count,
                                                                                        'cf1:title':title,
                                                                                        'cf1:has_video':has_video,
                                                                                        'cf1:share_url':share_url,
                                                                                        'cf1:id':id,
                                                                                        'cf1:source':source,
                                                                                        'cf1:comment_count':comment_count,
                                                                                        'cf1:article_url':article_url,
                                                                                        'cf1:image_url':image_url,
                                                                                        'cf1:middle_mode':middle_mode,
                                                                                        'cf1:large_mode':large_mode,
                                                                                        'cf1:item_source_url':item_source_url,
                                                                                        'cf1:media_url':media_url,
                                                                                        'cf1:display_time':display_time,
                                                                                        'cf1:publish_time':publish_time,
                                                                                        'cf1:go_detail_count':go_detail_count,
                                                                                        'cf1:image_list':image_list,
                                                                                        'cf1:gallary_image_count':gallary_image_count,
                                                                                        'cf1:item_seo_url':item_seo_url,
                                                                                        'cf1:video_duration_str':video_duration_str,
                                                                                        'cf1:source_url':source_url,
                                                                                        'cf1:tag_id':tag_id,
                                                                                        'cf1:item_id':item_id,
                                                                                        'cf1:natant_level':natant_level,
                                                                                        'cf1:seo_url':seo_url,
                                                                                        'cf1:display_url':display_url,
                                                                                        'cf1:url':url,
                                                                                        'cf1:level':level,
                                                                                        'cf1:digg_count':digg_count,
                                                                                        'cf1:behot_time':behot_time,
                                                                                        # 'cf1:image_detail':image_detail,
                                                                                        'cf1:tag':tag,
                                                                                        'cf1:has_gallery':has_gallery,
                                                                                        'cf1:has_image':has_image,
                                                                                        'cf1:group_id':group_id,
                                                                                        'cf1:middle_image':middle_image,
                                                                                        'cf1:fromurl':fromurl,
                                                                                        'cf1:crawldate':crawldate,
                                                                                        'cf1:kwords':kwords
                                                                                        })
        elif isinstance(item, TTDetailItem):
            table = self.connection.table(self.table_name2)
            text = item['text']
            digg_count = item['digg_count']
            # reply_data = item['reply_data']
            reply_list = item['reply_list']
            reply_count = item['reply_count']
            create_time = item['create_time']
            # user = item['user']
            avatar_url = item['avatar_url']
            user_id = item['user_id']
            name = item['name']
            dongtai_id = item['dongtai_id']
            user_digg = item['user_digg']
            id = item['id']
            fromurl = item['fromurl']
            crawldate = item['crawldate']
            kwords = item['kwords']
            group_id = item['group_id']
            table.put(md5(text.encode('utf-8') + name.encode('utf-8')).hexdigest(), {'cf1:text':text,
                                                                                        'cf1:digg_count':digg_count,
                                                                                        # 'cf1:reply_data':reply_data,
                                                                                        'cf1:reply_list':reply_list,
                                                                                        'cf1:reply_count':reply_count,
                                                                                        'cf1:create_time':create_time,
                                                                                        # 'cf1:user':user,
                                                                                        'cf1:avatar_url':avatar_url,
                                                                                        'cf1:user_id':user_id,
                                                                                        'cf1:name':name,
                                                                                        'cf1:dongtai_id':dongtai_id,
                                                                                        'cf1:user_digg':user_digg,
                                                                                        'cf1:id':id,
                                                                                        'cf1:fromurl': fromurl,
                                                                                        'cf1:crawldate': crawldate,
                                                                                        'cf1:kwords': kwords,
                                                                                        'cf1:group_id':group_id
                                                                                        })

        return item

class MongoDBPipeline(object):

    def __init__(self):
        host = settings['MONGODB_HOST']
        port = settings['MONGODB_PORT']
        dbName = settings['MONGODB_DBNAME']
        client = pymongo.MongoClient(host=host, port=port)
        self.tdb = client[dbName]



    def process_item(self, item, spider):

        if isinstance(item, TTDetailItem):
            print('11111')
            cl = dict(item)
            self.post2 = self.tdb[settings['MONGODB_DOCNAME2']]
            self.post2.insert(cl)
            return item
        elif isinstance(item, TouTiaoItem):
            print('22222')
            cl = dict(item)
            self.post1 = self.tdb[settings['MONGODB_DOCNAME1']]
            self.post1.insert(cl)
            return item

        return item
class CrawltoutiaoPipeline(object):
    def process_item(self, item, spider):
        return item
