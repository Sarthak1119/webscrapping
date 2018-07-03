# -*- coding: utf-8 -*-
"""
Created on Mon Jun 25 10:38:07 2018

@author: Rishabh
"""

import scrapy
import pymysql
import time
#import multiprocessing as mp
#import threading

class HotelInformation(scrapy.Spider):
    inittime=time.time()
    print(inittime)

    custom_settings={
            'DNSCACHE_ENABLED':'False',
            'DOWNLOAD_DELAY':'0.05',
            'CONCURRENT_REQUESTS':'50',
            'CONCURRENT_REQUESTS_PER_DOMAIN':'14',
            #'LOG_ENABLED':'False'
            }

    name="hotels"
    city_name=input("Enter City: ")
    city_name.lower().strip()
    filename="Hotels in %s"%city_name

    conn = pymysql.connect(host='127.0.0.1', user='root', passwd=None, db='tripadvisor',use_unicode=True, charset="utf8")
    cur=conn.cursor()
    cur.execute("SELECT url,id FROM city WHERE City= %s",(city_name))
    for r in cur:
        url=str(r[0]) 
        id_city=str(r[1])
        start_urls=[url]
    
    
    cur.close()
    hotelname=None




    def parse(self,response):
        for href in response.css('div.listing_title a::attr(href)'):
            yield response.follow(href,self.hotels_name)
              
        for href in response.css('div.pageNumbers a::attr(href)'):
            #threading.Thread(target=self.parse,args=(response.follow(href),)).start()
            yield response.follow(href,self.parse)
    
        exetime=time.time()
        print(exetime)
    
        
    def hotels_name(self,response):
        #text_rev=[]
            
        def extract_name(query):
            add=self.conn.cursor()
            # add.execute('INSERT INTO hotels_desc(name,city_id) values(%s,%s) ',(str(response.xpath(query).extract_first()),self.id_city))
            self.hotelname=str(response.xpath(query).extract_first())
            add.execute('INSERT INTO hotels_desc(name,city_id) SELECT * FROM (SELECT %s, %s) AS tmp WHERE NOT EXISTS( SELECT name, city_id from hotels_desc WHERE name=%s and city_id=%s)',(str(response.xpath(query).extract_first()),self.id_city,str(response.xpath(query).extract_first()),self.id_city))
            add.close()
            self.conn.commit()
            #with open(self.filename,'a') as f:
             #   f.write(response.xpath(query).extract_first()+"\n")
            return response.xpath(query).extract_first()
        def extract_url(query):
            rev=self.conn.cursor()
            rev.execute('update hotels_desc set review_summary=%s WHERE name=%s and city_id=%s',(str(response.xpath(query).extract_first()),self.hotelname,self.id_city))
            #with open("review_summary",'w') as u:
             #   u.write(str(response.xpath(query).extract_first())+"\n")
            rev.close()
            return response.xpath(query).extract_first()
    
        def extract_review(query):
            return response.xpath(query).extract_first()
            '''item=str(response.xpath(query).extract_first())
            seen=set(text_rev)
            if item not in seen:
                seen.add(item)
                text_rev.append(item)
            for i in text_rev:
                with open("Reviews",'a',encoding='utf-8') as r:
                    r.write(str(i)+"\n")
            return text_rev'''
    
        def extract_streetadd(query):
            self.street_address=str(response.xpath(query).extract_first())
            return response.xpath(query).extract_first()
    
        def extract_extnd(query):
            extended_add=str(response.xpath(query).extract_first())
            location='|'.join([self.street_address,extended_add])
            loc=self.conn.cursor()
            loc.execute('update hotels_desc set location=%s where name=%s and city_id=%s',(location,self.hotelname,self.id_city))
            return response.xpath(query).extract_first()
    
        exetime=time.time()
        print(exetime)
                #extract_review('//div[@class="prw_rup prw_reviews_text_summary_hsx"]/div[@class="entry"]')
        yield{
                'Name':extract_name('.//div[@class="ui_column is-12-tablet is-10-mobile hotelDescription"]/h1[@id="HEADING"]/text()'),
                'review_summary':extract_url('.//div[@class="prw_rup prw_common_bubble_rating rating"]/span/@alt'),
                'review':extract_review('.//div[@class="entry"]/p[@class="partial_entry"]/text()'),
                'street':extract_streetadd('.//span[@class="detail"]/span[@class="street-address"]/text()'),
                'extendedadd':extract_extnd('.//span[@class="detail"]/span[@class="extended-address"]/text()')
                }


