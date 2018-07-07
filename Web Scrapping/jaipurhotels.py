# -*- coding: utf-8 -*-
"""
Created on Mon Jun 25 10:38:07 2018

@author: Rishabh
"""

import scrapy
import pymysql



class HotelInformation(scrapy.Spider):
    #inittime=time.time()
    #print(inittime)
    name="hotels"

    custom_settings={
            'DOWNLOAD_DELAY':'0.05',
            'CONCURRENT_REQUESTS':'50',
            'CONCURRENT_REQUESTS_PER_DOMAIN':'10',
            #'LOG_ENABLED':'True'
            }

    
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
            yield response.follow(href,self.parse)
    
        #exetime=time.time()
        #print(exetime)
    
        
    def hotels_name(self,response):
        amenities=[]
            
        def extract_name(query):
            add=self.conn.cursor()
            self.hotelname=str(response.xpath(query).extract_first())
            add.execute('INSERT INTO hotels_desc(name,city_id) SELECT * FROM (SELECT %s, %s) AS tmp WHERE NOT EXISTS( SELECT name, city_id from hotels_desc WHERE name=%s and city_id=%s)',(str(response.xpath(query).extract_first()),self.id_city,str(response.xpath(query).extract_first()),self.id_city))
            add.close()
            self.conn.commit()
            return response.xpath(query).extract_first()
        def extract_url(query):
            rev=self.conn.cursor()
            rev.execute('update hotels_desc set review_summary=%s WHERE name=%s and city_id=%s',(str(response.xpath(query).extract_first()),self.hotelname,self.id_city))
            rev.close()
            return response.xpath(query).extract_first()
    
        def extract_streetadd(query):
            self.street_address=str(response.xpath(query).extract_first())
            return response.xpath(query).extract_first()
    
        def extract_extnd(query):
            extended_add=str(response.xpath(query).extract_first())
            location='|'.join([self.street_address,extended_add])
            loc=self.conn.cursor()
            loc.execute('update hotels_desc set location=%s where name=%s and city_id=%s',(location,self.hotelname,self.id_city))
            return response.xpath(query).extract_first()
        
        def extract_topamenities(query):
            for item in response.xpath(query):
                amenities.append(item.xpath('text()').extract()[0])
          
            stramen=','.join(amenities)
           
            amen=self.conn.cursor()
            amen.execute('SELECT id FROM hotels_desc WHERE city_id=%s and name=%s',(self.id_city,self.hotelname))
            for r in amen:
                h_id=r[0]
            amen.execute('INSERT INTO HotelDetails(hotel_id,Amenities) SELECT * FROM (SELECT %s,%s) As tmp WHERE NOT EXISTS( SELECT hotel_id from HotelDetails WHERE hotel_id= %s)',(h_id,stramen,h_id))
            amen.close()
            self.conn.commit()
            return amenities
        
        def extract_price(query):
            pri=self.conn.cursor()
            pri.execute('SELECT id FROM hotels_desc WHERE city_id=%s and name=%s',(self.id_city,self.hotelname))
            for r in pri:
                h_id=r[0]
            pri.execute('INSERT INTO hotelprice(hotel_id,Price) SELECT * FROM (SELECT %s,%s) As tmp WHERE NOT EXISTS(SELECT hotel_id from hotelprice where hotel_id=%s)',(h_id,response.xpath(query).extract_first(),h_id))
            pri.close()
            self.conn.commit()
            return response.xpath(query).extract_first() 
        #exetime=time.time()
       # print(exetime)
                
        yield{
                'Name':extract_name('.//div[@class="ui_column is-12-tablet is-10-mobile hotelDescription"]/h1[@id="HEADING"]/text()'),
                'review_summary':extract_url('.//div[@class="prw_rup prw_common_bubble_rating rating"]/span/@alt'),
                'street':extract_streetadd('.//span[@class="detail"]/span[@class="street-address"]/text()'),
                'extendedadd':extract_extnd('.//span[@class="detail"]/span[@class="extended-address"]/text()'),
                'TopAmenities':extract_topamenities('//*[@id="taplc_hotel_detail_overview_cards_0"]/div/div[2]/div/div[2]/div[@class="detailListItem"]'),
                'Price':extract_price('//*[@id="ABOUT_TAB"]/div[2]/div[1]/div[2]/div[2]/div[1]/div[2]/div[@class="textitem"]/text()')
                }


