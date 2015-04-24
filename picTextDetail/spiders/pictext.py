#coding=utf-8

from scrapy.spider import BaseSpider
from scrapy.contrib.spiders import  CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.selector import HtmlXPathSelector
from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.item import Item
import json,re
import datetime,time
import sys,base64 

nowTime=time.strftime("%Y%m%d-%H%M", time.localtime())
FileOut=open("/disk2/picTextDetail/Out/new",'w');
FileOut.write("use lidongde;\n")
FileOutJson1=open("/disk2/picTextDetail/ResultJson1/"+nowTime,'w');
FileOutJson2=open("/disk2/picTextDetail/ResultJson2/"+nowTime,'w');
FileIn=open("/disk2/picTextDetail/In/new.all",'r')

class picTextDetailSpider(CrawlSpider):       
    name = "picTextDetailSpider"
    allowed_domains = ["taobao.com","tmall.com"]

    def start_requests(self):
        search = "http://hws.m.taobao.com/cache/wdetail/5.0/?ttid=2013@taobao_h5_1.0.0&exParams={}&id="
        for line in FileIn: 
            lineArr = line.strip().split("\t")
            if len(lineArr)<=2:
                continue;
            tid = lineArr[0];
            ttype = lineArr[1]; #  pin_item  or pin_item_compare
            tableId  = lineArr[2]; #  
            time.sleep(0.1);
            queryStr = search+tid; 
            yield Request(queryStr,meta={'tid':tid,'ttype':ttype,'tableId':tableId},callback=self.parse)

    def parse(self, response):
        tid = response.meta['tid']
        ttype = response.meta['ttype']
        tableId = response.meta['tableId']
        jsonStr = str(response.body);
        FileOutJson1.write(jsonStr+"\n")
        try:
            jsonMap= json.loads(jsonStr)
            if( "SUCCESS::" in jsonMap["ret"][0]):
                jsonData = jsonMap["data"];
                jsonSeller = jsonData["seller"]
                jsonRateInfo = jsonData["rateInfo"]
                jsonCreditLevel = jsonSeller["creditLevel"]
                jsonPicTextDetail = jsonData["descInfo"]["briefDescUrl"]
                if jsonPicTextDetail.find("http://")==0:
                    yield Request(jsonPicTextDetail,meta={'tid':tid,'ttype':ttype,'jsonSeller':jsonSeller,'jsonRateInfo':jsonRateInfo,'creditLevel':jsonCreditLevel,'tableId':tableId},callback=self.parse2)
        except:
            pass;

    def addslashes(s):
        d = {'"':'\\"', "'":"\\'", "\0":"\\\0", "\\":"\\\\"}
        return ''.join(d.get(c, c) for c in s)

    def parse2(self, response):
        tid = response.meta['tid']
        ttype = response.meta['ttype']
        tableId = response.meta['tableId']
        jsonSeller = response.meta['jsonSeller']
        jsonRateInfo = response.meta['jsonRateInfo']
        jsonCreditLevel= response.meta['creditLevel']
        jsonStr = str(response.body);
        FileOutJson2.write(jsonStr+"\n")
        try:
            jsonMap=  json.loads(jsonStr)
            if( "SUCCESS::" in jsonMap["ret"][0]):
                jsontpDetail = jsonMap['data']['pages']
                sql = "";
                if ttype=="pin_item":
                    sql="update pin_item set picTextDetail=\"%s\",originComment=\"%s\" , storeInfo=\"%s\" where id=%s;"  %(addslashes(json.dumps(jsontpDetail)),addslashes(json.dumps(jsonRateInfo)),addslashes(json.dumps(jsonSeller)), tableId)
                elif ttype=="pin_item_compare":
                    sql="update pin_item_compare set level=%s where url=\"%s\";" %(jsonCreditLevel,tableId) 
                FileOut.write(sql+"\n")
                #FileOut.write("%s\1%s\1%s\1%s\1%s\1%s\1%s\n" %(tableId, tid,ttype, json.dumps(jsonSeller), json.dumps(jsonRateInfo),  json.dumps(jsontpDetail), json.dumps(jsonCreditLevel)))
            FileOut.flush();
        except:
            pass;
