#!/usr/bin/python
#-*-coding:utf-8-*-
# JSearchEngine
# Author: Jam <810441377@qq.com>
# -------//-------//----
import os
import sys
import cgi
import time
import gzip
import threading
import urllib2
import MySQLdb
from threading import Thread
from bs4 import BeautifulSoup
from cStringIO import StringIO
from urlparse import urlparse

Version = "1.0"
reload(sys)
sys.setdefaultencoding('utf8')

#调试编码
#sys.path.append('/src/chardet-1.1')
#import chardet
#print chardet.detect(htmlText)

################################################################
class DataBase(object):
	Host = "localhost"
	User = "root"
	Pass = "143205"
	DataBaseName = ""
	DataConn = None
	def __init__(self, dataBaseName):
		self.DataBaseName = dataBaseName.replace(".", "_")

	def Connect(self):
		self.DataConn = MySQLdb.connect(host=self.Host, user=self.User, passwd=self.Pass, db=self.DataBaseName, charset='utf8', port=3306)

	def Create(self):
		self.DataConn = MySQLdb.connect(host=self.Host, user=self.User, passwd=self.Pass, charset='utf8', port=3306)
		c = self.DataConn.cursor()
		try:
			c.execute('create database if not exists %s DEFAULT CHARACTER SET utf8 COLLATE utf8_bin' % (self.DataBaseName))
			self.DataConn.select_db(self.DataBaseName)
			c.execute("create table Link(Title Text, Link Text, Date Text);")
		except Exception, e:
			print "[E]->Class->DataBase->Create:%s" % (e)

		c.close()
		self.DataConn.commit()

	def Remove(self):
		self.DataConn = MySQLdb.connect(host=self.Host, user=self.User, passwd=self.Pass, charset='utf8', port=3306)
		c = self.DataConn.cursor()
		try:
			self.DataConn.select_db(self.DataBaseName)
			print "[I]Drop table LinkFactory, Drop table Link."
			c.execute("drop table if exists Link")
			#得自行删除数据库，否则某些情况MySQL会卡机
			#c.execute('drop database if exists %s' % (self.DataBaseName))
		
		except Exception, e:
			pass

		c.close()
		self.DataConn.commit()

	def LinkFactoryGet(self):
		c = self.DataConn.cursor()
		try:
			sql = "select * from Link where Title='';"
			c.execute(sql)
			return c.fetchone()

		except Exception, e:
			print "[E]->Class->DataBase->LinkFactoryGet:" + str(e)

	def LinkDel(self, link):
		c = self.DataConn.cursor()
		try:
			sql = "delete from Link where Link='%s';" % (link)
			c.execute(sql)
		except Exception, e:
			print "[E]->Class->DataBase->LinkDel:" + str(e)

		c.close()
		self.DataConn.commit()

	def LinkInsert(self, link):
		c = self.DataConn.cursor()
		try:
			sql = "select * from Link where Link='%s';" % (link)
			c.execute(sql)
			if c.fetchone() == None:
				sql = "insert into Link values('', '%s', '');" % (link)
				c.execute(sql)
				print sql

		except Exception, e:
			print "[E]->Class->DataBase->LinkInsert:" + str(e)

		c.close()
		self.DataConn.commit()
		return;

	def LinkUpdate(self, title, date, link):
		c = self.DataConn.cursor()
		try:
			sql = "update Link SET Title='%s',Date='%s' where Link='%s';" % (title, date, link)
			c.execute(sql)
			print sql

		except Exception, e:
			print "[E]->Class->DataBase->LinkUpdate:" + str(e)

		c.close()
		self.DataConn.commit()
		return;

	# def LinkInsert(self, title, link, dateTime):
	# 	c = self.DataConn.cursor()
	# 	#print "LinkInsert,title:%s,link:%s,dateTime:%s" %(title, link, dateTime)
	# 	try:
	# 		sql = "select * from Link where Link='%s';" % (link)
	# 		c.execute(sql)
	# 		if c.fetchone() == None:
	# 			sql = "insert into Link values('%s','%s','%s');" % (title, link, dateTime)
	# 			c.execute(sql)

	# 	except Exception, e:
	# 		print "[E]->Class->DataBase->LinkInsert:" + str(e)

	# 	c.close()
	# 	self.DataConn.commit()
	# 	return;

	def LinkSearch(self, title):
		c = self.DataConn.cursor()
		try:
			sql = "select * from Link where Title like '%" + title + "%';"
			c.execute(sql)
			data = c.fetchall()
			c.close()
			return data

		except Exception, e:
			print "[E]->Class->DataBase->LinkSearch:" + str(e)

		c.close()
		return [];


	def __del__(self):
		self.DataConn.close()


################################################################
#LinkFactory Class, 链接工厂类, 可以将要工作的链接放入流水线，也可以获取流水线上的链接
class LinkFactory(object):
	Host = ""
	MyDataBase = DataBase
	def __init__(self, dataBase):
		self.MyDataBase = dataBase

	def Get(self):
		link = self.MyDataBase.LinkFactoryGet()
		if link != None:
			return link[1]
		else:
			return None

###############################################################
#爬虫类，爬虫的一些操作都在这里执行
class Crawler(object):
	#全局变量
	TargetHost  = ""
	UserAgent   = ""

	UserAgent  = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.117 Safari/537.36'
	BadFilterRules  = ['#', '.jpeg','.jpg','.rar','.png','.zip','.rar','.7z','javascript:','mailto:']

	ThreadMax   = 15 # 最大线程
	ThreadLock  = threading.Lock()
	ThreadTotal = 0
	ThreadSignal = ""

	MyDataBase    = DataBase   
	MyLinkFactory = LinkFactory

	def __init__(self, host):
		self.TargetHost = host

	def ToUtf8(self, text):
		try:
			return text.decode("gbk")
		except Exception, e:
			return text

	def GetHtmlText(self, url):
		request  = urllib2.Request(url)
		request.add_header('Accept', "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp")
		request.add_header('Accept-Encoding', "*")
		request.add_header('User-Agent', self.UserAgent)
		rp = urllib2.urlopen(request)
		rpHtmlText = ""
		contentEncoding =  rp.headers.get('Content-Encoding')
		if  contentEncoding == 'gzip':
			compresseddata = rp.read()
			compressedstream = StringIO(compresseddata)
			gzipper = gzip.GzipFile(fileobj=compressedstream)
			rpHtmlText = gzipper.read()
		else:
			rpHtmlText = rp.read()
		return rpHtmlText

	def UrlEscape(self, url):
		try:
			url = urllib2.unquote(url)
			url = urllib2.quote(url.encode('utf8'))
			url = url.replace("%3A", ":")
		except Exception, e:
			print "[E]UrlEscape->%s,Url:%s" % (e, url) 
		
		return url;

	def UrlFilter(self, host, urls):
		returnUrls = []
		for url in urls:
			url_lower = url.lower()
			isBadUrl = False

			#判断是否为其他域名
			if url_lower.find("http:") >= 0 or url_lower.find("https:") >= 0:
				urlHost = ''
				try:
					urlHost = str(urlparse(url).hostname) # Have Bug
				except Exception, e:
					print "[E]->UrlFilter: %s" % (e)
				
				if urlHost.find(host) == -1:
					#print "!!! Fuck->Host:%s,Url:%s" % (urlHost, url)
					isBadUrl = True

			#进行过滤规则筛选
			for rule in self.BadFilterRules:
				if url_lower.find(rule) != -1: 
					#print url + "-" + rule
					isBadUrl = True

			if isBadUrl : continue

			#网址智能补全
			if url.find("http:") == -1 and url.find("https:") == -1:
				if url[0] != "/" : url = "/" + url
			if url.find("http:") == -1 and url.find("https:") == -1:
				url = "http://" + host + url
			
			url = self.UrlEscape(url)
			returnUrls.append(url)
		
		return returnUrls

	def AddUrls(self, urls):
		print "Thread AddUrls..."
		myDataBase = DataBase(self.TargetHost)
		myLinkFactory = LinkFactory(myDataBase)
		myDataBase.Connect()
		for url in urls:
			self.ThreadLock.acquire()
			myDataBase.LinkInsert(url)
			#myLinkFactory.Add(url)
			self.ThreadLock.release()

		return;

	def RunWork(self):

		myDataBase = DataBase(self.TargetHost)
		myLinkFactory = LinkFactory(myDataBase)
		myDataBase.Connect()

		while True:
			link = myLinkFactory.Get()

			if link == None:
				break
			else:
				print "[G]Link:" + link

			try:
				htmlText = self.GetHtmlText(link)
				htmlText = self.ToUtf8(htmlText)
				soup = BeautifulSoup(htmlText, from_encoding="utf8")
				docTitle = '404 - Not Found'

				try:
					docTitle = soup.title.string
					print docTitle

				except Exception, e:
					print "[E]soup.title.string:" + link 
				
				timeText = time.strftime('%Y-%m-%d',time.localtime(time.time()))
				myDataBase.LinkUpdate(docTitle, timeText, link)

				tags = soup.findAll('a')
				urls = [];
				for tag in tags:
					url = tag.get('href','')
					if url!= '' : urls.append(url)

				urls = self.UrlFilter(self.TargetHost, urls)
				print "Links length:%s" % (len(urls))

				t = threading.Thread(target=self.AddUrls,args=(urls,))
				t.setDaemon(True)
				t.start()


				while len(urls) >= 1 and myLinkFactory.Get() == None:
					myDataBase.Connect()
					print "Not Have work..."
					time.sleep(1)

				if self.ThreadTotal < 3:
					self.ThreadTotal += 1
					t = threading.Thread(target=self.RunWork,args=())
					t.setDaemon(True)
					t.start()

				#for url in urls:
				#	self.MyLinkFactory.Add(url)

			except urllib2.HTTPError,e:
					myDataBase.LinkDel(link)
					print "[E]->HTTP Error:%s,Link:%s" % (e, link)

			#break;

		return;

	def Work(self):
		self.MyDataBase = DataBase(self.TargetHost)
		self.MyLinkFactory = LinkFactory(self.MyDataBase)
		self.MyDataBase.Connect()
		self.RunWork()
		return;

	def NewWork(self):
		self.MyDataBase = DataBase(self.TargetHost)
		self.MyLinkFactory = LinkFactory(self.MyDataBase)
		self.MyDataBase.Remove()
		self.MyDataBase.Create()
		self.MyDataBase.Connect()
		self.MyDataBase.LinkInsert("http://" + self.TargetHost)
		print "[I]New work:" + "http://" + self.TargetHost
		self.RunWork()
		return;

	def Search(self, keyWord):
		print "You KeyWord:" + keyWord
		self.MyDataBase = DataBase(self.TargetHost)
		self.MyDataBase.Connect()
		data = self.MyDataBase.LinkSearch(keyWord)
		for row in data:
			print "Title:%s,Link:%s" % (row[0],row[1])

		return;



	def Stop(self):
		self.ThreadSignal = "stop"
		return;

def ThreadWork(host):
	newCrawler = Crawler(host)
	newCrawler.NewWork()


def Explain(argv):
	if len(argv) == 2:
		if argv[1] == "version":
			print "JSearchEngine v" + Version
			return;

	if len(argv) == 3:
		### Test
		if argv[1] == "fuck" and argv[2] == "this":
			print "Are you kidding me?"
			return;

		if(argv[1] == "newwork" and argv[2] != ""):
			newCrawler = Crawler(argv[2])
			newCrawler.NewWork()
			return;

		if(argv[1] == "work" and argv[2] != ""):
			newCrawler = Crawler(argv[2])
			newCrawler.Work()
			return;

	if len(argv) == 4:
		if(argv[1] == "search" and argv[2] != ""):
				newCrawler = Crawler(argv[2])
				newCrawler.Search(argv[3])
				return;

	print "JSearchEngine v" + Version
	print "python JSearchEngine.py newwork HOST \r\n         #Create a new work to search for HOST"
	print "python JSearchEngine.py work HOST \r\n         #Continue a work to search for HOST"
	print "python JSearchEngine.py search HOST title \r\n         #Search a title in DataBase"
	return;

def Main():
	Explain(sys.argv)

#___Main____
Main()