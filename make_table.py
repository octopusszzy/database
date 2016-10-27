# -*- coding: utf-8 -*-
import MySQLdb

try:
	conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='root',db='test',port=3306)
	cur=conn.cursor()
	#cur.execute('select * from user')
	#cur.execute('create database if not exists user')
	#cur.execute('create table if not exists user(id int, name varchar(100))')
	#cur.execute('delete from user where id=0')
	#cur.execute('insert into user values(0,"zzy")')

	#ignore type
	cur.execute('create table if not exists item(\
		id varchar(20) not null unique,\
		labels_en_value varchar(300),\
		labels_en_language varchar(10),\
		descriptions_en_value varchar(300),\
		descriptions_en_language varchar(10),\
		sitelinks_enwiki_site varchar(15),\
		sitelinks_enwiki_title varchar(80),\
		sitelinks_enwikiquote_site varchar(15),\
		sitelinks_enwikiquote_title varchar(80),\
		sitelinks_enwikiversity_site varchar(15),\
		sitelinks_enwikiversity_title varchar(80),\
		primary key (id))')

	cur.execute('create table if not exists wikiBadges(\
		id varchar(20) not null,\
		wikiType varchar(10) not null,\
		badges varchar(20) not null,\
		foreign key (id) references item,\
		primary key (id, wikiType, badges))')

	#ignore type
	cur.execute('create table if not exists aliases(\
		id varchar(20) not null,\
		language varchar(10) not null,\
		value varchar(300) not null,\
		foreign key (id) references item,\
		primary key (id, language, value))')

	#ignore type,datatype
	cur.execute('create table if not exists property(\
		id varchar(20) not null unique,\
		labels_en_value varchar(20),\
		labels_en_language varchar(10),\
		primary key (id))')

	cur.close()
	conn.close()
except MySQLdb.Error,e:
	print "Mysql Error %d: %s" % (e.args[0], e.args[1])