# -*- coding: utf-8 -*-
import json
import os
import MySQLdb

target = 'jsons'
path1 = os.path.join(target,'wikidata-latest-all.json')
path2 = os.path.join(target,'sub.json')
path_item = os.path.join(target,'item.json')
path_property = os.path.join(target,'property.json')

dic = dict()

def dfs(obj, bias,mode=1,output=None):
	if (type(obj) != type(dic)):
		return
	for (k,v) in obj.items():
		if (mode==2 and output!=None):
			output.write(bias*' '+str(k)+' '+str(type(obj[k]))+'\n')
		else:
			print (bias*' '+str(k)+' '+str(type(obj[k])))
		if (type(obj[k])==type(dic)):
			dfs(obj[k],bias+4,mode,output)
	return

def read_en_object(path,mode=1,output=None):
	cnt = 0
	f = open(path)
	f_output = None
	if (mode==2 and output!=None):
		f_output = open(output,'w')
	for line in f:
		if (str(line) > 0 and (line[-1] == '\n' or line[-1]=='\r')):
			line = line[:-1]
		if (str(line) > 0 and (line[-1] == '\n' or line[-1]=='\r')):
			line = line[:-1]
		if (str(line) > 0 and (line[-1] == ',')):
			line = line[:-1]
		cnt += 1
		if (cnt==13):
			break
		try:
			decodejson = json.loads(line)
			if (decodejson['type']=='item'):
				print (decodejson['id'],type(decodejson['aliases']['en'][0]))
			elif (decodejson['type']=='property'):
				print (decodejson['id'],decodejson['type'],decodejson['datatype'])
			else:
				print ('line: '+str(cnt))
		except Exception, e:
			if(cnt==1):
				continue
			print ('error in line '+str(cnt)+str(e))
			continue
	f.close()
	if (mode==2 and output!=None):
		f_output.close()


def read_object_detail(path,debug=False,stop=0):
	try:
		conn=MySQLdb.connect(host='127.0.0.1',user='root',passwd='root',db='test',port=3306)
		cur=conn.cursor()
		cnt = 0
		cnt_item = 0
		cnt_property = 0
		f = open(path)
		for line in f:
			if (str(line) > 0 and (line[-1] == '\n' or line[-1]=='\r')):
				line = line[:-1]
			if (str(line) > 0 and (line[-1] == '\n' or line[-1]=='\r')):
				line = line[:-1]
			if (str(line) > 0 and (line[-1] == ',')):
				line = line[:-1]
			line = line.encode('utf-8')
			cnt += 1
			try:
				decodejson = json.loads(line)
				if (decodejson['type']=='item'):
					cnt_item += 1
				elif (decodejson['type']=='property'):
					cnt_property += 1
				if (cnt % 1000 == 0):
					print ('Processing: '+str(cnt)+' items: '+str(cnt_item)+' properties: '+str(cnt_property))
				#if (cnt==5):
				#	break
				if (debug):
					if (cnt < stop):
						continue
					elif(cnt > stop):
						break
				if (debug):
					print (decodejson['type'])
				if (decodejson['type']=='item'):
					id = decodejson['id']
					if (debug):
						print ('a')
					labels_en_value = decodejson['labels']['en']['value'].encode("utf-8") if decodejson['labels'].get('en', None) != None else None
					if (debug):
						print ('b')
					labels_en_language = decodejson['labels']['en']['language'].encode("utf-8") if decodejson['labels'].get('en', None) != None else None
					if (debug):
						print ('c')
					descriptions_en_value = decodejson['descriptions']['en']['value'].encode("utf-8") if decodejson['descriptions'].get('en', None) != None else None
					if (debug):
						print ('d')
					descriptions_en_language = decodejson['descriptions']['en']['language'].encode("utf-8") if decodejson['descriptions'].get('en', None) != None else None
					if (debug):
						print ('e')
					sitelinks_enwiki_site = decodejson['sitelinks']['enwiki']['site'].encode("utf-8") if decodejson['sitelinks'].get('enwiki', None) != None else None
					if (debug):
						print ('f')
					sitelinks_enwiki_title = decodejson['sitelinks']['enwiki']['title'].encode("utf-8") if decodejson['sitelinks'].get('enwiki', None) != None else None
					if (debug):
						print ('g')
					sitelinks_enwikiquote_site = decodejson['sitelinks']['enwikiquote']['site'].encode("utf-8") if decodejson['sitelinks'].get('enwikiquote', None) != None else None
					if (debug):
						print ('h')
					sitelinks_enwikiquote_title = decodejson['sitelinks']['enwikiquote']['title'].encode("utf-8") if decodejson['sitelinks'].get('enwikiquote', None) != None else None
					if (debug):
						print ('i')
					sitelinks_enwikiversity_site = decodejson['sitelinks']['enwikiversity']['site'].encode("utf-8") if decodejson['sitelinks'].get('enwikiversity', None) != None else None
					if (debug):
						print ('j')
					sitelinks_enwikiversity_title = decodejson['sitelinks']['enwikiversity']['title'].encode("utf-8") if decodejson['sitelinks'].get('enwikiversity', None) != None else None

					value = [id,labels_en_value,labels_en_language,descriptions_en_value,\
						descriptions_en_language,sitelinks_enwiki_site,sitelinks_enwiki_title,sitelinks_enwikiquote_site,\
						sitelinks_enwikiquote_title,sitelinks_enwikiversity_site,sitelinks_enwikiversity_title]
					if (debug):
						print (1)
					cur.execute('replace into item values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', value)
					if (debug):
						print (2)
					if (sitelinks_enwiki_site!=None):
						for i in decodejson['sitelinks']['enwiki']['badges']:
							cur.execute('replace into wikiBadges values(%s,%s,%s)',[id,"enwiki",i.encode("utf-8")])
					if (debug):
						print (3)
					if (sitelinks_enwikiquote_site!=None):
						for i in decodejson['sitelinks']['enwikiquote']['badges']:
							cur.execute('replace into wikiBadges values(%s,%s,%s)',[id,"enwikiquote",i.encode("utf-8")])
					if (debug):
						print (4)
					if (sitelinks_enwikiversity_site!=None):
						for i in decodejson['sitelinks']['enwikiversity']['badges']:
							cur.execute('replace into wikiBadges values(%s,%s,%s)',[id,"enwikiversity",i.encode("utf-8")])
					if (debug):
						print (5)
					if (decodejson['aliases'].get('en',None)!=None):
						for i in decodejson['aliases']['en']:
							cur.execute('replace into aliases values(%s,%s,%s)',[id,i["language"],i["value"].encode("utf-8")])
					if (debug):
						print (6)
				elif (decodejson['type']=='property'):
					id = decodejson['id']
					labels_en_value = decodejson['labels']['en']['value'].encode("utf-8")
					labels_en_language = decodejson['labels']['en']['language'].encode("utf-8")
					if (debug):
						print (1)
					cur.execute('replace into property values(%s,%s,%s)', [id,labels_en_value,labels_en_language])
					if (debug):
						print (2)
					if (decodejson['aliases'].get('en',None)!=None):
						for i in decodejson['aliases']['en']:
							cur.execute('replace into aliases values(%s,%s,%s)',[id,i["language"],i["value"].encode("utf-8")])
					if (debug):
						print (3)
				else:
					print ('line: '+str(cnt))
			except Exception, e:
				if(cnt==1):
					continue
				print ('error in line '+str(cnt),str(e))
				continue
		f.close()
		cur.close()
		conn.close()
	except MySQLdb.Error,e:
		print "Mysql Error %d: %s" % (e.args[0], e.args[1])

def read_object(path,mode=1,output=None):
	cnt = 0
	f = open(path)
	f_output = None
	if (mode==2 and output!=None):
		f_output = open(output,'w')
	for line in f:
		if (str(line) > 0 and (line[-1] == '\n' or line[-1]=='\r')):
			line = line[:-1]
		if (str(line) > 0 and (line[-1] == '\n' or line[-1]=='\r')):
			line = line[:-1]
		if (str(line) > 0 and (line[-1] == ',')):
			line = line[:-1]
		cnt += 1
		if (cnt==3):
			break
		try:
			decodejson = json.loads(line)
			dfs(decodejson,0,mode,f_output)
		except Exception, e:
			if(cnt==1):
				continue
			print ('error in line '+str(cnt)+str(e))
			continue
	f.close()
	if (mode==2 and output!=None):
		f_output.close()

if __name__ == '__main__':
	#read_object(path=path_property,mode=2,output=os.path.join(target,'propertyDataType.txt'))
	#read_en_object(path=path_item,mode=1,output=None)
	read_object_detail(path=path1,debug=False,stop=12101)