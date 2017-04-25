import os;
import json;
from py2neo import *;
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords
from nltk.stem import *

stops = stopwords.words('english')
stemmer = SnowballStemmer("english")

files = os.listdir(os.getcwd() + '/test');

authenticate("localhost:7474", "neo4j", "alwar301")
graph = Graph();

data = [];

for f in files:
	data_file = open(os.getcwd() + '/test/' + f);
	data.append(json.load(data_file));
	data_file.close();

graph.run("MATCH(v) DETACH DELETE v");

def transform(i,rstop,string):
	if string == "tags":
		i = data[i]['videoInfo']['snippet'][string]
	else:
		i = data[i]['videoInfo']['snippet'][string].split()
	i = [x.lower() for x in i]
	i = [stemmer.stem(x) for x in i]
	if rstop:
		i = [x for x in i if x in stops]
		return i
	else:
		return i

def compute_common(i, j, string):
	common_s = len(set(transform(i,1, string)).intersection(set(transform(j,1, string))))
	common_ws = len(set(transform(i,0, string)).intersection(set(transform(j,0, string))))
	return common_ws + float(common_s)/4

channels = []
cids = []
for i in range(0,len(data)):
	d = data[i];
	if d['videoInfo']['snippet']['channelId'] not in cids:
		cids.append(d['videoInfo']['snippet']['channelId'])
		channels.append(d['videoInfo']['snippet']['channelTitle'])
	graph.run("CREATE (v:video { _id:'" + str(d['videoInfo']['id']) + "',commentCount:" + str(d['videoInfo']['statistics']['commentCount']) + ", viewCount:" + str(d['videoInfo']['statistics']['viewCount']) + ", favoriteCount:" +  str(d['videoInfo']['statistics']['favoriteCount']) + ", dislikeCount:" + str(d['videoInfo']['statistics']['dislikeCount']) + ", likeCount:" + d['videoInfo']['statistics']['likeCount'] + "})" );

for i in range(0,len(channels)):
	graph.run("CREATE (c:channel { id:'" + str(cids[i]) + "', channelTitle:'" + str(channels[i]) + "' })")


for i in range(0,len(data)-1):
	for j in range(i+1,len(data)):
		if i == j:
			continue;
		print i," ",j
		same_channel = data[i]['videoInfo']['snippet']['channelId'] == data[j]['videoInfo']['snippet']['channelId'];
		common_tags = 1;
		if 'tags' in data[i]['videoInfo']['snippet'].keys() and 'tags' in data[j]['videoInfo']['snippet'].keys():
			common_tags = compute_common(i,j,'tags') 
		else:
			common_tags = 0;
		common_desc = common_tags = compute_common(i,j,'description')
		common_tit = common_tags = compute_common(i,j,'title')
		w = 0
		if same_channel:
			graph.run("MATCH (v1:video), (v2:video) WHERE v1._id = '" + str(data[i]['videoInfo']['id']) + "' AND v2._id = '" + str(data[j]['videoInfo']['id']) + "' CREATE (v1)-[r1:SAME_CHANNEL]->(v2);");
			w = w + 1		
		if common_tags:
			graph.run("MATCH (v1:video), (v2:video) WHERE v1._id = '" + str(data[i]['videoInfo']['id']) + "' AND v2._id = '" + str(data[j]['videoInfo']['id']) + "' CREATE (v1)-[r2:COMMON_TAGS {weight:" + str(common_tags) + "}]->(v2);");
			w = w + 2*common_tags
		if common_desc:
			graph.run("MATCH (v1:video), (v2:video) WHERE v1._id = '" + str(data[i]['videoInfo']['id']) + "' AND v2._id = '" + str(data[j]['videoInfo']['id']) + "' CREATE (v1)-[r3:COMMON_DESC {weight:" + str(common_desc) + "}]->(v2);");
			w = w + 1*common_desc
		if common_tit:
			graph.run("MATCH (v1:video), (v2:video) WHERE v1._id = '" + str(data[i]['videoInfo']['id']) + "' AND v2._id = '" + str(data[j]['videoInfo']['id']) + "' CREATE (v1)-[r3:COMMON_TITLE {weight:" + str(common_tit) + "}]->(v2);");
			w = w + 3*common_tit
		if w:
			graph.run("MATCH (v1:video), (v2:video) WHERE v1._id = '" + str(data[i]['videoInfo']['id']) + "' AND v2._id = '" + str(data[j]['videoInfo']['id']) + "' CREATE (v1)-[r3:RELATED_SCORE {weight:" + str(w) + "}]->(v2);");