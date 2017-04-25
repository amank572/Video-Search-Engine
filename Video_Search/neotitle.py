import os;
import json;
from py2neo import *;
import nltk

files = os.listdir(os.getcwd() + '/test');

authenticate("localhost:7474", "neo4j", "alwar301")
graph = Graph();

data = [];

for f in files:
	data_file = open(os.getcwd() + '/test/' + f);
	data.append(json.load(data_file));
	data_file.close();

for i in range(0,len(data)-1):
	print(i);
	for j in range(i+1,len(data)):
		if i == j:
			continue;
		print i,j
		titi = data[i]['videoInfo']['snippet']['title'].split();
		titj = data[j]['videoInfo']['snippet']['title'].split();
		[x.lower() for x in titi];
		[x.lower() for x in titj];
		common_tit = len(set(titi).intersection(set(titj)));
		if common_tit:
			graph.run("MATCH (v1:video), (v2:video) WHERE v1._id = '" + str(data[i]['videoInfo']['id']) + "' AND v2._id = '" + str(data[j]['videoInfo']['id']) + "' CREATE (v1)-[r3:COMMON_TITLE {weight:" + str(common_tit) + "}]->(v2);");