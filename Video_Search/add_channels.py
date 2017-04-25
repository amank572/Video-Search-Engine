import os;
from py2neo import *;
from pymongo import *

authenticate("localhost:7474", "neo4j", "alwar301")
graph = Graph();

client = MongoClient()
db = client['vid']
collection = db['videos']

datas = collection.find()
channels = {}

for d in datas:
	channels[d['videoInfo']['snippet']['channelId']]  = d['videoInfo']['snippet']['channelTitle']

for c in channels.keys():
	graph.run("CREATE (c:channel { id:'" + str(c) + "', channelTitle:'" + str(channels[c]) + "' })")