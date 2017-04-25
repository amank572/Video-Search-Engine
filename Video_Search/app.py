import json
import os
from pprint import pprint
from pymongo import *
from flask import *
from py2neo import *
import pymysql.cursors
import collections
import time
from operator import itemgetter
# import functools

app = Flask(__name__)
app.secret_key = 'any random string'
app.jinja_env.globals.update(max=max)
authenticate("localhost:7474", "neo4j", "alwar301")
graph = Graph()


client = MongoClient()
db= client.vid
videos = db.videos

connection = pymysql.connect(host='localhost',user='root',password='1234',db='VIDEOS',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor);

"""
 - called whenever user watched a video
 - increaments the weight of the edge between user and video by one
 - stores latest viewed time as an attribute of edge
"""
def init_user(user_id ,ip_address, video_id):
	# print user_id , ip_address , video_id
	graph.run("MERGE(u:user{ _id:'" + user_id + "', ip_address:'" + ip_address + "' })")
	cursor = graph.run("MATCH (u:user)-[r:WATCHED]-(v:video) WHERE v._id='" + str(video_id) + "' AND u._id= '" + str(user_id) + "' RETURN (u)")
	views = 1
	isWatched = cursor.evaluate()
	if isWatched!=None:
		views = 1 + int(graph.run("MATCH (u:user)-[r:WATCHED]-(v:video) WHERE v._id='" + str(video_id) + "' AND u._id= '" + str(user_id) + "' RETURN r.views").evaluate())
		print("views=" + str(views))		
		graph.run("MATCH (v:video)-[r:WATCHED]-(u:user) WHERE v._id = '" + str(video_id) + "' AND u._id = '" + str(user_id) + "' SET r.views = '" + str(views) + "', r.time = timestamp() RETURN r.views");
	else:
		views = graph.run("MATCH (v:video), (u:user) WHERE v._id = '" + str(video_id) + "' AND u._id = '" + str(user_id) + "' CREATE (v)-[r:WATCHED { views:'" + str(views) + "',time : timestamp()}]->(u)");
		print("views=" + str(views))

def add_edge(vid1 , vid2):
	if vid1 == vid2:
		return
	# print user_id , ip_address , video_id
	cursor = graph.run("MATCH (v1:video)-[r:NEXT]-(v2:video) WHERE v1._id='" + str(vid1) + "' AND v2._id='" + str(vid2) + "' RETURN r.weight")
	weight = 1
	isNext = cursor.evaluate()
	if isNext!=None:
		weight = 1 + int(graph.run("MATCH (v1:video)-[r:NEXT]-(v2:video) WHERE v1._id='" + str(vid1) + "' AND v2._id='" + str(vid2) + "' RETURN r.weight").evaluate())
		graph.run("MATCH (v1:video)-[r:NEXT]-(v2:video) WHERE v1._id='" + str(vid1) + "' AND v2._id='" + str(vid2) + "' SET r.weight = '" + str(weight) + "' RETURN r.weight");
	else:
		views = graph.run("MATCH (v1:video),(v2:video) WHERE v1._id =  '" + str(vid1) +"' AND v2._id =  '" + str(vid2) +"' CREATE (v1)-[r2:NEXT { weight:'" + str(1) + "'}]->(v2)");

def cursor_to_list(C):
	return [rec for rec in C]

def neo2mongo2(ids):
	videos = []
	for v in ids:
		# print v
		for r in searchMongoById(v):
			videos.append(r)
	return videos

def uniqueit(l):
	ids = []
	videos = []
	for v in l:
		if v['videoInfo']['id'] not in ids:
			videos.append(v)
			ids.append(v['videoInfo']['id'])
	return videos


def test_related_videos(video_id,tag_weight=5,desc_weight=3,channel_weight=1,next_weight=2,title_weight=4 ,top_n=20):
	list1 = cursor_to_list(graph.run("MATCH (v1:video)-[r:COMMON_TAGS]-(v2:video) WHERE v1._id = '" + str(video_id) + "' return v2._id,r.weight ORDER BY r.weight DESC"))
	list2 = cursor_to_list(graph.run("MATCH (v1:video)-[r:COMMON_DESC]-(v2:video) WHERE v1._id = '" + str(video_id) + "' return v2._id,r.weight ORDER BY r.weight DESC"))
	list3 = cursor_to_list(graph.run("MATCH (v1:video)-[r:SAME_CHANNEL]-(v2:video) WHERE v1._id = '" + str(video_id) + "' return v2._id,1"))
	list4 = cursor_to_list(graph.run("MATCH (v1:video)-[r:NEXT]-(v2:video) WHERE v1._id = '" + str(video_id) + "' return v2._id,r.weight ORDER BY r.weight DESC"))
	list5 = cursor_to_list(graph.run("MATCH (v1:video)-[r:COMMON_TITLE]-(v2:video) WHERE v1._id = '" + str(video_id) + "' return v2._id,r.weight ORDER BY r.weight DESC"))
	D = {}
	for r in list1:
		if r['v2._id'] not in D.keys():
			D[r['v2._id']] = float(r['r.weight'])*float(tag_weight)
		else:
			D[r['v2._id']] += float(r['r.weight'])*float(tag_weight)
	for r in list2:
		if r['v2._id'] not in D.keys():
			D[r['v2._id']] = float(r['r.weight'])*float(desc_weight)
		else:
			D[r['v2._id']] += float(r['r.weight'])*float(desc_weight)
	for r in list3:
		if r['v2._id'] not in D.keys():
			D[r['v2._id']] = float(1)*float(channel_weight)
		else:
			D[r['v2._id']] += float(1)*float(channel_weight)
	for r in list4:
		if r['v2._id'] not in D.keys():
			D[r['v2._id']] = float(r['r.weight'])*float(next_weight)
		else:
			D[r['v2._id']] += float(r['r.weight'])*float(next_weight)
	for r in list5:
		if r['v2._id'] not in D.keys():
			D[r['v2._id']] = float(r['r.weight'])*float(title_weight)
		else:
			D[r['v2._id']] += float(r['r.weight'])*float(title_weight)
	final = [[key,D[key]] for key in D.keys()]
	final.sort(key=lambda k: k[1], reverse=True)
	for r in final:
		print r[1]
	return [x[0] for x in final[0:top_n]]

def recommendations(user_id,n=10):
	if 'username' in session:
		hist = cursor_to_list(graph.run("MATCH (u:user)-[r:WATCHED]-(v2:video) WHERE u._id = '"+session['username']+"' RETURN v2._id ORDER BY r.time DESC"))
	
	print hist
	big_record = []
	for h in hist:
		big_record.extend(test_related_videos(h['v2._id']))
	top_n = [x[0] for x in collections.Counter(big_record).most_common(n)]
	print top_n
 	videos = []
	for id in top_n:
		print "unique " + id
		if len(list(searchMongoById(id))):
			videos.extend(list(searchMongoById(id)))
 	return videos

def same_session_vids(video_id):
	return cursor_to_list(graph.run("MATCH (v1:video)-[r:SAME_SESSION]-(v2:video) WHERE v1._id = '" + str(video_id) + "' AND r.weight > 0 RETURN v2._id ORDER BY r.weight"))	

def add_session_edge(video_id_prev,video_id_cur):
	views = 1 + int(graph.run("MATCH (v1:video)-[r:SAME_SESSION]-(v2:video) WHERE v1._id='" + str(video_id_prev) + "' AND v2._id= '" + str(video_id_cur) + "' RETURN r.weight").evaluate())
	graph.run("MATCH (v1:video)-[r:SAME_SESSION]-(v2:video) WHERE v1._id = '" + str(video_id_prev) + "' AND v2._id = '" + str(video_id_cur) + "' SET r.weight = " + str(views) + " RETURN r.weight")

def compare(i1,i2):
	if i1['score'] < i2['score']:
		return 1;
	elif i1['score'] > i2['score']:
		return -1;
	else:
		return 0;

def searchMongo(query):
	res = db.videos.find({"$text": {"$search": query}}, {"score": {"$meta": "textScore"}})
	result = []
	for r in res:
		result.append(r)
	result = sorted(result, cmp = compare)
	# result = sorted(result, key=functools.cmp_to_key(compare))
	return result

def searchMongoById(id):
	return db.videos.find({"videoInfo.id": id})	

@app.route('/', methods = ['POST', 'GET', 'PUT'])
def home():
	logged_in = True
	user = ""
	trending = []
	subscriptions = []
	recommended = []
	playlist = []
	if "username" not in session:
		logged_in = False
	else:
		user = session['username']
		subscriptions = getSubsribedChannelVids(session['username'])
		recommended = recommendations(session['username'],20)	
		playlist = cursor_to_list( graph.run("MATCH (p:playlist) WHERE p.user = '" + session['username']  + "' RETURN p.name "))
	# Pass [logged_in, username, trending, subscriptions, recommended]		
	trending = recent()
	return render_template('index1.html', result = [logged_in, user, uniqueit(trending), uniqueit(subscriptions), uniqueit(recommended), False,playlist])

@app.route('/search', methods = ['POST', 'GET'])
def search():
	if 'curr_video' in session:
		session.pop('curr_video',None)
	logged_in = True
	user = ""
	trending = []
	subscriptions = []
	recommended = []
	playlist = []
	if "username" not in session:
		logged_in = False
	else:
		user = session['username']	
		subscriptions = getSubsribedChannelVids(session['username'])
		recommended = recommendations(session['username'],20)
		playlist = cursor_to_list( graph.run("MATCH (p:playlist) WHERE p.user = '" + session['username']  + "' RETURN p.name "))
	# Pass [logged_in, username, trending, subscriptions, recommended]		
	if request.method == 'POST':
		query = request.form['query']
		session['query'] = query
		result = list(searchMongo(query))
		for i in range(0,len(result)):
			result[i]['rank'] = i+1
		return render_template('search_results.html', result = [logged_in, user, uniqueit(result),playlist])
	else:
		trending = recent()
		print trending
		return render_template('index1.html', result = [logged_in, user, uniqueit(trending), uniqueit(subscriptions), uniqueit(recommended),False, playlist])

def neo2mongo(neo4j_videos):
	videos = []
	for v in neo4j_videos:
		# print v
		for r in searchMongoById(v["v2._id"]):
			videos.append(r)
	return videos

def verify_user(username,password):
	c = connection.cursor()
	sql = "SELECT `username`, `password` FROM `users` WHERE `username`=%s"
	c.execute(sql, (username,))
	L = list(c);
	if len(L) > 0:
		if L[0]['password'] == password:
			session['username'] = username
			return 1
		else:
			return 0
	else:
		return 2


@app.route('/register', methods = ['POST', 'GET'])
def register():
	# print "in register"
	playlist = []
	if request.method == 'POST':
		username = request.form['username']
		password = request.form['password']
		valid = create_user(username,password)
		if valid == 0 or valid == 5:
			playlist = []
			if 'username' in session:
				playlist = cursor_to_list( graph.run("MATCH (p:playlist) WHERE p.user = '" + session['username']  + "' RETURN p.name "))	
			return render_template('/index1.html',result = [False, "", uniqueit(recent()),[],[],True, playlist])
		if valid == 10:
			session['username'] = username
		return redirect("/")

@app.route('/signOut', methods = ['POST', 'GET'])
def signout():
	session.pop('username',None)
	return redirect("/")

def create_user(username,password):
	c = connection.cursor()
	valid = verify_user(username,password)
	if valid == 0:
		return 0
	if valid == 2:
		sql = "INSERT INTO `users` (`username`, `password`) VALUES (%s,%s)"
		c.execute(sql, (username,password))
		connection.commit()
		return 10
	else:
		if 'username' in session:
			session.pop('username',None)	
		return 5

@app.route('/store', methods = ['POST', 'GET'])
def store():
	if request.method == 'GET':
		user_id = ""
		if 'username' in session:
			user_id = session['username']
		video_id = request.args.get('_id')
		rank = request.args.get('rank')
		query_clicked(user_id,video_id,rank,session['query'])
		return redirect("/video?_id="+str(video_id))



@app.route('/video', methods = ['POST', 'GET'])
def video():
	user = ""
	logged_in = False
	playlist = []
	if 'username' in session:
		logged_in = True
		user = session['username']
	if request.method == 'GET':
		video_id = request.args.get('_id')
		videos.update({"videoInfo.id": video_id}, {"$inc": {"videoInfo.statistics.viewCount":1}})
		if 'curr_video' in session:
			add_edge(session['curr_video'],video_id)
		session['curr_video'] = video_id
		clicked(video_id)
		current_video = list(searchMongoById(video_id))[0]
		subscribed = False
		liked = False
		disliked = False
		if logged_in:
			init_user(session['username'],request.remote_addr,video_id)
			L = list(graph.run("MATCH (u:user)-[r:SUBSCRIBED]-(c:channel)  WHERE u._id = '"+ str(session['username']) +"' AND c.id = '"+ str(current_video['videoInfo']['snippet']['channelId']) +"' RETURN r"))
			if len(L) > 0:
				subscribed = True
			playlist = cursor_to_list( graph.run("MATCH (p:playlist) WHERE p.user = '" + session['username']  + "' RETURN p.name "))
			liked = len(list(graph.run("MATCH (u:user)-[r:LIKED]-(v:video) where u._id = '" + str(session['username']) + "' AND v._id = '" + str(video_id) + "' RETURN r")))
			disliked = len(list(graph.run("MATCH (u:user)-[r:DISLIKED]-(v:video) where u._id = '" + str(session['username']) + "' AND v._id = '" + str(video_id) + "' RETURN r")))
		related_video = neo2mongo2(test_related_videos(video_id))
			 # **********
		comments = comment_list(video_id)
		return render_template('single.html', result = [logged_in,user,current_video, uniqueit(related_video),subscribed, playlist,liked,disliked,comments])
	else:
		return render_template('video.html', result = [])


@app.route('/subscribe', methods = ['POST', 'GET'])
def subscribe_it():
	channel = request.args.get('channel')
	status = request.args.get('status')
	if status == "Subscribe":
		subscribe(session['username'],channel)
	else:
		unsubscribe(session['username'],channel)
	return "resp"



@app.route('/unsubscribe', methods = ['POST', 'GET'])
def unsub():
	user = ""
	logged_in = False
	playlist = []
	if 'username' in session:
		logged_in = True
		user = session['username']
		playlist = cursor_to_list( graph.run("MATCH (p:playlist) WHERE p.user = '" + session['username']  + "' RETURN p.name "))
	if request.method == 'GET':
		video_id = request.args.get('_id')
		current_video = list(searchMongoById(video_id))[0]
		subscribed = False
		unsubscribe(user, current_video['videoInfo']['snippet']['channelId'])
		related_video = neo2mongo2(test_related_videos(video_id))
		return render_template('single.html', result = [logged_in,user,current_video, uniqueit(related_video),subscribed, playlist])
	else:
		return render_template('video.html', result = [])

@app.route('/add_comment', methods = ['POST', 'GET'])
def add_com():
	c = request.form['comment']
	db = pymysql.connect("localhost","root","1234","VIDEOS")
	cursor = db.cursor()
	sql = "INSERT INTO `comments` ( `video_id`, `user_id`, `comment`, `timestamp` ) VALUES (%s,%s,%s,%s);"
	print cursor.execute(sql, (session['username'],session['curr_video'],str(c), str(time.time()),))
	return redirect("/video?_id=" + session['curr_video'])

@app.route('/login', methods = ['POST', 'GET'])
def login():
	user = ""
	trending = []
	subscriptions = []
	recommended = []
	playlist = []
	if request.method == 'POST':
		username = request.form['username']
		password = request.form['password']
		valid = verify_user(username,password)
		if valid != 1:
			return redirect("/")
		else:
			recommended = recommendations(session['username'],20)
			subscriptions = getSubsribedChannelVids(session['username'])
			playlist = cursor_to_list( graph.run("MATCH (p:playlist) WHERE p.user = '" + session['username']  + "' RETURN p.name "))
			return render_template('index1.html', result = [True,session['username'],uniqueit(recent()),uniqueit(subscriptions),uniqueit(recommended), False,playlist])
	else:
		return render_template('login.html', result = [])

@app.route('/history', methods = ['POST', 'GET'])
def history():
	if 'username' not in session:
		return redirect("/")
	playlist = cursor_to_list( graph.run("MATCH (p:playlist) WHERE p.user = '" + session['username']  + "' RETURN p.name "))
	hist = neo2mongo( graph.run("MATCH (u:user)-[r:WATCHED]-(v2:video) WHERE u._id = '"+session['username']+"' RETURN v2._id ORDER BY r.time DESC"))
	return render_template('search_results.html', result = [True, session['username'], uniqueit(hist), playlist,True])


@app.route('/playlist', methods = ['POST', 'GET'])
def load_playlist():
	if 'username' not in session:
		return redirect("/")
	playlist_name = request.args.get('name')
	playlist = cursor_to_list( graph.run("MATCH (p:playlist) WHERE p.user = '" + session['username']  + "' RETURN p.name "))
	query = "MATCH (p:playlist)-[r:ADDED]-(v2:video) where p.name = '" + str(playlist_name) + "' AND p.user = '" + str(session['username'])  + "' RETURN v2._id"
	videos_in = neo2mongo(graph.run(query))
	return render_template('search_results.html', result = [True, session['username'], uniqueit(videos_in), playlist,False, playlist_name])


def recent():
	return neo2mongo( graph.run("MATCH (u:user)-[r:WATCHED]-(v2:video) RETURN v2._id ORDER BY r.time DESC LIMIT 25"))

def subscribe(user_id, channel_id):
	graph.run("MATCH (u:user), (c:channel) where u._id = '" + str(user_id) + "' AND c.id = '" + str(channel_id) + "' CREATE (u)-[r1:SUBSCRIBED]->(c)")

def unsubscribe(user_id, channel_id):
	graph.run("MATCH (u:user)-[r:SUBSCRIBED]-(c:channel) where u._id = '" + str(user_id) + "' AND c.id = '" + str(channel_id) + "' DELETE r")

def getSubsribedChannelVids(user_id):
	channels = list(graph.run("MATCH (u:user)-[r:SUBSCRIBED]-(c:channel) where u._id = '" + str(user_id) + "'  RETURN c.id"))
	L = []
	ids = []
	for c in channels:
		l = list(videos.find({"videoInfo.snippet.channelId": c['c.id']}))
		for v in l:
			if v['videoInfo']['id'] not in ids:
				v['score'] = v['videoInfo']['statistics']['viewCount']
				ids.append(v['videoInfo']['id'])
				L.append(v)
	return sorted(L,cmp=compare) 
	
# creates a node playlist{name, user}
# creates a relationship "ADDED" between video and user's playlist

# schema  search(user_id, video_id, timestamp, rank)
# schema  clicks(video_id, timestamp)

def clicked(video_id):
	db = pymysql.connect("localhost","root","1234","VIDEOS")
	cursor = db.cursor()
	cursor.execute("CREATE TABLE IF NOT EXISTS clicks (video_id "  + "VARCHAR(255), timestamp INT);")
	cursor.execute("INSERT INTO" + " clicks (video_id, "+ "timestamp) VALUES ('" + str(video_id) + "', " + str(time.time()) + ");")
	db.commit()
	db.close()

def comment_list(video_id):
	db = pymysql.connect("localhost","root","1234","VIDEOS")
	cursor = db.cursor(pymysql.cursors.DictCursor)
	sql = "SELECT `user_id`, `comment` FROM `comments` WHERE `video_id`=%s ORDER BY"  + " timestamp DESC"
	cursor.execute(sql, (video_id,))
	return list(cursor)
	db.close()

def query_clicked(user_id, video_id, rank, query):
	db = pymysql.connect("localhost","root","1234","VIDEOS")
	cursor = db.cursor()
	cursor.execute("CREATE TABLE IF NOT EXISTS search_stats (user_id" + " VARCHAR(255), video_id VARCHAR(255), timestamp INT, rank INT, query " +" VARCHAR(255));")
	sql = "INSERT INTO" + " search_stats (user_id, video_id, timestamp, rank, query) VALUES ('" + str(user_id) + "', '" + str(video_id) + "', " + str(time.time()) + ", " + str(rank) + ", '" + str(query) + "');"
	print sql
	cursor.execute(sql)
	db.commit()
	db.close()


@app.route('/like_it', methods = ['POST', 'GET'])
def like():
	video_id = request.args.get('current_video')
	status = request.args.get('current_video')
	video = list(searchMongoById(video_id))[0]
	if status == "Unlike":
		graph.run("MATCH (u:user), (v:video) where u._id = '" + str(session['username']) + "' AND v._id = '" + str(video_id) + "' CREATE (u)-[r1:LIKED]->(v)")
		videos.update({"videoInfo.id": video_id}, {"$set": {"videoInfo.statistics.likeCount": str(int(video['videoInfo']['statistics']['likeCount']) - 1) }})
	else:
		graph.run("MATCH (u:user)-[r:LIKED]-(v:video) where u._id = '" + str(session['username']) + "' AND v._id = '" + str(video_id) + "' DELETE r")
		videos.update({"videoInfo.id": video_id}, {"$set": {"videoInfo.statistics.likeCount":str(int(video['videoInfo']['statistics']['likeCount']) + 1)}})
	return "response"

@app.route('/dislike_it', methods = ['POST', 'GET'])
def dislike():
	video_id = request.args.get('current_video')
	status = request.args.get('current_video')
	if status == "Undislike":
		graph.run("MATCH (u:user), (v:video) where u._id = '" + str(session['username']) + "' AND v._id = '" + str(video_id) + "' CREATE (u)-[r1:DISLIKED]->(v)")
		
		videos.update({"videoInfo.id": video_id}, {"$inc": {"videoInfo.statistics.dislikeCount":-1}})
	else:
		graph.run("MATCH (u:user)-[r:DISLIKED]-(v:video) where u._id = '" + str(session['username']) + "' AND v._id = '" + str(video_id) + "' DELETE r")
		videos.update({"videoInfo.id": video_id}, {"$inc": {"videoInfo.statistics.dislikeCount":1}})
	return "response"

# @app.route('/unlike', methods = ['POST', 'GET'])
# def unlike():
# 	if request.method == 'GET':
# 		video_id = request.args.get('current_video')
	
# 	print(video_id + "inside unlike()")
# 	return "response"

# @app.route('/dislike', methods = ['POST', 'GET'])
# def dislike():
# 	if request.method == 'GET':
# 		video_id = request.args.get('current_video')
# 	graph.run("MATCH (u:user), (v:video) where u._id = '" + str(session['username']) + "' AND v._id = '" + str(video_id) + "' CREATE (u)-[r1:DISLIKED]->(v)")
# 	print(video_id + "inside unlike()")
# 	return "response"

# @app.route('/undislike', methods = ['POST', 'GET'])
# def undislike(): 
# 	if request.method == 'GET':
# 		video_id = request.args.get('current_video')
# 	graph.run("MATCH (u:user)-[r:DISLIKED]-(v:video) where u._id = '" + str(session['username']) + "' AND v._id = '" + str(video_id) + "' DELETE r")
# 	print(video_id + "inside unlike()")
# 	return "response"

# creates a node playlist{name, user}
@app.route('/create_playlist', methods = ['POST', 'GET'])
def create_playlist():
	if request.method == 'GET':
		playlist_name = request.args.get('playlist_name')
		current_video = request.args.get('current_video')
	playlists = [x['p.name'] for x in cursor_to_list( graph.run("MATCH (p:playlist) WHERE p.user = '" + session['username']  + "' RETURN p.name "))]
	if playlist_name not in playlists:
		graph.run("CREATE (p:playlist{ name: '" + str(playlist_name) + "', user: '" + str(session['username']) + "' })")
	query = "MATCH (p:playlist)-[r:ADDED]-(v:video) where p.name = '" + str(playlist_name) + "' AND p.user = '" + str(session['username'])  + "' AND v._id = '" + str(current_video) + "' RETURN v._id"
	if len(list(graph.run(query))) == 0:
		add_to_playlist(playlist_name, current_video)
	return "response"

# creates a relationship "ADDED" between video and user's playlist
def add_to_playlist(playlist_name, video_id):
	query = "MATCH (u:user), (p:playlist), (v:video) where u._id = '" + str(session['username']) + "' AND p.name = '" + str(playlist_name) + "' AND p.user = '" + str(session['username'])  + "' AND v._id = '" + str(video_id) + "' CREATE (p)-[r:ADDED]->(v)"
	graph.run(query)



if __name__ == '__main__':
	app.run(host='0.0.0.0',debug=True)