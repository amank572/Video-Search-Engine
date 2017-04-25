import pymysql
import time

# schema  search(user_id, video_id, timestamp, rank)
# schema  clicks(video_id, timestamp)

def clicked(video_id):
	db = pymysql.connect("localhost","root","1234","VIDEOS")
	cursor = db.cursor()
	cursor.execute("CREATE TABLE IF NOT EXISTS clicks (video_id "  + "VARCHAR(255), timestamp INT);")
	cursor.execute("INSERT INTO" + " clicks (video_id, "+ "timestamp) VALUES ('" + str(video_id) + "', " + str(time.time()) + ");")
	db.commit()
	db.close()

def query_clicked(user_id, video_id, rank):
	db = pymysql.connect("localhost","root","1234","VIDEOS")
	cursor = db.cursor()
	cursor.execute("CREATE TABLE IF NOT EXISTS search (user_id" + " VARCHAR(255), video_id VARCHAR(255), timestamp INT, rank INT);")
	cursor.execute("INSERT INTO " + " search (user_id, video_id, timestamp, rank) VALUES ('" + str(user_id) + "', '" + str(video_id) + "', " + str(time.time()) + ", " + str(rank) + ");")
	db.commit()
	db.close()