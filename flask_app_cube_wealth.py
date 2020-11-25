# import flask dependencies
from flask import Flask,request
import json
import pdb
import mysql.connector
import sqlite3 as sql
from flask_pymongo import PyMongo
import json
import logging

#from googlesearch import search
#from pymongo import MongoClient

# initialize the flask app
app = Flask(__name__)
import requests




#initializing loggr object and file
logger = logging.getLogger('myapp')
hdlr = logging.FileHandler('/var/tmp/myapp.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.WARNING)


app = Flask(__name__)


# creating connection with mongo db to insert our event
app.config['MONGO_DBNAME'] = 'event_collect'
app.config['MONGO_URI'] = 'mongodb://localhost:27017/event_collect'

mongo = PyMongo(app)




# default route
@app.route('/')
def hello():
    return """Hello ,
	    World!"""




# Method to send push notification, if the event falls under some pre specified rule
def rule_engine(event,event_db):
	user_id = event.get('userid')
	if event_db.find({'userid':user_id,'verb':'pay'}).count() == 1:
		logger.info('Hurray! First Ever Pay')

	if not event.get('properties').get('text'):
		logger.info('Aah! No Feedback Given')


@app.route('/event_ing/')
def event_handler():

	#taking out data from event call
	req_map = request.args.to_dict()
	event_a = req_map.get('event_ingest')
	

	event_db = mongo.db.event_data
	
	#converting json data  dictionary object
	event_a = json.loads(event_a)
	
	#inserting event into our NOSQL DB - MongDB 
	event_id = event_db.insert_one(event_a)

	"""For Push Notification, sending event data to rule engine
	to check, if the data falls into any of the rule specified
	"""
	rule_engine(event_a,event_db)

	# returning success - 200, if all goes well, else 500
	return 'Success'


# run the app
if __name__ == '__main__':
   app.run()


