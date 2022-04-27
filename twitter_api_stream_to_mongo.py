import json
import sys
import socket
from datetime import datetime
import time
import requests
import os
import threading
import pymongo



# To set the bearer token in the environment variables:
# https://www.toptal.com/apache/apache-spark-streaming-twitter
# https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/Filtered-Stream/filtered_stream.py
# safer option is to register it as an environment variable
# bearer_token = os.environ.get("BEARER_TOKEN")

bearer_token = "<<TOKEN>>"

# Mongo DB Database Connection Details
#mongoclient = pymongo.MongoClient("mongodb://localhost:27017/")
mongoclient = pymongo.MongoClient("mongodb://USER:PASSWORD@IP:PORT/?authSource=test")
mydb = mongoclient["twitter_streaming_data"]
mycol = mydb["tweets"]

# stream start
stream_start_time = datetime.now()

def bearer_oauth(r):
	"""
	Method required by bearer token authentication.
	"""

	r.headers["Authorization"] = f"Bearer {bearer_token}"
	r.headers["User-Agent"] = "v2FilteredStreamPython"
	return r


def set_rules(delete):
	# Define three rules for the filtered streaming:
	#	- Filter for tweets from the russia/ukraine conflict
	#	- Filter for tweets regarding climate change
	#	- Filter for tweets regarding covid-19
	sample_rules = [
		{"value": "(Russland OR Ukraine OR Putin OR Selenski OR Lwiw OR Kiew OR Mariupol OR Klitschko OR Lawrow) lang:de -is:retweet", "tag": "Russland"},
		{"value": "(Klima OR Klimawandel OR Energiepolitik OR erneuerbar OR Solaranlage OR Klimaerwärmung) lang:de -is:retweet", "tag": "Klimawandel"},
		{"value": "(Covid OR Corona OR SARS-CoV-2 OR Omikron OR Impfplicht OR Impfung) lang:de -is:retweet", "tag": "Covid"}
	]
	payload = {"add": sample_rules}
	response = requests.post(
		"https://api.twitter.com/2/tweets/search/stream/rules",
		auth=bearer_oauth,
		json=payload,
	)
	if response.status_code != 201:
		raise Exception(
			"Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
		)
	print(json.dumps(response.json()))


def get_stream(set):
	print(str(datetime.now()) + " - [THREAD ID] " + str(threading.get_ident()))
	response = requests.get(
	"https://api.twitter.com/2/tweets/search/stream?tweet.fields=author_id,created_at,source,entities", 
	auth=bearer_oauth, 
	stream=True,
	)
	print(str(datetime.now()) + " - HTTP Response Status for Stream Definition: ",response.status_code)
	if response.status_code != 200:
		raise Exception(
			"Cannot get stream (HTTP {}): {}".format(
				response.status_code, response.text
			)
		)
	
	print(str(datetime.now()) + " - Start reading Tweets from Stream...")
	count = 0
	# Iterate through each streamed tweet
	for response_line in response.iter_lines():
		if response_line:
			try:
				full_tweet = json.loads(response_line)
				#tweet_json_string = json.dumps(full_tweet)
				# Useful for debugging purposes
				# print("Tweet Text: " + tweet_json_string)
				#print ("------------------------------------------")
				mycol.insert_one(full_tweet)
				count += 1
				if count % 100 == 0:
					print(str(datetime.now()) + " - Successfully processed " + str(count) + " Tweets.")
			except:
				e = sys.exc_info()[0]
				print("%s - Error: %s" % str(datetime.now()),e)

	if response.raw.closed:
		# Disconnect has happened
		print("%s - Error... Stream was forcibly closed by Twitter: %s. Raising Exception now!" 
				% str(datetime.now()),response.raw.closed)
		raise Exception

	

		

def get_rules():
	response = requests.get(
		"https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
	)
	if response.status_code != 200:
		raise Exception(
			"Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
		)
	print(json.dumps(response.json()))
	return response.json()


def delete_all_rules(rules):
	if rules is None or "data" not in rules:
		return None

	ids = list(map(lambda rule: rule["id"], rules["data"]))
	payload = {"delete": {"ids": ids}}
	response = requests.post(
		"https://api.twitter.com/2/tweets/search/stream/rules",
		auth=bearer_oauth,
		json=payload
	)
	if response.status_code != 200:
		print(json.dumps(response.json()))
		raise Exception(
			"Cannot delete rules (HTTP {}): {}".format(
				response.status_code, response.text
			)
		)
	

def main():
	rules = get_rules()
	delete = delete_all_rules(rules)
	set = set_rules(delete)
	print(str(stream_start_time) + " - [STARTING] start time")
	while(True):
		try:
			print(str(datetime.now()) + " - [STARTING] stream is starting in new thread...")
			thread = threading.Thread(target=get_stream(set))
			thread.start()
		except Exception as e:
			print(str(datetime.now()) + " - Caught the exception: " + str(e) 
					+ ". Wait for 30 seconds and try again.")
			#thread.join()
			time.sleep(30)

if __name__ == "__main__":
	main()


