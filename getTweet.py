import requests
import json
from pymongo import MongoClient


class MongoDB(object):
    def __init__(self, host='localhost', port=27017, database_name=None, collection_name=None):
        try:
            self._connection = MongoClient(host=host, port=port, maxPoolSize=200)
        except Exception as error:
            raise Exception(error)
        self._database = None
        self._collection = None
        if database_name:
            self._database = self._connection[database_name]
        if collection_name:
            self._collection = self._database[collection_name]

    def __readFromUrl(self, url):
        response = requests.get(url)
        if response.status_code != 200:
            print('Failed to get data:', response.status_code)
        data = response.text
        return data

    def __checkExist(self, id):
        cnt = self._collection.count_documents({"id": id})
        return (cnt == 0)

    def insert(self, url, tweetNum):
        count = self._collection.estimated_document_count()
        while(count != tweetNum):
            print("Count:::", count)
            result = json.loads(self.__readFromUrl(url))
            data = result["items"]
            for tweet in data:
                doesNotExist = self.__checkExist(tweet["id"])
                if  doesNotExist:
                    self._collection.insert_one(tweet)
                    count += 1
        return "%d added to tweets DB", count

    def findUserWithMostTweets(self):
        sortedCntUsers = list(self._collection.aggregate([
            {"$sortByCount": "$senderUsername"}
        ]))
        return sortedCntUsers[0]

    def getTweets(self, username):
        tweets = list(self._collection.find({"senderUsername" : username}))
        for tweet in tweets:
            print(tweet["content"],"\n")


url = 'https://www.sahamyab.com/guest/twiter/list?v=0.1'
mongo_db = MongoDB(database_name='SahamTweets', collection_name='tweets')
mongo_db.insert(url, 150)
# a = mongo_db.findUserWithMostTweets()
# print(a)
mongo_db.getTweets('gharibzahedi')