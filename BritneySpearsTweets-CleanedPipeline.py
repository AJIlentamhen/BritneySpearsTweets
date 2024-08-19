import os
import pandas as pd
from pymongo import MongoClient, ASCENDING
from datetime import datetime

df = pd.read_csv("Copy of correct_twitter_202102.tsv", delimiter = "\t")

sorted_cols_missing_values = df.isna().sum()
df = df.drop(["mentioned_author_ids", "mentioned_handles"], axis=1)

threshold = 46
cols_not_unique_enough = df.columns[df.nunique() < threshold]
df = df.drop(cols_not_unique_enough, axis=1)

df = df.rename(columns = {"id":"tweet_id", "ts1":"timestamp1", " ts2":"timestamp2",
                          "replied_to": "replied_to_tweet_id"})
df = df.replace(r"\s+", " ", regex=True)

df["timestamp1"] = pd.to_datetime(df["timestamp1"], errors = "coerce", utc = True)
df["timestamp2"] = pd.to_datetime(df["timestamp2"], errors = "coerce", utc = True)
df["created_at"] = pd.to_datetime(df["created_at"], errors = "coerce", utc = True)

df["timestamp1"].fillna(pd.Timestamp("1981-12-02"), inplace = True)
df["timestamp2"].fillna(pd.Timestamp("1981-12-02"), inplace = True)
df["created_at"].fillna(pd.Timestamp("1981-12-02"), inplace = True)

convert_float_ids_to_objects = ["author_id", "conversation_id", "replied_to_tweet_id", "quoted_author_id",
                                "retweeted_author_id"]
df[convert_float_ids_to_objects] = df[convert_float_ids_to_objects].astype(str)

df.fillna(0)


MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(MONGO_URI)
db = client["Britney_Tweet_Data"]
collection = db["Britney_Tweets"]

dataframe_dictionary = df.to_dict("records")
collection.insert_many(dataframe_dictionary, ordered = False)

collection.create_index([("text", "text")])
collection.create_index("created_at")
collection.create_index("author_id")

def tweets_per_day(term):
    return list(collection.aggregate([{
        "$match": {"text": {"$regex": term, "$options": "i"}}},
        {"$group": {"_id": {"dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}}, "count": {"$sum": 1}}}
    ]))

def unique_users(term):
    return list(collection.aggregate([
        {"$match": {"text": {"$regex": term, "$options": "i"}}},
        {"$group": {"_id": "$author_id"}},
        {"$group": {"_id": None, "count": {"$sum": 1}}}
    ]))
def average_likes(term):
    return list(collection.aggregate([
        {"$match": {"text": {"$regex": term, "$options": "i"}}},
        {"$group": {"_id": None, "avgLikes": {"$avg": "$like_count"}}}
    ]))
def tweets_by_location(term):
    return list(collection.aggregate([
        {"$match": {"text": {"$regex": term, "$options": "i"}}},
        {"$group": {"_id": "$place_id", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}
    ]))

def tweets_by_time_of_day(term):
    return list(collection.aggregate([
        {"$match": {"text": {"$regex": term, "$options": "i"}}},
        {"$group": {"_id": {"$hour": "$created_at"}, "count": {"$sum": 1}}}, {"$sort": {"_id": ASCENDING}}
    ]))

def user_with_most_tweets(term):
    return list(collection.aggregate([
        {"$match": {"text": {"$regex": term, "$options": "i"}}},
        {"$group": {"_id": "$author_handle", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]))

def main():
    while True:
        term = input("Enter a term to search for in tweets (or 'exit' to quit): ")
        if term.lower() == 'exit':
            break
        print("Tweets per day:", tweets_per_day(term))
        print("Unique Users:", unique_users(term))
        print("Average Likes:", average_likes(term))
        print("Tweets by Location:", tweets_by_location(term))
        print("Tweets by Time of Day:", tweets_by_time_of_day(term))
        print("User with Most Tweets:", user_with_most_tweets(term))

if __name__ == "__main__":
   main()
