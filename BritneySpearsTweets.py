__author__ = "AJ Ilentamhen"
#These first many lines of code were part of my data cleaning/wrangling process. I wanted to understand
#the format of the data before getting into creating functions for users to use as queries in MongoDB!

#First, I knew I would have to use pandas to see and clean the data, and wanted to upload data to MongoDB,
# so I uploaded those two necessary packages: pandas and MongoClient.  Furthermore, I know the compatibility
# of datetime data in newer versions of Python and MongoDB is tricky, so I imported the datetime package
#just in case I would need it for later
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

#Here, I uploaded the tsv file and used the delimeter in python to specify that it is indeed a tsv and not
#a csv file
df = pd.read_csv("Copy of correct_twitter_202102.tsv", delimiter = "\t")

#I wanted to knock out some introductory data wrgangling and cleaning at once. I had already opened the
#tsv file separately in a tsv viewer, but seeing how many columns had missing values was valuable information
#I gained from using the .isna() and .sum() methods, and sorting these value in order
print(df.shape, df.head(), df.columns, df.info())
print(df.isna().sum())

sorted_cols_missing_values = df.isna().sum()
print(sorted_cols_missing_values.sort_values(ascending = True))

#So, getting rid of irrelevant information and making sense of all information available is always important
#as a data scientist, particularly when you initially import data before cleaning. I saw that these two
#columns had no data in them at all, and were not useful for that reason
df = df.drop(["mentioned_author_ids", "mentioned_handles"], axis=1)

#This is one of my favorite python methods to use.  I thought it would be useful to see, of the remaining
#columns, how many unique values each had in order (for the sake of organization)
print(df.nunique().sort_values(ascending = True))

#I saw that there were a number of columns with 45 or less unique values, while all the others had
#considerably more than that number, so dropped all columns with less than 46 unique values
threshold = 46
cols_not_unique_enough = df.columns[df.nunique() < threshold]
df = df.drop(cols_not_unique_enough, axis=1)

#I enjoy working with sets to discover more insight into what is unique in a dataframe.  I had the idea
#to convert two suspicious columns into sets to see if they both were needed, or if they were redundant.
#I found the difference in their uniqueness and decided to keep both, though why there are two separate
#timestamps per row still escapes me. This would be a great question, as a datascientist, for me to pose
#to whoever sourced the data
set1 = set(df["ts1"])
set2 = set(df[" ts2"])
set_difference = set1.difference(set2)
set_difference_count = len(set_difference)

print(set_difference_count)

#After all of this, I took another look at column names, and cleaned things up to be more specific, clear,
#and thorough. I also noticed the ts2 column had a tricky leading white space in its name, and got rid of
#all white space in the dataframe
print(df.columns)

df = df.rename(columns = {"id":"tweet_id", "ts1":"timestamp1", " ts2":"timestamp2",
                          "replied_to": "replied_to_tweet_id"})
df = df.replace(r"\s+", " ", regex=True)

#I took a look at the data types to see what cleaning I should do there. I saw three columns that needed to
#be converted to datetimes and did just that.  Newer versions of Python require a utc to be set to True,
#so I did that as well.  Later on, when I tried to upload this data to MongoDB, I received multiple errors
#saying Mongo could not accept NaT data in these datetime columns at all. I tried to change the NaT data to
#Null, but Mongo was not accepting that either, so I chose to set all NaT dates to a date that Britney
#could not have possibly tweeted on, her birthdate! This will be specified in my hypothethical instructions
#on how to use the system in MongoDB (ignore all dates set to Britney's birthdate). If I were creating a
#machine learning model with the dataframe however, I would have approached this differently
print(df.dtypes)
df["timestamp1"] = pd.to_datetime(df["timestamp1"], errors = "coerce", utc = True)
df["timestamp2"] = pd.to_datetime(df["timestamp2"], errors = "coerce", utc = True)
df["created_at"] = pd.to_datetime(df["created_at"], errors = "coerce", utc = True)
print(df.dtypes)
df["timestamp1"].fillna(pd.Timestamp("1981-12-02"), inplace = True)
df["timestamp2"].fillna(pd.Timestamp("1981-12-02"), inplace = True)
df["created_at"].fillna(pd.Timestamp("1981-12-02"), inplace = True)

#I changed ids to string data types rather than floats, for the sake of organization
convert_float_ids_to_objects = ["author_id", "conversation_id", "replied_to_tweet_id", "quoted_author_id",
                                "retweeted_author_id"]
df[convert_float_ids_to_objects] = df[convert_float_ids_to_objects].astype(str)

#Out of habit, although I'm not creating a machine learning model with this dataframe, I set missing
#values to zero (of course, outside of the missing datetime data, so that this data could be transferred
#to MongoDB
df.fillna(0)

#I wanted one last look at the data before sending to Mongo
print(df.head())


client = MongoClient("mongodb://localhost:27017/")
db = client["Britney_Tweet_Data"]
collection = db["Britney_Tweets"]

dataframe_dictionary = df.to_dict("records")
collection.insert_many(dataframe_dictionary)

#These are all functions I spent some time figuring out on how to, essentially, "solve for music", or any
#other term that can take the place of music.  I wanted to follow the specific query examples that were
#presented in the instructions
def tweets_per_day(music):
    return list(collection.aggregate([{
        "$match": {"text": {"$regex": music, "$options": "i"}}},
        {"$group": {"_id": {"dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}}, "count": {"$sum": 1}}}
    ]))

def unique_users(music):
    return list(collection.aggregate([
        {"$match": {"text": {"$regex": music, "$options": "i"}}},
        {"$group": {"_id": "$author_id"}},
        {"$group": {"_id": None, "count": {"$sum": 1}}}
    ]))
def average_likes(music):
    return list(collection.aggregate([
        {"$match": {"text": {"$regex": music, "$options": "i"}}},
        {"$group": {"_id": None, "avgLikes": {"$avg": "$like_count"}}}
    ]))
def tweets_by_location(music):
    return list(collection.aggregate([
        {"$match": {"text": {"$regex": music, "$options": "i"}}},
        {"$group": {"_id": "$place_id", "count": {"$sum": 1}}}
    ]))

def tweets_by_time_of_day(music):
    return list(collection.aggregate([
        {"$match": {"text": {"$regex": music, "$options": "i"}}},
        {"$group": {"_id": {"$hour": "$created_at"}, "count": {"$sum": 1}}}
    ]))

def user_with_most_tweets(music):
    return list(collection.aggregate([
        {"$match": {"text": {"$regex": music, "$options": "i"}}},
        {"$group": {"_id": "$author_handle", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]))

if __name__ == "__main__":
    term = "music"
    print("Tweets per day:", tweets_per_day(term))
    print("Unique Users:", unique_users(term))
    print("Average Likes:", average_likes(term))
    print("Tweets by Location:", tweets_by_location(term))
    print("Tweets by Time of Day:", tweets_by_time_of_day(term))
    print("User with Most Tweets:", user_with_most_tweets(term))

#To use this system, first you must download MongoDB on your computer and use this script in an IDE
#that accepts Python.  Place this script in the IDE while simultaneously making sure MongoDB is open.
#You can then run queries specific to the ones in the instructions for this project to get results!!