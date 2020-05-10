from datetime import datetime
import tweepy
import logging
import twitter
import src.credentials as c
import pandas as pd
from textblob import TextBlob
from textblob import Word
from textblob.sentiments import NaiveBayesAnalyzer

#
# TODO: Connect to twitter
#

auth = tweepy.OAuthHandler(c.API_key, c.API_secret_key) #Interacting with twitter's API
auth.set_access_token(c.Access_token, c.Access_token_secret)

api = tweepy.API(auth)


#Extracting Tweets
def extract_tweets(num_tweets=10):
    results = []
    for tweet in tweepy.Cursor (api.search, q = 'covid zambia', lang = "en").items(num_tweets): 
        results.append(tweet)
    
    print("Done Extracting tweets")
    return results


def tweets_df(results):
    id_list = [tweet.id for tweet  in results]
    data_set = pd.DataFrame(id_list, columns = ["id"])

    data_set["text"] = [tweet.text for tweet in results]
    data_set["created_at"] = [tweet.created_at for tweet in results]
    data_set["retweet_count"] = [tweet.retweet_count for tweet in results]
    data_set["user_screen_name"] = [tweet.author.screen_name for tweet in results]
    data_set["user_followers_count"] = [tweet.author.followers_count for tweet in results]
    data_set["user_location"] = [tweet.author.location for tweet in results]
    data_set["Hashtags"] = [tweet.entities.get('hashtags') for tweet in results]
    
    print("Done Making dataframe")
    return data_set

def remove_dublicate(data_set):
    text = data_set["text"]

    for i in range(0,len(text)):
        txt = ' '.join(word for word in text[i] .split() if not word.startswith('https:'))
        data_set.at[i, 'text2']= txt
    
    data_set.drop_duplicates('text2', inplace=True)
    data_set.reset_index(drop = True, inplace=True)
    data_set.drop('text', axis = 1, inplace = True)
    data_set.rename(columns={'text2': 'text'}, inplace=True)

    print("Done removing dublicates")
    return data_set


def sentiment_classification(data_set):
    text = data_set["text"]
    for i in range(0,len(text)):
        textB = TextBlob(text[i])
        sentiment = textB.sentiment.polarity
        data_set.at[i, 'Sentiment']=sentiment
        if sentiment <0.00:
            SentimentClass = 'Negative'
            data_set.at[i, 'SentimentClass']= SentimentClass 
        elif sentiment >0.00:
            SentimentClass = 'Positive'
            data_set.at[i, 'SentimentClass']= SentimentClass 
        else:
            SentimentClass = 'Neutral'
            data_set.at[i, 'SentimentClass']= SentimentClass 

    print("Done classifying")
    return data_set


def bundle_all(num_tweets):
    result=extract_tweets(num_tweets)
    df=tweets_df(result)
    df=remove_dublicate(df)
    df=sentiment_classification(df)

    df.to_csv('data/COVID19_ZM_Tweets.csv')
    print("Done Bundling")


if __name__ == "__main__":
    bundle_all(1000)
