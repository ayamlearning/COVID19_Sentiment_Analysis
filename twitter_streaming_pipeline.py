from datetime import date, timedelta
import pandas as pd
from textblob import TextBlob
import GetOldTweets3 as got
import pickle
import os


#
# TODO: Extracting Tweets
#
def extract_tweets(str_strt_date,str_end_date,lst_search_string):
    filepath="data/raw"
    search_string=' '.join([str(elem) for elem in lst_search_string])

    tweetCriteria = got.manager.TweetCriteria()\
                        .setQuerySearch(search_string)\
                        .setSince(str_strt_date)\
                        .setUntil(str_end_date)

    result = got.manager.TweetManager.getTweets(tweetCriteria)
    pickle.dump( result,open(os.path.join(filepath,"raw_tweets.pkl"), "wb" ) )
    print("Done extracting tweets")
   

#
# TODO: Transform Tweets
#
def transform_tweets():
    filepath="data"
    results= pickle.load( open(os.path.join(filepath,"raw/raw_tweets.pkl"), "rb" ) )

    id_list = [tweet.id for tweet  in results]
    data_set = pd.DataFrame(id_list, columns = ["id"],index=None)

    data_set["username"] = [tweet.username for tweet in results]
    data_set["text"] = [tweet.text for tweet in results]
    data_set["created_at"] = [tweet.date for tweet in results]
    #extracting the date from the created at column and making a date column
    data_set['date'] =  pd.to_datetime(data_set['created_at']).dt.date
    data_set["retweets"] = [tweet.retweets for tweet in results]
    data_set["hashtags"] = [tweet.hashtags for tweet in results]
    data_set["geo"] = [tweet.geo for tweet in results]
        
    data_set=remove_duplicate(data_set)
    data_set=sentiment_classification(data_set)

    if os.path.exists(os.path.join(filepath,"processed/COVID19_ZM_transformedTweets.csv")):
        df=pd.read_csv(os.path.join(filepath,"processed/COVID19_ZM_transformedTweets.csv"))
        df_2=pd.concat([data_set,df])
        df_2=remove_duplicate(df_2)

        df_2.to_csv(os.path.join(filepath,"processed/COVID19_ZM_transformedTweets.csv"))
    
    else:
        data_set.to_csv(os.path.join(filepath,"processed/COVID19_ZM_transformedTweets.csv"))
        
    print("Done tranforming tweets \nShape:{}".format(data_set.shape))


#
# TODO: Remove Dublicates
#
def remove_duplicate(data_set):
    return data_set.drop_duplicates(subset =["id","username"],keep = False) 


#
# TODO: Classify tweets
#
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

    return data_set

#
# TODO: Automatically add todays tweets
#


#Main Methods
if __name__ == "__main__":
    end_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    extract_tweets(end_date, date.today().strftime("%Y-%m-%d"),
                   ['covid OR corona%', 'zambia'])
    transform_tweets()
