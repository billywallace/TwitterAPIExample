from mysql_api import *
from load_data import get_user_name_password, load_tweets, FILEPATH_USERNAME_PW, FILEPATH_TWEETS

# Before running, ensure that the Tweet table is empty
if __name__ == "__main__":
    username, password = get_user_name_password(FILEPATH_USERNAME_PW)
    tweets = load_tweets(FILEPATH_TWEETS)

    post_tweets_speed = MySqlAPI(username, password)

    post_tweets_speed.post_tweets(tweets)

