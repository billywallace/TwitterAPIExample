from twitter_api import TwitterAPI
import pymysql
import time
from datetime import datetime
import random


class MySqlAPI(TwitterAPI):
    def __init__(self, user: str, passw: str):
        self.username = user
        self.password = passw
        self.connection = pymysql.connect(host='localhost', user=user,
                                         password=passw,
                                         db='Twitter', charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)

    # Accepts csv of tweets, and adds the tweets to the Tweet table in the db
    def post_tweets(self, tweets):
        try:

            tweet_id = 1
            start = time.time()
            num_records = 0
            # iterate through csv object and post each entry to the Tweet table
            for row in tweets:
                tweet_id += 1
                num_records += 1
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                user_id = row[0]
                tweet = row[1]
                cur = self.connection.cursor()
                sql = "INSERT INTO `Tweet` (`tweet_id`, `user_id`, `tweet_ts`, `tweet_text`)VALUES (%s, %s, %s, %s)"
                cur.execute(sql, (tweet_id, user_id, timestamp, tweet))
                cur.close()
            # How many seconds passed?
            elapsed_time_lc = (time.time() - start)

            records_per_second = num_records / elapsed_time_lc
            print('Records per second:', records_per_second)
            self.connection.commit()

        except pymysql.err.OperationalError as e:
            print('Error: %d: %s' % (e.args[0], e.args[1]))

    # accepts a user_id, and returns a list of users who are followed by the given user_id
    def get_followees(self, user_id: int):
        try:
            cur = self.connection.cursor()
            sql = "SELECT follows_id FROM Follow WHERE " + user_id + "= user_id;"
            cur.execute(sql)
            followees = [str(item['follows_id']) for item in cur.fetchall()]
            cur.close()

            # If a user follows nobody, return a list of 0
            if len(followees) == 0:
                followees = ['0']

            return followees

        except pymysql.err.OperationalError as e:
            print('Error: %d: %s' % (e.args[0], e.args[1]))

    # gets the unique user_ids from the Tweet table
    def get_unique_user_ids(self):
        try:
            cur = self.connection.cursor()
            sql = "SELECT DISTINCT user_id FROM Tweet;"
            cur.execute(sql)
            user_ids = [item for item in cur.fetchall()]

            cur.close()

            return user_ids

        except pymysql.err.OperationalError as e:
            print('Error: %d: %s' % (e.args[0], e.args[1]))

    # Given a list of user_ids, return the top 10 most recent tweets posted by those users
    def get_timeline(self, followees: list):
        try:
            cur = self.connection.cursor()

            # format list to be in line with SQL syntax
            followees = ', '.join(followees)
            followees = '(' + followees + ')'

            sql = "SELECT * FROM Tweet WHERE user_id IN " + followees + " ORDER BY tweet_ts DESC LIMIT 10;"
            cur.execute(sql)
            tweets = [item for item in cur.fetchall()]
            cur.close()

            return tweets

        except pymysql.err.OperationalError as e:
            print('Error: %d: %s' % (e.args[0], e.args[1]))

    # Executes a timeline speed test for a given n number of timelines
    def run_timeline_speed_test(self, n: int):
        user_ids = self.get_unique_user_ids()
        random.shuffle(user_ids)


        # Pull out user_id from unique list of user_ids
        list_user_ids = [str(item['user_id']) for item in user_ids]

        # Only run for n values
        list_user_ids = list_user_ids[0: n]

        # generate list of people that the list of users follow for use in timeline retrieval
        list_followees = [self.get_followees(user_id) for user_id in list_user_ids]
        start = time.time()
        for user_id, followees in zip(list_user_ids, list_followees):
            print(user_id)
            self.get_timeline(followees)

        elapsed_time_lc = (time.time() - start)

        time_lines_per_second = n / elapsed_time_lc
        print('Records per second:', time_lines_per_second)
        self.connection.close()




