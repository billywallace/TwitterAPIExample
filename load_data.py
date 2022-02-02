import csv

#file to load csv data and grab user name and passwords
FILEPATH_TWEETS = "tweet.csv"
FILEPATH_USERNAME_PW = "twitter_sql_user_pass.txt"

# accepts a filename for a text file with two lines of text, top line is the username second line is the password
def get_user_name_password(filename):
    with open(filename) as f:
        lines = f.readlines()

    username = lines[0]
    username = username.rstrip("\n")

    password = lines[1]

    return username, password

# Accepts a filename of file containing tweets and returns csv.reader object
def load_tweets(filename):
    file = open(filename)
    csvreader = csv.reader(file)
    next(csvreader)

    return csvreader


