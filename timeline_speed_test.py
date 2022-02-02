from mysql_api import *
from load_data import get_user_name_password, FILEPATH_USERNAME_PW


if __name__ == "__main__":
    username, password = get_user_name_password(FILEPATH_USERNAME_PW)

    timeline_speed_test = MySqlAPI(username, password)
    timeline_speed_test.run_timeline_speed_test(1000)