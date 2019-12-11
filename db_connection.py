import sqlite3
import json

time_frame = '2015-01'
sql_transaction = []

connection = sqlite3.connect('{}2.db'.format(time_frame))
c_obj = connection.cursor()


def format_data(body_data):
    data = body_data.replace('\n', ' newlinechar ').replace('\r', ' newlinechar ').replace('"', "'")
    return data;


def find_parent(pid):

    print("Parent id : " + pid)
    try:
        sql = "SELECT comment FROM parent_replay WHERE comment_id = '{}' LIMIT 1".format(pid)
        c_obj.execute(sql)
        result = c_obj.fetchone()

        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        print("Find Parent " + str(e))
    finally:
        return False

def find_existing_score(pid):
    print("Parent id : " + pid)
    try:
        sql = "SELECT comment FROM parent_replay WHERE parent_id = '{}' LIMIT 1".format(pid)
        c_obj.execute(sql)
        result = c_obj.fetchone()

        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        print("Find Parent " + str(e))
    finally:
        return False


def acceptable(data):
    len_of_data=len(data)
    if len_of_data.split(' ') > 50 or len_of_data < 1:
        return False
    elif len_of_data > 1000:
        return False
    elif data==['deleted']:
        return False
    elif data==['removed']:
        return False
    else:
        return True

def create_table():
    c_obj.execute(
        "CREATE TABLE IF NOT EXISTS parent_replay(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")


if __name__ == '__main__':
    create_table()
    row_counter = 0
    paired_rows = 0

    with open('RC_{}'.format(time_frame.split('-')[0], time_frame), buffering=1000) as f:
        for row in f:

            print("Each Row")
            print(row)

            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            comment_id = row['name']
            subreddit = row['subreddit']
            parent_data = find_parent(parent_id)


            if score >= 2:
                existing_comment_score = find_existing_score(parent_id);
                if existing_comment_score:
                    if score > existing_comment_score:

