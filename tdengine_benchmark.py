import taos
import time
from datetime import datetime
import random

print()

def fetch_all_demo(conn: taos.TaosConnection):
    result: taos.TaosResult = conn.query("select * from camera01 where event_id=0 and ts<='2023-02-24 08:10:00.482'")
    rows = result.fetch_all_into_dict()
    print("row count:", result.row_count)
    print("===============all data===================")
    print(rows)

def create_stable_table(conn: taos.TaosConnection, num_cameras=20, num_events=7):
    for cam_id in range(num_cameras):
        print('Creating Stable')
        sql = "CREATE STABLE camera_{0} (ts timestamp,video_durations float,stream_path binary(64)) TAGS (event_id int)".format(cam_id+1)
        try:
            affected_rows = conn.execute(sql)
            print("Stable camera_{0} created".format(cam_id+1))
        except Exception as e:
            print(str(e))
        
        # for event_id in range(num_events): 
        #     print('Creating sub table')
        #     sql = "CREATE TABLE camera_{0}_{1} USING camera_{0} TAGS ({1})".format(cam_id +1, event_id+1)
        #     try:
        #         affected_rows = conn.execute(sql)
        #         print("Sub table camera_{0}_{1} created".format(cam_id +1, event_id+1))
        #     except Exception as e:
        #         print(str(e))

import time

def insert_data(conn: taos.TaosConnection):
    total_time = 0
    num_records = 100000
    for i in range(num_records):
        camera_id = random.randint(1,20)
        event_id = random.randint(1,7)
        duration = random.randint(1,500)
        str_time = str(datetime.fromtimestamp(time.time()+i))
        sql = "INSERT INTO camera_{0}_{1} USING camera_{0} TAGS({1}) VALUES('{2}',{3},'temp/abc.m3u8')".format(camera_id, event_id, str_time, duration)
        start = time.time()
        affected_rows = conn.execute(sql)
        total_time += time.time() - start

        print("affected_rows", affected_rows) 
        if i %1000==0:
            print('done inserted', i)
    print('Speed insert time/record =', total_time/(num_records))

def test_connection():
    # all parameters are optional.
    # if database is specified,
    # then it must exist.
    conn = taos.connect(host="localhost",
                        port=6030,
                        user="root",
                        password="taosdata",
                        database="eventcamera")
    print('client info:', conn.client_info)
    print('server info:', conn.server_info)
    fetch_all_demo(conn)
    conn.close()

def get_connection() -> taos.TaosConnection:
    """
    create connection use firstEp in taos.cfg and use default user and password.
    """
    conn = taos.connect(host="localhost",
                        port=6030,
                        user="root",
                        password="taosdata",
                        database="eventcamera")
    return conn
    
def select_benchmark_from_stable(conn: taos.TaosConnection):
    total_time = 0
    num_sql = 1000
    for i in range(num_sql):
        camera_id = random.randint(1,20)
        event_id = random.randint(1,7)
        sql = "select count(*) from camera_{0} where event_id={1} and ts>'2023-02-26 15:43:45.111'".format(camera_id, event_id)
        # sql = "select count(*) from camera_{0} where event_id={1}".format(camera_id, event_id)
        start = time.time()
        result: taos.TaosResult = conn.query(sql)
        total_time += time.time() - start
        print(result.fetch_all_into_dict())
    print('Speed sql time/query =', total_time/num_sql)

def select_benchmark_from_subtable(conn: taos.TaosConnection):
    total_time = 0
    num_sql = 1000
    for i in range(num_sql):
        camera_id = random.randint(1,20)
        event_id = random.randint(1,7)
        sql = "select count(*) from camera_{0}_{1} where ts>'2023-02-26 15:43:45.111'".format(camera_id, event_id)
        start = time.time()
        result: taos.TaosResult = conn.query(sql)
        total_time += time.time() - start
        print(result.fetch_all_into_dict())
    print('Speed sql time/query =', total_time/num_sql)

if __name__ == "__main__":
    connection = get_connection()
    try:
        # create_stable_table(conn=connection, num_cameras=20, num_events=7)
        insert_data(connection)
        # select_benchmark_from_stable(connection)
        # select_benchmark_from_subtable(connection)
    finally:
        connection.close()
