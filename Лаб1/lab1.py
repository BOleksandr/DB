import threading
import psycopg2
import time

username = 'Babenko'
password = 'postgres'
database = 'Labs_DB'
host = 'localhost'
port = '5432'




def lost_update(): 
    conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
    cursor = conn.cursor()
    with conn:    
        for _ in range(1, 10001):
            cursor.execute('SELECT counter FROM user_counter WHERE user_id = 1')
            counter = cursor.fetchone()[0]
            counter += 1
            cursor.execute("update user_counter set counter = %s where user_id = %s", (counter, 1))
            conn.commit()

def in_place_update():
    conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
    cursor = conn.cursor()
    with conn:
        for _ in range(1, 10001): 
            cursor.execute("update user_counter set counter = counter + 1 where user_id = 1")
            conn.commit()

def row_level_locking():
    conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
    cursor = conn.cursor()
    with conn:
        for _ in range(1, 10001): 
            cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
            counter = cursor.fetchone()[0]
            counter = counter + 1
            cursor.execute("update user_counter set counter = %s where user_id = %s", (counter, 1))
            conn.commit()
    
def optimistic_concurrency_control():
    conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
    cursor = conn.cursor()
    with conn:
        for _ in range(1, 10001):
            while True: 
                cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
                (counter, version) = cursor.fetchone()
                counter = counter + 1
                cursor.execute("update user_counter set counter = %s, version = %s where user_id = %s and version = %s", (counter, version + 1, 1, version))
                conn.commit()
                count = cursor.rowcount
                if (count > 0): 
                    break



def run_10_threads(f):
    threads = []

    for _ in range(10):
        thread = threading.Thread(target= f)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()         


def execution_time(func):
    start = time.time()
    run_10_threads(func)
    return time.time() - start



def get_counter():
    conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
    cursor = conn.cursor()
    with conn:
        cursor.execute('Select counter, version from user_counter where user_id = 1')
        counter = cursor.fetchone()[0]
        cursor.execute('update user_counter set counter = 0, version = 0 where user_id = 1')
        conn.commit()
    return counter    


func = [
    ('Lost-update', lost_update),
    ('In-place update', in_place_update),
    ('Row-level locking', row_level_locking),
    ('Optimistic concurrency control', optimistic_concurrency_control)
]

for i, j in func:
    print(i)
    print(f'time: {execution_time(j)} seconds')
    print('counter:', get_counter())
    print('-' * 20)

#  Lost-update
# time: 13.014509439468384 seconds
# counter: 13506
# --------------------
# In-place update
# time: 11.21582841873169 seconds
# counter: 100000
# --------------------
# Row-level locking
# time: 19.496898889541626 seconds
# counter: 100000
# --------------------
# Optimistic concurrency control
# time: 67.66792297363281 seconds
# counter: 100000
# --------------------   