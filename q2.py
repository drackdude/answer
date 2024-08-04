import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv


load_dotenv()

def connect():
    conn = psycopg2.connect(
        database=os.getenv('DATABASE'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST'),
        port=os.getenv('PORT')
    )

    cur = conn.cursor()
    
    return conn, cur

def find_customer(name):
    conn, cur = connect()
    

    query = """
    SELECT ci.customer_id, cwi.full_name, ci.city, ci.personal_phnum, ci.personal_email, 
           cwi.off_loc, cwi.subscription_date, 
           current_date::date - cwi.subscription_date::date AS pending_days, 
           cwi.work_phnum, cwi.work_email 
    FROM customer_info ci 
    NATURAL JOIN customer_work_info cwi 
    WHERE ci.f_name LIKE %s;
    """
    
    cur.execute(query, (name + '%',))
    data = cur.fetchall()
    
    
    columns = [
        'customer_id', 'full_name', 'city', 'personal_phnum', 'personal_email', 
        'off_loc', 'subscription_date', 'pending_days', 'work_phnum', 'work_email'
    ]
    
    
    df = pd.DataFrame(data, columns=columns)
    
    
    df.to_csv('customer_details.csv', index=False)
    
    # if in case of printing
    # print_data(data)
    
    
    cur.close()
    conn.close()


def print_data(data):
    for i in data:
        print(i)


if __name__ == '__main__':
    name = input("enter the string : ")
    find_customer(name)
