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


def  create_table():

    conn,cur = connect()
    
    csv_file_path = 'customers.csv'

    df = pd.read_csv(csv_file_path)

    cur.execute("CREATE TABLE customer(id integer primary key,customer_id varchar(50),first_name varchar(50),last_name varchar(50),company varchar(50), city varchar(50),country varchar(50),phone1 varchar(50), phone2 varchar(50),email varchar(50), subscription_date varchar(50), website varchar(50));")

    for index, row in df.iterrows():
        cur.execute(
            f"""
            INSERT INTO customer (id, customer_id, first_name, last_name, company,
                                    city, country, phone1, phone2, email, subscription_date, website)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            tuple(row)
        )

    
    conn.commit()
    cur.close()
    conn.close()


def parse():

    conn,cur = connect()

    cur.execute("""
                
        CREATE TABLE customer_info(customer_id varchar(50) primary key,f_name varchar(50),l_name varchar(50), city varchar(50),country varchar(50),personal_phnum varchar(50),personal_email varchar(50));

        CREATE TABLE customer_work_info(customer_id varchar(50) primary key,full_name varchar(50), off_loc varchar(50), subscription_date varchar(50), website varchar(50),work_phnum varchar(50), work_email varchar(50));

        insert into customer_info select customer_id,UPPER(first_name),UPPER(last_name),city,country,phone1,email from customer; 

        insert into customer_work_info select customer_id,concat(first_name,' ',last_name),city,subscription_date,phone2,concat(LOWER(first_name),'_',LOWER(last_name),'@sample.com') from customer; 

        """)
    
    conn.commit()
    cur.close()
    conn.close()


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
    qus = input("enter question number")
    if qus == "1":
        create_table()
        parse()
    else:
        name = input("enter the string : ")
        find_customer(name)
