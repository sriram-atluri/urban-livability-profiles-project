#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import psycopg2 

conn = psycopg2.connect(user="postgres",  password='ta', port='5433', host="localhost")
curr = conn.cursor()

curr.execute("CREATE TABLE continent(continent_id INTEGER PRIMARY KEY, country_name VARCHAR(100))")
conn.commit()

continent_dictionary={1: 'Africa', 2: 'Asia', 3:'Europe', 4: 'North America', 5: 'Oceania', 6: 'South America'}

for key, value in continent_dictionary.items():
    curr = conn.cursor()
    insert_query = '''INSERT INTO continent (continent_id, continent_name) VALUES (%s, %s)'''
    record_to_insert = (key, value)
    curr.execute(insert_query, record_to_insert)
    conn.commit()

curr = conn.cursor()
sql = '''SELECT * FROM continent'''
curr.execute(sql)
conn.commit()
print(curr.fetchall())
    

