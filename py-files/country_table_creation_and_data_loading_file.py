#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
import psycopg2 
import numpy as np

conn = psycopg2.connect(user="postgres",  password='ta', port='5433', host="localhost")
curr = conn.cursor()

curr.execute("CREATE TABLE country(country_id INTEGER PRIMARY KEY, country_name VARCHAR(100), continent_id INTEGER REFERENCES continent(continent_id) ON DELETE CASCADE)")
conn.commit()

#NOTE: The mapping of the countries to the continents was done with the IF function in Excel

country_id_name_continent_file = "countries_and_their_ids.csv"
df = pd.read_csv(country_id_name_continent_file)

for i in range(len(df['country_id'])):
    curr = conn.cursor()
    insert_query = '''INSERT INTO country (country_id, country_name, continent_id) VALUES (%s, %s, %s)'''
    record_to_insert = (df.values[i][0], df.values[i][1], df.values[i][3])
    curr.execute(insert_query, record_to_insert)
    conn.commit()
    
curr = conn.cursor()
sql = '''SELECT * FROM country'''
curr.execute(sql)
conn.commit()
print(curr.fetchall())
    


# In[ ]:




