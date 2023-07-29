#!/usr/bin/env python
# coding: utf-8

# In[16]:


import pandas as pd
import psycopg2 
import numpy as np


cumulative_file = "uaScoresDataFrame_states_Version 21.csv"
df = pd.read_csv(cumulative_file, encoding='latin1')

conn = psycopg2.connect(user="postgres",  password='ta', port='5433', host="localhost")

sql= '''CREATE TABLE livability_results (results_id INTEGER PRIMARY KEY, housing_index DECIMAL, cost_of_living_index DECIMAL, startups_index DECIMAL, venture_capital_index DECIMAL, travel_connectivity_index DECIMAL, commute_index DECIMAL, business_freedom_index DECIMAL, safety_index DECIMAL, healthcare_index DECIMAL, education_index DECIMAL, environment_quality_index DECIMAL, economy_index DECIMAL, taxation_index DECIMAL, internet_access DECIMAL, leisure_index DECIMAL, tolerance_index DECIMAL, outdoors_index DECIMAL, city_id INTEGER, CONSTRAINT fk_city FOREIGN KEY(city_id) REFERENCES city(city_id) ON DELETE CASCADE)'''
curr = conn.cursor()
curr.execute(sql)
conn.commit()

row_count = len(df['UA_Name'])
print(row_count)

for i in range(row_count):
    curr = conn.cursor()
    insert_query = '''INSERT INTO livability_results (results_id, housing_index, cost_of_living_index, startups_index, venture_capital_index, travel_connectivity_index, commute_index, business_freedom_index, safety_index, healthcare_index, education_index, environment_quality_index, economy_index, taxation_index, internet_access, leisure_index, tolerance_index, outdoors_index, city_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    record_to_insert = (df.values[i][0], float(df.values[i][7]), float(df.values[i][8]), float(df.values[i][9]), float(df.values[i][10]), float(df.values[i][11]), float(df.values[i][12]), float(df.values[i][13]), float(df.values[i][14]), float(df.values[i][15]), float(df.values[i][16]), float(df.values[i][17]), float(df.values[i][18]), float(df.values[i][19]), float(df.values[i][20]), float(df.values[i][21]), float(df.values[i][22]), float(df.values[i][23]), df.values[i][0])
    curr.execute(insert_query, record_to_insert)
    conn.commit()
    
curr = conn.cursor()
#sql = '''SELECT * FROM livability_results WHERE city_id=1'''
#sql = '''SELECT city_name FROM city INNER JOIN (SELECT state_id FROM state WHERE state.country_id=14) A ON city.state_id=A.state_id'''
#sql = '''SELECT city_name FROM city INNER JOIN state ON city.state_id=state.state_id WHERE state.country_id=3'''
#sql = '''SELECT COUNT(city_name) FROM livability_results INNER JOIN (SELECT city_id, city_name FROM city INNER JOIN state ON city.state_id = state.state_id WHERE country_id=20) A ON A.city_id=livability_results.city_id'''
curr.execute(sql)
#colnames = [desc[0] for desc in curr.description]
#print(colnames)
conn.commit()
print(curr.fetchall())    
    
    
# ------- LIVABILITY RESULTS TABLE DONE ----#


# In[ ]:




