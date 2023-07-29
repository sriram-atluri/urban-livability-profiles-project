#!/usr/bin/env python
# coding: utf-8

# In[48]:


import pandas as pd
import psycopg2 
import numpy as np

#states_and_ids_file = "states_and_ids_9_temporary.xlsx"
#df = pd.read_excel(states_and_ids_file)

conn = psycopg2.connect(user="postgres",  password='ta', port='5433', host="localhost")

us_country_id = 3
india_country_id = 20
australia_country_id = 2
brazil_country_id = 39
canada_country_id = 34
germany_country_id = 22

file1 = open('List of States_United States.txt', 'r')
file2 = open('List of States_India _2_NoCity.txt', 'r')
file3 = open('List of States_Australia_2_NoCity.txt', 'r')
file4 = open('List of States_Brazil_2_NoCity.txt', 'r')
file5 = open('List of States_Canada_2_NoCity.txt', 'r')
file6 = open('List of States_Germany_2_NoCity.txt', 'r', encoding='latin1')

lines1 = file1.readlines()
lines2 = file2.readlines()
lines3 = file3.readlines()
lines4 = file4.readlines()
lines5 = file5.readlines()
lines6 = file6.readlines()

file_states = "states_and_ids_9_temporary.csv"

tr3 = pd.read_csv(file_states)
print(len(tr2))
#print(tr2)

state_keys_and_values = {}
print(len(lines1))

for i in range(len(tr3)):
    state_keys_and_values[tr3['state_name_2'][i]] = i+1

us_state_and_country_name = {}
for i in range(len(lines1)):
    us_state_and_country_name[lines1[i].rstrip("\n")] = 'United States'

for i in range(len(tr3)):
    for j in us_state_and_country_name:
        if(tr3.loc[i, "state_name_2"] == j):
            tr3.at[i, 'country_id'] = int(us_country_id)

#-----------------#

india_state_and_country = {}
for i in range(len(lines2)):
    india_state_and_country[lines2[i].rstrip("\n")] = 'India'

for i in range(len(tr3)):
    for j in india_state_and_country:
        if(tr3.loc[i, "state_name_2"] == j):
            tr3.at[i, 'country_id'] = int(india_country_id)
            
#----------------#

australia_state_and_country = {}
for i in range(len(lines3)):
    australia_state_and_country[lines3[i].rstrip("\n")] = 'Australia'

for i in range(len(tr3)):
    for j in australia_state_and_country:
        if(tr3.loc[i, "state_name_2"] == j):
            tr3.at[i, 'country_id'] = int(australia_country_id)

#--------------#
brazil_state_and_country = {}
for i in range(len(lines4)):
    brazil_state_and_country[lines4[i].rstrip("\n")] = 'Brazil'

for i in range(len(tr3)):
    for j in brazil_state_and_country:
        if(tr3.loc[i, "state_name_2"] == j):
            tr3.at[i, 'country_id'] = int(brazil_country_id)
            
#------------#

canada_state_and_country = {}
for i in range(len(lines5)):
    canada_state_and_country[lines5[i].rstrip("\n")] = 'Canada'

for i in range(len(tr3)):
    for j in canada_state_and_country:
        if(tr3.loc[i, "state_name_2"] == j):
            tr3.at[i, 'country_id'] = int(canada_country_id)
            
#--------------#

germany_state_and_country = {}
for i in range(len(lines6)):
    germany_state_and_country[lines6[i].rstrip("\n")] = 'Germany'

for i in range(len(tr3)):
    for j in germany_state_and_country:
        if(tr3.loc[i, "state_name_2"] == j):
            tr3.at[i, 'country_id'] = int(germany_country_id)
            
            
tr3.to_csv('final_state_file.csv')
print(tr3.values[0][2])
for i in range(len(df['state_id'])):
    curr = conn.cursor()
    insert_query = '''INSERT INTO state (state_id, state_name, country_id) VALUES (%s, %s, %s)'''
    record_to_insert = (tr3.values[i][0], tr3.values[i][1], tr3.values[i][2])
    curr.execute(insert_query, record_to_insert)
    conn.commit()
    
curr = conn.cursor()
sql = '''SELECT * FROM state'''
curr.execute(sql)
conn.commit()
print(curr.fetchall())


# In[ ]:





# In[ ]:




