#!/usr/bin/env python
# coding: utf-8

# In[9]:


import psycopg2 #Imports the package to connect to PostGreSQL
import bs4 #BeautifulSoup #Imports the package for scraping data
import csv #Imports the csv reading utilities
import pandas as pd
import numpy as np

#The code to load and extract the data from the csv file

file1 = "List of States_United States.txt" #A file1 variable is created
file1_open = open(file1)
#print(file1_open.readline())

file2 = "List of States_India.txt"
file2_open = open(file2)
#print(file2_open.readline())

file3 = "List of States_Australia.txt"
file3_open = open(file3, 'r')
#print(file3_open.readline())

file4 = "List of States_Brazil.txt"
file4_open = open(file4, 'r')
#print(file4_open.readline())

file5 = "List of States_Germany.txt"
file5_open = open(file5, 'r')
#print(file5_open.readline())

file6 = "List of States_Canada.txt"
file6_open = open(file6, 'r')
#print(file6_open.readline())
#------ Create the dictionaries of the capitals and states to add the individual states through using for loops ------

#Create an empty dictionary to map the states and capitals within United States and within the subsequent for loop add "United States"
us_states_and_capitals = []

lines_to_fetch = file1_open.readline()

while lines_to_fetch:
    us_states_and_capitals.append(lines_to_fetch)
    lines_to_fetch = file1_open.readline()
file1_open.close()

print(type(us_states_and_capitals))


#Create dictionaries for mapping the city to the state and appending the "state value from the dictionary"
india_states_and_capitals = {}

lines_to_fetch = file2_open.readline()

while lines_to_fetch:
    field1 = lines_to_fetch.split(",")
    india_states_and_capitals[field1[0]] = field1[1].strip()
    lines_to_fetch = file2_open.readline()
file2_open.close()

print(india_states_and_capitals)

australia_states_and_capitals = {}

lines_to_fetch = file3_open.readline()


while lines_to_fetch:
    field1 = lines_to_fetch.split(",")
    australia_states_and_capitals[field1[0]] = field1[1].strip()
    lines_to_fetch = file3_open.readline()
    
print(australia_states_and_capitals)

brazil_states_and_capitals = {}
lines_to_fetch = file4_open.readline()
while lines_to_fetch:
    field1 = lines_to_fetch.split(",")
    brazil_states_and_capitals[field1[0]] = field1[1].strip()
    lines_to_fetch = file4_open.readline()

print(brazil_states_and_capitals)

germany_states_and_capitals = {}
lines_to_fetch = file5_open.readline()
while lines_to_fetch:
    field1 = lines_to_fetch.split(",")
    germany_states_and_capitals[field1[0]] = field1[1].strip()
    lines_to_fetch = file5_open.readline()
    
print(germany_states_and_capitals)

#--
canada_states_and_capitals = {}
lines_to_fetch = file6_open.readline()

while lines_to_fetch:
    field1 = lines_to_fetch.split(",")
    canada_states_and_capitals[field1[0]] = field1[1].strip()
    lines_to_fetch = file6_open.readline()

print(canada_states_and_capitals)

#---One time activities to make the dataset more complete ----

#print(header)

csv_file = 'uaScoresDataFrame_states_Version 21.csv' #Self-note: The iteration number will change with each revision
#with open(csv_file, 'r') as file:
 #   csvreader = csv.reader(file)
 #   header = next(csvreader)
 #   for row in csvreader:
 #       print(row)
 #       break
df3 = pd.read_csv(csv_file, encoding='latin1')
#print(pd.read_csv('uaScoresDataFrame_states_Version 21.csv'))
df3["UA_Country"].fillna('United States', inplace=True)
    
#print(df.to_string())

#df.to_csv('uaScoresDataFramea_states_Version 4.csv')
#print(csv_file_open.readline())

print('------------------\n------------------\n')

#print(df["UA_Country"][0])
#if df["UA_Country"].str.contains("Denmark").any():
    #print("Hello")
    
#print(india_states_and_capitals['Hyderabad'])

#print(df["UA_Name"].str.contains("Aarhus"))
    
#for i in india_states_and_capitals: #Iterates through the dictionary
#np.where(df["UA_Name"].str.contains("Hyderabad").any(), "Telangana", df["UA_State"])

for i in india_states_and_capitals:    
    df3.loc[df3.UA_Name == i, "UA_State"] = india_states_and_capitals[i]
    
for i in australia_states_and_capitals:    
    df3.loc[df3.UA_Name == i, "UA_State"] = australia_states_and_capitals[i]

for i in brazil_states_and_capitals:    
    df3.loc[df3.UA_Name == i, "UA_State"] = brazil_states_and_capitals[i]
    
for i in germany_states_and_capitals:    
    df3.loc[df3.UA_Name == i, "UA_State"] = germany_states_and_capitals[i]
    
for i in canada_states_and_capitals:    
    df3.loc[df3.UA_Name == i, "UA_State"] = canada_states_and_capitals[i]
        
#print(df["UA_State"])

#print(df["UA_State"])
#df["UA_State"].fillna('Placeholder', inplace=True)
       # if str(india_states_and_capitals.get(str(row[1]))) is None:
       # else:
            #print(1)
             #df["UA_State"].fillna(str(india_states_and_capitals[str(row[1])]), inplace=True)
        #if row[1] != india_states_and_capitals[str(row[1])]:
         #   continue
       # else:
        #    df["UA_State"].fillna(str(india_states_and_capitals[str(row[1])]), inplace=True)


            
df3.to_csv('uaScoresDataFrame_states_Version 22.csv')

#-------- Establish the connection to PostGRESQL and do the related connection operations ---

#Creates the conn (connect) variable that we will use to connet to PostGreSQL
conn = psycopg2.connect(user="postgres",  password='ta', port='5433', host="localhost")
print(conn)
cursor = conn.cursor()
print(cursor)

row_count = df3.count()[0]

#--- INSERT THE DATA FROM THE LATEST CSV FILE LATER ON --#
#-- OBJECTIVE HAS BEEEN MET ON GETTING THE STATE DATA --#
sql = '''CREATE DATABASE test''';
curr.execute(sql)
conn.commit()
#sql = '''DROP TABLE big_cities'''
sql= '''CREATE TABLE big_cities_latest (city_name VARCHAR(100), state VARCHAR(100), country VARCHAR(100), continent VARCHAR(100), housing_index DECIMAL, cost_of_living_index DECIMAL, startups_index DECIMAL, venture_capital_index DECIMAL, travel_connectivity_index DECIMAL, commute_index DECIMAL, business_freedom_index DECIMAL, safety_index DECIMAL, healthcare_index DECIMAL, education_index DECIMAL, environment_quality_index DECIMAL, economy_index DECIMAL, taxation_index DECIMAL, internet_access DECIMAL, leisure_index DECIMAL, tolerance_index DECIMAL, outdoors_index DECIMAL)'''
curr.execute(sql)
conn.commit()

#print(df3.values[0][4])

#YOU NEED TO DELETE THE FIRST MOST COLUMN FROM THE CSV FILE FOR THE BELOW LOOP TO WORK

for i in range(row_count):
    curr = conn.cursor()
    insert_query = '''INSERT INTO big_cities_latest (city_name, state, country, continent, housing_index, cost_of_living_index, startups_index, venture_capital_index, travel_connectivity_index, commute_index, business_freedom_index, safety_index, healthcare_index, education_index, environment_quality_index, economy_index, taxation_index, internet_access, leisure_index, tolerance_index, outdoors_index) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    record_to_insert = (df3.values[i][1], df3.values[i][2], df3.values[i][3], df3.values[i][5], df3.values[i][6], float(df3.values[i][7]), float(df3.values[i][8]), float(df3.values[i][9]), float(df3.values[i][10]), float(df3.values[i][11]), float(df3.values[i][12]), float(df3.values[i][13]), float(df3.values[i][14]), float(df3.values[i][15]), float(df3.values[i][16]), float(df3.values[i][17]), float(df3.values[i][18]), float(df3.values[i][19]), float(df3.values[i][20]), float(df3.values[i][21]), float(df3.values[i][22]))
    curr.execute(insert_query, record_to_insert)
    conn.commit()

curr = conn.cursor()
#sql = '''SELECT state FROM big_cities'''
#sql = '''DROP TABLE big_cities'''
#curr.execute(sql)

#print(curr.fetchall())
#conn.commit()
conn.close()

#----- SUCCESS, THE FIRST DATASET IS NOW PROCESSED INTO POSTGRESQL, OTHER DATA TO COME IN SOON ON HOUSING PRICES ---#


# In[ ]:




