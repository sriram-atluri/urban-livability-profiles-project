#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import psycopg2 
import numpy as np

cumulative_file = "uaScoresDataFrame_states_Version 21.csv"
df = pd.read_csv(cumulative_file, encoding='latin1')

countries_and_ids_file = "countries_and_their_ids.csv"
df2 = pd.read_csv(countries_and_ids_file)

states_and_ids_file = "states_and_ids_9.xlsx"
df3 = pd.read_excel(states_and_ids_file)


#For cities with no state values, map to country id first and then map to the corresponding state id


#First step: lets map the cities with states, if exists, otherwise the countries
cities_with_states = {}
cities_with_countries = {}

for i in range(len(df['UA_Name'])):
    if (pd.isnull(df['UA_State_2'][i])):
        cities_with_countries[df['UA_Name'][i]] = df['UA_Country'][i]
    else:
        cities_with_states[df['UA_Name'][i]] = df['UA_State_2'][i]
        
print("Length of cities vs countries dictionary: ", len(cities_with_countries))
print("Length of cities vs states dictionary: ", len(cities_with_states))
        
print("---- Cities and their states ----\n\n")
    
print(cities_with_states)

#Second step: lets map the countries with ids and states with their ids
countries_and_ids = {}

for i in range(len(df2['UA_Country'])):
    countries_and_ids[df2['UA_Country'][i]] = df2['country_id'][i]
    
print ("---- Countries and their ids ----\n\n")
print(countries_and_ids)


#Third step: Lets map the states with their ids
states_and_ids = {}

for i in range(len(df3['state_id'])):
    states_and_ids[df3['state_name_2'][i]] = df3['state_id'][i]
    
print ("---- States and their ids ----")
print(states_and_ids)

print ("\n\n ---- Country ids vs states ----")

country_ids_and_states = {}

for i in range(len(df3['state_name_2'])):
    if (pd.isnull(df3['state_name_2'][i])):
        country_ids_and_states[df3['country_id'][i]] = df3['state_id'][i]
    
print(country_ids_and_states)
    
#--------------

#Fourth step: Let's map the city_id with the city_name and state_name, and subsequently the state_id

print('\n\n ---- city id vs state id for cities with states ---- \n\n')
city_id_city_name_state_id_1 = {}
count=0

#print("Key value for new mexico: ", states_and_ids.get("New Mexcio"))

#print(df[df['UA_Name'] == 'Anchorage'].index.values)
for key, value in cities_with_states.items():
        #print(states_and_ids.get(value))
        numbering_from_dataset = tuple(df[df['UA_Name'] == key].index.values + 1)
        city_id_city_name_state_id_1[numbering_from_dataset[0]]=[key, states_and_ids.get(value)]

print(city_id_city_name_state_id_1)
print(len(city_id_city_name_state_id_1))

print('\n\n ---- city id vs state id for cities with no states ---- \n\n')

city_id_city_name_state_id_2 = {}

for key, value in cities_with_countries.items():
            numbering_from_dataset_2 = tuple(df[df['UA_Name'] == key].index.values + 1)
            country_name_for_id = countries_and_ids.get(value)
            city_id_city_name_state_id_2[numbering_from_dataset_2[0]] = [key, country_ids_and_states.get(country_name_for_id)]
    

city_id_city_name_state_id_2.update(city_id_city_name_state_id_1)

# The below print call is to verify that the overall length is the same as the one in the main dataset
#print(len(city_id_city_name_state_id_2))

merged_dictionary_itemized = city_id_city_name_state_id_2.items()

sorted_state_dictionary = sorted(merged_dictionary_itemized)

print(type(sorted_state_dictionary))

print("--- Sorted state dictionary --- \n\n")
print(type(sorted_state_dictionary[0][0]))
print(sorted_state_dictionary[0][1][0])
print(sorted_state_dictionary[0][1][1])
#print ("---- States and country ids ----")


#Keep note  that you once you get the country value, that cities can definitely share the same country_id value

#Loop through the dataframe
    
#Establishes the PostgrSQL connection
conn = psycopg2.connect(user="postgres",  password='ta', port='5433', host="localhost")

#------- City entry insertion looping ---#
for i in range(len(df['UA_Name'])):
    curr = conn.cursor()
    insert_query = '''INSERT INTO city (city_id, city_name, state_id) VALUES (%s, %s, %s)'''
    record_to_insert = (str(sorted_state_dictionary[i][0]), str(sorted_state_dictionary[i][1][0]), str(sorted_state_dictionary[i][1][1]))
    curr.execute(insert_query, record_to_insert)
    conn.commit()
    

curr = conn.cursor()

#Test queries prior to implementing in PHP
#sql = '''SELECT * FROM country INNER JOIN continent ON country.continent_id=continent.continent_id'''
#sql = '''SELECT * FROM state INNER JOIN country ON state.country_id = country.country_id'''

#City table verification query
sql = '''SELECT * FROM city'''
curr.execute(sql)
conn.commit()
print(curr.fetchall())


#------ WELL DONE -----#


# In[ ]:





# In[ ]:




