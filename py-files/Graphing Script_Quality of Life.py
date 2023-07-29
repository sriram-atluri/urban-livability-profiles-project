#!/usr/bin/env python
# coding: utf-8

# In[118]:


#Import all the relevant graphing and database packages
import matplotlib.pyplot as plt
import numpy as np
import psycopg2
import pandas as pd

#Write the code to connect to the database and generate the bar graph

try:
    conn = psycopg2.connect(user="postgres",  password='ta', port='5433', host="localhost")
    conn.autocommit = True
except:
    print("Failed to connect to the database")
    
csv_file = 'uaScoresDataFrame_states_Version 21.csv'
df2 = pd.read_csv(csv_file)

city_names = []

for i in range(len(df2['UA_Name'])):
    city_names.append(df2['UA_Name'][i])
    
print(city_names)
#------ Modification and or testing ------

curr = conn.cursor()
#curr.execute("""UPDATE big_cities SET city_name='Portland_2' WHERE state=' Maine'""")
#curr.execute("SELECT * FROM big_cities")
conn.commit()

#result = curr.fetchall()
#print(result)

# The code for generating the matplotlib graph images

for i in range(len(df2['UA_Name'])):
    dbcursor = conn.cursor()
    dbcursor.execute("""SELECT housing_index, cost_of_living_index, startups_index, venture_capital_index, travel_connectivity_index, commute_index, business_freedom_index, safety_index, healthcare_index, education_index, environment_quality_index, economy_index, taxation_index, internet_access, leisure_index, tolerance_index, outdoors_index FROM big_cities WHERE city_name='""" + city_names[i] + """' """)
    result = dbcursor.fetchall()
    quality_of_life_index_names = ['Housing', 'Cost of Living', 'Startups', 'Venture Capital', 'Travel Connectivity', 'Commute', 'Business Freedom', 'Safety', 'Healthcare', 'Education', 'Environmental Quality', 'Economy', 'Taxation', 'Internet Access', 'Leisure & Culture', 'Tolerance', 'Outdoors'] #A list to hold onto the quality of life index names
    quality_of_life_results_list = [] #A list to hold onto the results to be eventually displayed in the bar graph
    for row in result:
        for j in range(len(row)):
            print("Row:::: " + str(row[j]))
            quality_of_life_results_list.append(float(row[j]))
    x = quality_of_life_index_names
    y = quality_of_life_results_list
    fig = plt.figure()
    fig.set_figwidth(30)
    fig.set_figheight(30)
     #print("----" + df['UA_Name'][i])
    plt.title('Livability Indicators and Performance Index')
    plt.rcParams.update({'font.size': 20})
    plt.barh(x, y)
    plt.xlabel("Livability Performance Index Measurement")
    plt.ylabel("Livabiity Indicators")
    #plt.gca().invert_yaxis()
    plt.savefig(df2['UA_Name'][i] + ".jpg")
    print("Iteration: " + str(i) + " City Name: " + df2['UA_Name'][i])
    plt.show()

#dbcursor.execute("""SELECT * FROM information_schema.tables""")
#dbcursor.execute("""INSERT INTO continent VALUES(6, 'South America')""")
#dbcursor.execute("""ALTER TABLE big_cities ADD CONSTRAINT city_id FOREIGN KEY""")


#print(df['country_id'][0])

#df=df.convert_dtypes()

#print(df['continent_index'].unique()[0])

    
#for i in range(df['UA_State'].nunique()):
    #print(df['UA_Name'].unique()[i])
    #print(i)
    #curr = conn.cursor()
    #insert_query = '''INSERT INTO country (continent_id) VALUES (%s)'''
    #record_to_insert = (i+1, df['UA_State'].unique()[i])
    #curr.execute(insert_query, record_to_insert)
    #conn.commit()
    

#dbcursor.execute("CREATE TABLE country(country_id INTEGER PRIMARY KEY, country_name VARCHAR(100), continent_id INTEGER REFERENCES continent(continent_id))")
#dbcursor.execute("DELETE FROM country")
#dbcursor.execute("SELECT * FROM country WHERE continent_id=6")
#dbcursor.execute("SELECT COUNT(country_name) FROM country")
#dbcursor.execute("ALTER TABLE country ADD CONSTRAINT c_name UNIQUE(country_name)")

#column_names = [desc[0] for desc in dbcursor.description]
#for i in column_names:
    #print(i)
#result = dbcursor.fetchall()
#countries=[]
#for i in range(df['UA_Country'].count()):
    #print(row[1])
 #   countries.append(df['UA_Country'][i])
    
#print(len(countries))

#countries_ct_index=[]

#for i in range(df['continent_index'].nunique()):
    #countries_ct_index.append(df['continent_index'].unique()[i])
    
#print(countries_ct_index)

#countries_ct_index2 = []

#print(df["UA_Country"].count())
#print (df["UA_Continent"] == "Europe")
#for i in range(len(countries)):
 #   if (df['UA_Continent'][i] == "Europe"):
  #      countries_ct_index2.append(df['continent_index'][i])
    #    countries_ct_index2.append(df['continent_index'][i])
   # elif (df['UA_Continent'][i] == "Asia"):
    #    countries_ct_index2.append(df['continent_index'][i])
    #elif (df['UA_Continent'][i] == "Oceania"):
     #   countries_ct_index2.append(df['continent_index'][i])
    #elif (df['UA_Continent'][i] == "North America"):
    #    countries_ct_index2.append(df['continent_index'][i])
    #elif (df['UA_Continent'][i] == "South America"):
    #    countries_ct_index2.append(df['continent_index'][i])
    #elif (df['UA_Continent'][i] == "Africa"):
     #   countries_ct_index2.append(df['continent_index'][i])

#print(countries)
#print(len(countries_ct_index2))

#countries_ct_index2 = list(dict.fromkeys(countries_ct_index2))
#print("modification 2 size:")
#print(len(countries_ct_index2))
#print(countries_ct_index2)

#for i in range(len(countries)):
    #curr = conn.cursor()
   # continent_id = int(countries_ct_index2[i])
   # print(countries_ct_index2[i])
#insert_query = '''UPDATE country SET continent_id=%s WHERE country_name=%s'''
#curr.execute(insert_query, (2, 'Japan'))
   # conn.commit()

#row_count = df.count()[0]
#print(row_count)
    
#for i in range(row_count):
    #curr = conn.cursor()
    #insert_query = '''INSERT INTO country (country_id, country_name, continent_id) VALUES (%s, %s, %s)'''
    #record_to_insert = (df.values[i][0], df.values[i][1], df.values[i][3])
    #curr.execute(insert_query, record_to_insert)
    #conn.commit()
    
#print("Test of country table"  + result)


#------------------
#dbcursor = conn.cursor()
#dbcursor.execute("select current_database()")
#print("Database name:" + dbcursor.fetchone()[1])


#print(result[0])

#quality_of_life_index_names = ['Housing', 'Cost of Living', 'Startups', 'Venture Capital', 'Travel Connectivity', 'Commute', 'Business Freedom', 'Safety', 'Healthcare', 'Education', 'Environmental Quality', 'Economy', 'Taxation', 'Internet Access', 'Leisure & Culture', 'Tolerance', 'Outdoors'] #A list to hold onto the quality of life index names
#quality_of_life_index_names_1 = ['Housing', 'Cost of Living', 'Startups', 'Venture Capital', 'Travel Connectivity', 'Commute', 'Business Freedom', 'Safety']
#quality_of_life_index_names_2 = ['Healthcare', 'Education', 'Environmental Quality', 'Economy', 'Taxation', 'Internet Access', 'Leisure & Culture', 'Tolerance', 'Outdoors'] #A list to hold onto the quality of life index names
#quality_of_life_results_list = [] #A list to hold onto the results to be eventually displayed in the bar graph

#print("List test: \n")
#for row in result:
 #   for i in range(len(row)):
  #      quality_of_life_results_list.append(float(row[i]))

    
#middle_length = len(quality_of_life_index_names)//2
#first_half_of_x = quality_of_life_index_names[:middle_length]
#first_half_of_y = quality_of_life_results_list[:middle_length]

#x = quality_of_life_index_names
#y = quality_of_life_results_list


# In[ ]:





# In[ ]:




