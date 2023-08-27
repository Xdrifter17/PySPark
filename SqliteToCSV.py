#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlite3
import pandas as pd


# In[9]:


conn = sqlite3.connect("database.sqlite")
cur = conn.cursor()


# In[12]:


# list of tables
cur.execute("select name from sqlite_master where type = 'table';")
print(cur.fetchall())


# In[14]:


matches = pd.read_sql_query("select * from Matches;", conn)
teams_in_matches = pd.read_sql_query("select * from Teams_in_Matches;", conn)
teams = pd.read_sql_query("select * from Teams;", conn)
unique_teams = pd.read_sql_query("select * from Unique_Teams;", conn)


# conn = sqlite3.connect("database.sqlite")
# cur = conn.cursor()

# In[15]:


matches.to_csv("./Data/Matches.csv", index=False)
teams_in_matches.to_csv(r"C:\Users\SAMAD\Downloads\Datasets\Matches", index=False)
teams.to_csv(r"C:\Users\SAMAD\Downloads\Datasets\Teams", index=False)
unique_teams.to_csv(r"C:\Users\SAMAD\Downloads\Datasets\Unique_Teams", index=False)

