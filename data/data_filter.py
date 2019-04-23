from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import optparse
import subprocess
import random

import sqlite3

def filteringData():

    # LABELS: user_id,age,sex,country,checkin_date,trackable_id,trackable_type,trackable_name,trackable_value
    conn = sqlite3.connect('database.db')
    c = conn.cursor()

    # Create tables
    c.execute('''CREATE TABLE users
                (user_id string, gender string, country string, checkin_date string)''')
    c.execute('''CREATE TABLE symptoms
                (id string, symptom string)''')
    c.execute('''CREATE TABLE conditions
                (id string, condition string)''')
    c.execute('''CREATE TABLE treatments
                (id string, treatment string)''')
    c.execute('''CREATE TABLE tags
                (id string, tag string)''')

    with open("fd-export.csv", "r") as f:
        for line in f:
            # print(line.split(",")[6])
            # c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
            conn.commit()
    conn.close()
    sys.stdout.flush()

if __name__ == "__main__":
    filteringData()
