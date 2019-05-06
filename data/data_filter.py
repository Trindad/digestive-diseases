from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import optparse
import subprocess
import random

import sqlite3
import csv


def filteringData():

    # LABELS: user_id,age,sex,country,checkin_date,trackable_id,trackable_type,trackable_name,trackable_value
    conn = sqlite3.connect('database.db')
    conn.text_factory = str
    c = conn.cursor()

    # Create tables
    c.execute('''CREATE TABLE IF NOT EXISTS users
                (user_id string, gender string, age integer, country string, checkin_date string)''')

    c.execute('''CREATE TABLE IF NOT EXISTS symptoms
                (id string, symptom string)''')

    c.execute('''CREATE TABLE IF NOT EXISTS conditions
                (id string, condition string)''')

    c.execute('''CREATE TABLE IF NOT EXISTS treatments
                (id string, treatment string)''')

    c.execute('''CREATE TABLE IF NOT EXISTS tags
                (id string, tag string)''')

    c.execute('''CREATE TABLE IF NOT EXISTS weathers
                (id string, weather string)''')

    c.execute('''CREATE TABLE IF NOT EXISTS users_symptoms
                (user_id string, symptom_id string)''')

    c.execute('''CREATE TABLE IF NOT EXISTS users_conditions
                (user_id string, condition_id string)''')

    c.execute('''CREATE TABLE IF NOT EXISTS users_treatments
                (user_id string, treatment_id string)''')

    c.execute('''CREATE TABLE IF NOT EXISTS users_tags
                (user_id string, tag_id string)''')

    c.execute('''CREATE TABLE IF NOT EXISTS users_weathers
                (user_id string, weather_id string)''')
    count = 0
    with open('fd-export.csv', mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            count += 1
            # print("Line: ", count, " ", row)
            c.execute(
                "SELECT user_id FROM users WHERE users.user_id = '"+row["user_id"] + "'")

            if c.fetchone() == None:
                # user_id string, gender string, age integer, country string, checkin_date string
                c.execute("INSERT INTO users VALUES (?, ?, ?, ?, ?)", (
                    row["user_id"], row["sex"], row["age"], row["country"], row["checkin_date"]))

            if row["trackable_type"] == 'Symptom':
               c.execute("INSERT INTO symptoms VALUES (?, ?)",
                         (row["trackable_id"], row["trackable_name"]))
               c.execute("INSERT INTO users_symptoms VALUES (?, ?)",
                         (row["user_id"], row["trackable_id"]))

            if row["trackable_type"] == 'Condition':
                c.execute("INSERT INTO conditions VALUES (?, ?)",
                          (row["trackable_id"], row["trackable_name"]))
                c.execute("INSERT INTO users_conditions VALUES (?, ?)",
                          (row["user_id"], row["trackable_id"]))

            if row["trackable_type"] == 'Treatment':
                c.execute("INSERT INTO treatments VALUES (?, ?)",
                          (row["trackable_id"], row["trackable_name"]))
                c.execute("INSERT INTO users_treatments VALUES (?, ?)",
                          (row["user_id"], row["trackable_id"]))

            if row["trackable_type"] == 'Tag':
                c.execute("INSERT INTO tags VALUES (?, ?)",
                          (row["trackable_id"], row["trackable_name"]))
                c.execute("INSERT INTO users_tags VALUES (?, ?)",
                          (row["user_id"], row["trackable_id"]))

            if row["trackable_type"] == 'Weather':
                c.execute("INSERT INTO weathers VALUES (?, ?)",
                          (row["trackable_id"], row["trackable_name"]))
                c.execute("INSERT INTO users_weathers VALUES (?, ?)",
                          (row["user_id"], row["trackable_id"]))
        conn.commit()
    conn.close()
    sys.stdout.flush()


def queries():
   conn = sqlite3.connect('database.db')
   conn.text_factory = str
   c = conn.cursor()

   c.execute("SELECT ")


if __name__ == "__main__":
    filteringData()
    queries()
