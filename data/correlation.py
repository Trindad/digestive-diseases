from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import optparse
import subprocess
import random

import sqlite3
import csv

def perfil_alcohol_and_bowel(c):

   c.execute("DROP VIEW IF EXISTS profilePattern")
   
   labels = "p.bowel_profile, p.alcohol_profile, p.consumer_profile, p.nutrition_profile"
   
   view_name = "profilePattern"
   view = "CREATE VIEW "+view_name+" AS SELECT count(*) AS nPatients, "+labels+" FROM patients p GROUP BY "
   c.execute(view+" p.alcohol_profile, p.bowel_profile")

   queries = []
   for row in c.execute("SELECT alcohol_profile, bowel_profile, nPatients FROM profilePattern WHERE nPatients >= 1"):
      queries.append(row)
      print(row)
   with open('q1.csv', mode='w') as result_file:
      result_writer = csv.writer(result_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      for query in queries:
         result_writer.writerow(query)


def perfil_nutrition_and_bowel(c):

   c.execute("DROP VIEW IF EXISTS profilePattern")

   labels = "p.bowel_profile, p.alcohol_profile, p.consumer_profile, p.nutrition_profile"

   view_name = "profilePattern"
   view = "CREATE VIEW "+view_name + \
       " AS SELECT count(*) AS nPatients, "+labels+" FROM patients p GROUP BY "
   c.execute(view+" p.nutrition_profile, p.bowel_profile")

   queries = []
   for row in c.execute("SELECT nutrition_profile, bowel_profile, nPatients FROM profilePattern WHERE nPatients >= 1"):
      queries.append(row)
      print(row)
   with open('q2.csv', mode='w') as result_file:
      result_writer = csv.writer(
          result_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      for query in queries:
         result_writer.writerow(query)


def perfil_consumer_and_bowel(c):

   c.execute("DROP VIEW IF EXISTS profilePattern")

   labels = "p.bowel_profile, p.alcohol_profile, p.consumer_profile, p.nutrition_profile"

   view_name = "profilePattern"
   view = "CREATE VIEW "+view_name + \
       " AS SELECT count(*) AS nPatients, "+labels+" FROM patients p GROUP BY "
   c.execute(view+" p.consumer_profile, p.bowel_profile")

   queries = []
   for row in c.execute("SELECT consumer_profile, bowel_profile, nPatients FROM profilePattern WHERE nPatients >= 1"):
      queries.append(row)
      print(row)
   with open('q3.csv', mode='w') as result_file:
      result_writer = csv.writer(
          result_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      for query in queries:
         result_writer.writerow(query)


def perfil_consumer_and_nutrition(c):

   c.execute("DROP VIEW IF EXISTS profilePattern")

   labels = "p.bowel_profile, p.alcohol_profile, p.consumer_profile, p.nutrition_profile"

   view_name = "profilePattern"
   view = "CREATE VIEW "+view_name + \
       " AS SELECT count(*) AS nPatients, "+labels+" FROM patients p GROUP BY "
   c.execute(view+" p.consumer_profile, p.nutrition_profile")

   queries = []
   for row in c.execute("SELECT consumer_profile, nutrition_profile, nPatients FROM profilePattern WHERE nPatients >= 1"):
      queries.append(row)
      print(row)
   with open('q4.csv', mode='w') as result_file:
      result_writer = csv.writer(
          result_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      for query in queries:
         result_writer.writerow(query)

def correlation_profile():

   conn = sqlite3.connect('database.db')
   conn.text_factory = str
   c = conn.cursor()

   # condition =  "WHERE BHQ010_b = 1  OR BHQ020_b = 1  OR  BHQ030_b = 1  OR  BHQ040_b = 1  OR BHD050_b = 1  OR  BHQ060_b = 1  OR  BHQ070_b = 1  OR  BHQ080_b = 1  OR  BHQ090_b = 1  OR  BHQ110_b = 1" 
   perfil_alcohol_and_bowel(c)
   perfil_nutrition_and_bowel(c)
   perfil_consumer_and_bowel(c)
   perfil_consumer_and_nutrition(c)

   conn.commit()

if __name__ == "__main__":
   correlation_profile()
   sys.stdout.flush()
