from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import optparse
import subprocess
import random

import sqlite3
import csv


def correlation_profile():

   conn = sqlite3.connect('database.db')
   conn.text_factory = str
   c = conn.cursor()

   csv_file = "bowel_disease.csv"

   #grouping profiles
   c.execute("DROP VIEW IF EXISTS profilePattern")
  
   # condition =  "WHERE BHQ010_b = 1  OR BHQ020_b = 1  OR  BHQ030_b = 1  OR  BHQ040_b = 1  OR BHD050_b = 1  OR  BHQ060_b = 1  OR  BHQ070_b = 1  OR  BHQ080_b = 1  OR  BHQ090_b = 1  OR  BHQ110_b = 1" 
  
   view = "CREATE VIEW profilePattern AS SELECT count(*) AS nPatients, p.bowel_profile, p.alcohol_profile, p.consumer_profile, p.nutrition_profile FROM patients p GROUP BY p.nutrition_profile, p.bowel_profile"
   c.execute(view)

   queries = []
   for row in c.execute("SELECT nutrition_profile, bowel_profile, nPatients FROM profilePattern WHERE nPatients >= 1"):
      print(row)

if __name__ == "__main__":
   correlation_profile()
   sys.stdout.flush()
