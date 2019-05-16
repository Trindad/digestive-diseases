from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import optparse
import subprocess
import random

import sqlite3
import csv

# BHQ010 - Bowel leakage consisted of gas?
# BHQ020 - Bowel leakage consisted of mucus?
# BHQ030 - Bowel leakage consisted of liquid?
# BHQ040 - Bowel leakage consisted of solid stool?
# BHD050 - How often have bowel movements?
# BHQ060 - Common Stool Type
# BHQ070 - Had an urgent need to empty bowels?
# BHQ080 - In past 12 months been constipated?
# BHQ090 - In past 12 months had diarrhea?
# BHQ100 - In past 30 days taken laxative?
def bowelTable(c, conn):
   c.execute("DROP TABLE IF EXISTS bowel_health")
   conn.commit()
   c.execute('''CREATE TABLE bowel_health(
      SEQN int, 
      BHQ010 integer,
      BHQ010_b integer DEFAULT 0, 
      BHQ020 integer,
      BHQ020_b integer DEFAULT 0, 
      BHQ030 integer,
      BHQ030_b integer DEFAULT 0, 
      BHQ040 integer,
      BHQ040_b integer DEFAULT 0, 
      BHD050 integer,
      BHD050_b integer DEFAULT 0, 
      BHQ060 integer,
      BHQ060_b integer DEFAULT 0, 
      BHQ070 integer,
      BHQ070_b integer DEFAULT 0, 
      BHQ080 integer,
      BHQ080_b integer DEFAULT 0, 
      BHQ090 integer,
      BHQ090_b integer DEFAULT 0, 
      BHQ100 integer,
      BHQ100_b integer DEFAULT 0,
      BHQ110 integer,
      BHQ110_b integer DEFAULT 0
   )''')
   
   with open('BHQ_F.csv', mode='r') as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
         c.execute("SELECT SEQN FROM bowel_health WHERE SEQN = '" +row["SEQN"] + "'")
         if c.fetchone() == None:
            
            c.execute("INSERT INTO bowel_health (SEQN, BHQ010, BHQ020, BHQ030, BHQ040, BHD050, BHQ060, BHQ070, BHQ080, BHQ090, BHQ100, BHQ110) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
               row["SEQN"], row["BHQ010"], row["BHQ020"], row["BHQ030"],
               row["BHQ040"], row["BHD050"], row["BHQ060"], row["BHQ070"], row["BHQ080"], row["BHQ090"], row["BHQ100"], row["BHQ110"]))
   conn.commit()


# DBQ010 - Ever breastfed or fed breastmilk
# DBD030 - Age stopped breastfeeding(days)
# DBD041 - Age first fed formula(days)
# DBD050 - Age stopped receiving formula(days)
# DBD055 - Age started other than breastmilk/fomula
# DBD061 - Age first fed milk(days)
# DBQ073A - Type of milk first fed - whole milk
# DBQ073D - Type of milk first fed - fat free milk
# DBQ073E - Type of milk first fed - soy milk
# DBQ073U - Type of milk first fed - other
# DBQ700 - How healthy is the diet
# DBQ197 - Past 30 day milk product consumption
# DBQ223A - You drink whole or regular milk
# DBQ223U - You drink another type of milk
# DBQ229 - Regular milk use 5 times per week
# DBQ235A - How often drank milk age 5-12
# DBQ235B - How often drank milk age 13-17
# DBQ235C - How often drank milk age 18-35
# DBQ424 - Summer program meal free/reduced price
# DBD895 -  # of meals not home prepared
# DBD900 -  # of meals from fast food or pizza place
# DBD905 -  # of ready-to-eat foods in past 30 days
# DBD910 -  # of frozen meals/pizza in past 30 days
# DBQ915 - Self-perceived vegetarian
# DBQ920 - Having food allergies
# DBQ925a - Allergic to wheat
# DBQ925b - Allergic to cow's milk
# DBQ925c - Allergic to eggs
# DBQ925d - Allergic to fish
# DBQ925e - Allergic to shellfish
# DBQ925f - Allergic to corn
# DBQ925g - Allergic to peanut
# DBQ925h - Allergic to other nuts
# DBQ925i - Allergic to soy products
# DBQ925j - Allergic to other foods
# DBQ930 - Main meal planner/preparer
# DBQ935 - Shared meal planning/preparing duty
# DBQ940 - Main food shopper
# DBQ945 - Shared food shopping duty
def nutritionTable(c, conn):
   c.execute("DROP TABLE IF EXISTS nutrition")
   conn.commit()
   c.execute('''CREATE TABLE IF NOT EXISTS nutrition(
      SEQN integer, 
      DBQ010 integer, 
      DBQ010_b integer DEFAULT 0,
      DBD030 integer, 
      DBD030_b integer DEFAULT 0,
      DBD041 integer, 
      DBD041_b integer DEFAULT 0,
      DBD050 integer, 
      DBD050_b integer DEFAULT 0,
      DBD055 integer, 
      DBD055_b integer DEFAULT 0,
      DBD061 integer, 
      DBD061_b integer DEFAULT 0,
      DBQ073A integer, 
      DBQ073A_b integer DEFAULT 0,
      DBQ073D integer, 
      DBQ073D_b integer DEFAULT 0,
      DBQ073E integer, 
      DBQ073E_b integer DEFAULT 0,
      DBQ073U integer, 
      DBQ073U_b integer DEFAULT 0,
      DBQ700 integer, 
      DBQ700_b integer DEFAULT 0,
      DBQ197 integer, 
      DBQ197_b integer DEFAULT 0,
      DBQ223A integer, 
      DBQ223A_b integer DEFAULT 0,
      DBQ223U integer, 
      DBQ223U_b integer DEFAULT 0,
      DBQ229 integer, 
      DBQ229_b integer DEFAULT 0,
      DBQ235A integer, 
      DBQ235A_b integer DEFAULT 0,
      DBQ235B integer, 
      DBQ235B_b integer DEFAULT 0,
      DBQ235C integer,
      DBQ235C_b integer DEFAULT 0,
      DBQ424  integer, 
      DBQ424_b integer DEFAULT 0,
      DBD895 integer, 
      DBD895_b integer DEFAULT 0,
      DBD900 integer, 
      DBD900_b integer DEFAULT 0,
      DBD905 integer, 
      DBD905_b integer DEFAULT 0,
      DBD910 integer, 
      DBD910_b integer DEFAULT 0,
      DBQ915 integer, 
      DBQ915_b integer DEFAULT 0,
      DBQ920 integer, 
      DBQ920_b integer DEFAULT 0,
      DBQ925A integer, 
      DBQ925A_b integer DEFAULT 0,
      DBQ925B integer, 
      DBQ925B_b integer DEFAULT 0,
      DBQ925C integer, 
      DBQ925C_b integer DEFAULT 0,
      DBQ925D integer, 
      DBQ925D_b integer DEFAULT 0,
      DBQ925E integer, 
      DBQ925E_b integer DEFAULT 0,
      DBQ925F integer, 
      DBQ925F_b integer DEFAULT 0,
      DBQ925G integer, 
      DBQ925G_b integer DEFAULT 0,
      DBQ925H integer,
      DBQ925H_b integer DEFAULT 0,
      DBQ925I integer, 
      DBQ925I_b integer DEFAULT 0,
      DBQ925J integer, 
      DBQ925J_b integer DEFAULT 0,
      DBQ930 integer, 
      DBQ930_b integer DEFAULT 0,
      DBQ935 integer, 
      DBQ935_b integer DEFAULT 0,
      DBQ940 integer, 
      DBQ940_b integer DEFAULT 0,
      DBQ945 integer,
      DBQ945_b integer DEFAULT 0
   )''')

   with open('DBQ_F.csv', mode='r') as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
         c.execute("SELECT SEQN FROM nutrition WHERE SEQN = '" +
                   row["SEQN"] + "'")
         if c.fetchone() == None:
            c.execute("INSERT INTO nutrition (SEQN, DBQ010, DBD030, DBD041, DBD050, DBD055, DBD061, DBQ073A, DBQ073D, DBQ073E, DBQ073U, DBQ700, DBQ197, DBQ223A, DBQ223U, DBQ229, DBQ235A, DBQ235B, DBQ235C, DBQ424, DBD895, DBD900, DBD905, DBD910, DBQ915, DBQ920, DBQ925A, DBQ925B, DBQ925C, DBQ925D, DBQ925E, DBQ925F, DBQ925G, DBQ925H, DBQ925I, DBQ925J, DBQ930, DBQ935, DBQ940, DBQ945) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
               row["SEQN"],   row["DBQ010"], row["DBD030"], row["DBD041"], row["DBD050"], row["DBD055"], row["DBD061"],
               row["DBQ073A"], row["DBQ073D"], row["DBQ073E"], row["DBQ073U"], row["DBQ700"],
               row["DBQ197"], row["DBQ223A"], row["DBQ223U"], row["DBQ229"], row["DBQ235A"], 
               row["DBQ235B"], row["DBQ235C"], row["DBQ424"], row["DBD895"], row["DBD900"], 
               row["DBD905"], row["DBD910"], row["DBQ915"], row["DBQ920"], row["DBQ925A"], row["DBQ925B"], 
               row["DBQ925C"], row["DBQ925D"], row["DBQ925E"], row["DBQ925F"], row["DBQ925G"], row["DBQ925H"],
               row["DBQ925I"], row["DBQ925J"], row["DBQ930"], row["DBQ935"], row["DBQ940"], row["DBQ945"]))
   conn.commit()


# SEQN - Respondent sequence number
# CBD010 - Anyone in the family on a special diet
# CBQ020 - Fruits available at home
# CBQ030 - Dark green vegetables available at home
# CBQ040 - Salty snacks available at home
# CBQ050 - Fat-free/low fat milk available at home
# CBQ060 - Soft drinks available at home
# CBD070 - Money spent at supermarket/grocery store
# CBD120 - Money spent on eating out
# CBD130 - Money spent on carryout/delivered foods
# CBQ140 - How often do you do major food shopping
# CBD150 - Time to get to grocery store
# CBD160 -  # of times someone cooked dinner at home
def consumerBehaviorTable(c, conn):
   c.execute("DROP TABLE IF EXISTS consumer_behavior")
   conn.commit()
   c.execute('''CREATE TABLE IF NOT EXISTS consumer_behavior(
      SEQN integer, 
      CBD010 integer, 
      CBD010_b integer DEFAULT 0,
      CBQ020 integer, 
      CBQ020_b integer DEFAULT 0,
      CBQ030 integer, 
      CBQ030_b integer DEFAULT 0,
      CBQ040 integer, 
      CBQ040_b integer DEFAULT 0,
      CBQ050 integer, 
      CBQ050_b integer DEFAULT 0,
      CBQ060 integer, 
      CBQ060_b integer DEFAULT 0,
      CBD070 integer, 
      CBD070_b integer DEFAULT 0,
      CBD120 integer,
      CBD120_b integer DEFAULT 0,
      CBD130 integer, 
      CBD130_b integer DEFAULT 0,
      CBQ140 integer, 
      CBQ140_b integer DEFAULT 0,
      CBD150 integer, 
      CBD150_b integer DEFAULT 0,
      CBD160 integer,
      CBD160_b integer DEFAULT 0
   )''')

   with open('CBQ_F.csv', mode='r') as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
         c.execute("SELECT SEQN FROM consumer_behavior WHERE SEQN = '" +
                   row["SEQN"] + "'")
         if c.fetchone() == None:
            # print(row)
            c.execute("INSERT INTO consumer_behavior (SEQN, CBD010, CBQ020, CBQ030, CBQ040, CBQ050, CBQ060, CBD070, CBD120, CBD130, CBQ140, CBD150, CBD160) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                row["SEQN"],   row["CBD010"], row["CBQ020"], row["CBQ030"], row["CBQ040"],  
                row["CBQ050"], row["CBQ060"], row["CBD070"], row["CBD120"], 
                row["CBD130"], row["CBQ140"], row["CBD150"], row["CBD160"]))
   conn.commit()


# SEQN - Respondent sequence number
# ALQ101 - Had at least 12 alcohol drinks/1 yr?
# ALQ110 - Had at least 12 alcohol drinks/lifetime?
# ALQ120Q - How often drink alcohol over past 12 mos
# ALQ120U -  # days drink alcohol per wk, mo, yr
# ALQ130 - Avg  # alcoholic drinks/day -past 12 mos
# ALQ140Q -  # days have 5 or more drinks/past 12 mos
# ALQ140U -  # days per week, month, year?
# ALQ150 - Ever have 5 or more drinks every day?
def alcoholUseTable(c, conn):
   c.execute("DROP TABLE IF EXISTS alcohol_use")
   conn.commit()
   c.execute('''CREATE TABLE alcohol_use(
      SEQN int,
      ALQ101 integer,
      ALQ101_b integer DEFAULT 0,
      ALQ110 integer,
      ALQ110_b integer DEFAULT 0,
      ALQ120Q integer,
      ALQ120Q_b integer DEFAULT 0,
      ALQ120U integer,
      ALQ120U_b integer DEFAULT 0,
      ALQ130 integer,
      ALQ130_b integer DEFAULT 0,
      ALQ140Q integer,
      ALQ140Q_b integer DEFAULT 0,
      ALQ140U integer,
      ALQ140U_b integer DEFAULT 0,
      ALQ150 integer,
      ALQ150_b integer DEFAULT 0
   )''')

   with open('ALQ_F.csv', mode='r') as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
         c.execute("SELECT SEQN FROM alcohol_use WHERE SEQN = '" +
                   row["SEQN"] + "'")
         if c.fetchone() == None:

            c.execute("INSERT INTO alcohol_use (SEQN, ALQ101, ALQ110, ALQ120Q, ALQ120U, ALQ130 , ALQ140Q , ALQ140U , ALQ150) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                row["SEQN"], row["ALQ101"], row["ALQ110"], row["ALQ120Q"], row["ALQ120U"], row["ALQ130"], row["ALQ140Q"], row["ALQ140U"], row["ALQ150"]))
   conn.commit()

def updateRegisters(c):

   c.execute("UPDATE bowel_health SET BHQ010_b = 1 WHERE BHQ010 BETWEEN 1 AND 5")
   c.execute("UPDATE bowel_health SET BHQ020_b = 1 WHERE BHQ020 BETWEEN 1 AND 5")
   c.execute("UPDATE bowel_health SET BHQ030_b = 1 WHERE BHQ030 BETWEEN 1 AND 5")
   c.execute("UPDATE bowel_health SET BHQ040_b = 1 WHERE BHQ040 BETWEEN 1 AND 5")
   c.execute("UPDATE bowel_health SET BHD050_b = 1 WHERE BHD050 BETWEEN 1 AND 63")
   c.execute("UPDATE bowel_health SET BHQ060_b = 1 WHERE BHQ060 IN (1,2,5,6,7)")
   c.execute("UPDATE bowel_health SET BHQ070_b = 1 WHERE BHQ070 BETWEEN 1 AND 3")
   c.execute("UPDATE bowel_health SET BHQ080_b = 1 WHERE BHQ080 BETWEEN 1 AND 2")
   c.execute("UPDATE bowel_health SET BHQ090_b = 1 WHERE BHQ090 BETWEEN 1 AND 3")
   c.execute("UPDATE bowel_health SET BHQ010_b = 1 WHERE BHQ010 = 1")
   c.execute("UPDATE bowel_health SET BHQ110_b = 1 WHERE BHQ110 BETWEEN 1 AND 4")


   c.execute("UPDATE nutrition SET DBQ010_b = 1 WHERE DBQ010 = 2")
   # c.execute("UPDATE nutrition SET DBD030_b = 1 WHERE DBD030 IN ()")
   c.execute("UPDATE nutrition SET DBD041_b = 1 WHERE DBD041 = 0")
   # c.execute("UPDATE nutrition SET DBD050_b = 1 WHERE DBD050 IN ()")
   # c.execute("UPDATE nutrition SET DBD055_b = 1 WHERE DBD055 IN ()")
   c.execute("UPDATE nutrition SET DBD061_b = 1 WHERE DBD061 BETWEEN 0 AND 1825")
   c.execute("UPDATE nutrition SET DBQ073A_b = 1 WHERE DBQ073A BETWEEN 10 AND 77")
   c.execute("UPDATE nutrition SET DBQ073D_b = 1 WHERE DBQ073D IN (13)")
   c.execute("UPDATE nutrition SET DBQ073E_b = 1 WHERE DBQ073E IN (14)")
   c.execute("UPDATE nutrition SET DBQ073U_b = 1 WHERE DBQ073U IN (30)")
   c.execute("UPDATE nutrition SET DBQ700_b = 1 WHERE DBQ700 > 3")
   c.execute("UPDATE nutrition SET DBQ197_b = 1 WHERE DBQ197 > 1")
   c.execute("UPDATE nutrition SET DBQ223A_b = 1 WHERE DBQ223A IN (10)")
   c.execute("UPDATE nutrition SET DBQ223U_b = 1 WHERE DBQ223U IN (30)")
   c.execute("UPDATE nutrition SET DBQ229_b = 1 WHERE DBQ229 < 7")
   c.execute("UPDATE nutrition SET DBQ235A_b = 1 WHERE DBQ235A < 7")
   # c.execute("UPDATE nutrition SET DBQ235B_b = 1 WHERE DBQ235B IN ()")
   # c.execute("UPDATE nutrition SET DBQ235C_b = 1 WHERE DBQ235C IN ()")
   # c.execute("UPDATE nutrition SET DBQ424_b = 1 WHERE DBQ424 IN ()")
   c.execute("UPDATE nutrition SET DBD895_b = 1 WHERE DBD895 BETWEEN 1 AND 21")
   c.execute("UPDATE nutrition SET DBD900_b = 1 WHERE DBD900 BETWEEN 1 AND 5555")
   c.execute("UPDATE nutrition SET DBD905_b = 1 WHERE DBD905 BETWEEN 1 AND 150")
   c.execute("UPDATE nutrition SET DBD910_b = 1 WHERE DBD910 BETWEEN 1 AND 180")
   # c.execute("UPDATE nutrition SET DBQ915_b = 1 WHERE DBQ915 IN ()")
   c.execute("UPDATE nutrition SET DBQ920_b = 1 WHERE DBQ920 IN (1)")
   c.execute("UPDATE nutrition SET DBQ925A_b = 1 WHERE DBQ925A IN (10)")
   c.execute("UPDATE nutrition SET DBQ925B_b = 1 WHERE DBQ925B IN (11)")
   c.execute("UPDATE nutrition SET DBQ925C_b = 1 WHERE DBQ925C IN (12)")
   c.execute("UPDATE nutrition SET DBQ925D_b = 1 WHERE DBQ925D IN (13)")
   c.execute("UPDATE nutrition SET DBQ925E_b = 1 WHERE DBQ925E IN (14)")
   c.execute("UPDATE nutrition SET DBQ925F_b = 1 WHERE DBQ925F IN (15)")
   c.execute("UPDATE nutrition SET DBQ925G_b = 1 WHERE DBQ925G IN (16)")
   c.execute("UPDATE nutrition SET DBQ925H_b = 1 WHERE DBQ925H IN (17)")
   c.execute("UPDATE nutrition SET DBQ925I_b = 1 WHERE DBQ925I IN (18)")
   c.execute("UPDATE nutrition SET DBQ925J_b = 1 WHERE DBQ925J IN (19)")
   c.execute("UPDATE nutrition SET DBQ930_b = 1 WHERE DBQ930 IN (2)")
   c.execute("UPDATE nutrition SET DBQ935_b = 1 WHERE DBQ935 IN (2)")
   # c.execute("UPDATE nutrition SET DBQ940_b = 1 WHERE DBQ940 IN ()")
   # c.execute("UPDATE nutrition SET DBQ945_b = 1 WHERE DBQ945 IN ()")

   c.execute("UPDATE consumer_behavior SET CBD010_b = 1 WHERE CBD010 = 1")
   c.execute("UPDATE consumer_behavior SET CBQ020_b = 1 WHERE CBQ020 IN (4,5)")
   c.execute("UPDATE consumer_behavior SET CBQ030_b = 1 WHERE CBQ030 IN (4,5)")
   c.execute("UPDATE consumer_behavior SET CBQ040_b = 1 WHERE CBQ040 IN (1,2,3)")
   c.execute("UPDATE consumer_behavior SET CBQ050_b = 1 WHERE CBQ050 IN (4,5)")
   c.execute("UPDATE consumer_behavior SET CBQ060_b = 1 WHERE CBQ060 IN (1,2,3)")
   # c.execute("UPDATE consumer_behavior SET CBD120_b = 1 WHERE CBD120 IN ()")
   # c.execute("UPDATE consumer_behavior SET CBD130_b = 1 WHERE CBD130 IN ()")
   c.execute("UPDATE consumer_behavior SET CBQ140_b = 1 WHERE CBQ140 = 6")
   # c.execute("UPDATE consumer_behavior SET CBD150_b = 1 WHERE CBD150 IN ()")
   c.execute("UPDATE consumer_behavior SET CBD160_b = 1 WHERE CBD160 < 3")

   c.execute("UPDATE alcohol_use SET ALQ101_b = 1 WHERE ALQ101 = 1")
   # c.execute("UPDATE alcohol_use SET ALQ110_b = 1 WHERE ALQ110 = 1")
   c.execute("UPDATE alcohol_use SET ALQ120Q_b = 1 WHERE ALQ120Q BETWEEN 1 AND 366")
   c.execute("UPDATE alcohol_use SET ALQ120U_b = 1 WHERE ALQ120U < 4")
   c.execute("UPDATE alcohol_use SET ALQ130_b = 1 WHERE ALQ130 BETWEEN 1 AND 36")
   c.execute("UPDATE alcohol_use SET ALQ140Q_b = 1 WHERE ALQ140Q BETWEEN 1 AND 304")
   c.execute("UPDATE alcohol_use SET ALQ140U_b = 1 WHERE ALQ140U < 4")
   c.execute("UPDATE alcohol_use SET ALQ150_b = 1 WHERE ALQ150 = 1")

def queries(c):

   # c.execute("CREATE INDEX seqn_bowel on bowel_health (SEQN)")
   # c.execute("CREATE INDEX seqn_nutrition on nutrition (SEQN)")
   # c.execute("CREATE INDEX seqn_consumer on consumer_behavior (SEQN)")
   # c.execute("CREATE INDEX seqn_alcohol on alcohol_use (SEQN)")

   # for row in c.execute("select count(bowel_health.SEQN) from nutrition, bowel_health where nutrition.SEQN = bowel_health.SEQN"):
   #    print(row)
   # for row in c.execute("select count(nutrition.SEQN) from consumer_behavior, nutrition where nutrition.SEQN = consumer_behavior.SEQN"):
   #    print(row)
   # for row in c.execute("select * from consumer_behavior, bowel_health where consumer_behavior.SEQN = bowel_health.SEQN"):
   #    print(row)
   #
   # BHQ010, BHQ020, BHQ030, BHQ040, BHQD50, BHQ060, BHQ070, BHQ080, BHQ090, BHQ0100, BHQ0110, CBD010, CBQ020, CBQ030, CBQ040, CBQ050, CBQ060, CBQ140, CBD160, DBQ010, DBD030, DBD041, DBD050, DBD055, DBD061, DBQ073A, DBQ073D, DBQ073E, DBQ073U, DBQ700, DBQ197, DBQ223A, DBQ223U, DBQ229, DBQ235A, DBQ235B, DBQ235C, DBQ424, DBD895, DBD900, DBD905, DBD910, DBQ915, DBQ920, DBQ925A, DBQ925B, DBQ925C, DBQ925D, DBQ925E, DBQ925F, DBQ925G, DBQ925H, DBQ925I, DBQ925J, DBQ930, DBQ935, DBQ940, DBQ945
   c.execute("DROP TABLE IF EXISTS patients")
   # c.execute("CREATE TABLE patients as select * from consumer_behavior, bowel_health, alcohol_use where consumer_behavior.SEQN = bowel_health.SEQN") 
   bowel_health = "BHQ010_b, BHQ020_b, BHQ030_b, BHQ040_b, BHD050_b, BHQ060_b, BHQ070_b, BHQ080_b, BHQ090_b, BHQ010_b, BHQ110_b"
   alcohol_use = "ALQ101_b, ALQ120Q_b, ALQ120U_b, ALQ130_b, ALQ140Q_b, ALQ140U_b, ALQ150_b"
   consumer_behavior = "CBD010_b, CBQ020_b, CBQ030_b, CBQ040_b, CBQ050_b, CBQ060_b, CBQ140_b, CBD160_b"
   nutrition = "DBQ010_b, DBD041_b, DBD061_b, DBQ073A_b, DBQ073D_b, DBQ073E_b, DBQ073U_b, DBQ700_b, DBQ197_b, DBQ223A_b, DBQ223U_b, DBQ229_b, DBQ235A_b, DBD895_b, DBD900_b, DBD905_b, DBD910_b, DBQ920_b, DBQ925A_b, DBQ925B_b, DBQ925C_b, DBQ925D_b, DBQ925E_b, DBQ925F_b, DBQ925G_b, DBQ925H_b, DBQ925I_b, DBQ925J_b, DBQ930_b, DBQ935_b"
   
   registers = bowel_health+alcohol_use+consumer_behavior+nutrition

   query = "create table patients as select"+registers+"from consumer_behavior join bowel_health on bowel_health.SEQN = consumer_behavior.SEQN join nutrition on nutrition.SEQN = consumer_behavior.SEQN join alcohol_use on alcohol_use.SEQN = consumer_behavior.SEQN"

   c.execute(query)
   # c.execute("ALTER TABLE patients ADD COLUMN profile string")
   c.execute("CREATE INDEX seqn_patient on patients (SEQN)")

   for row in c.execute("SELECT count(*) from patients"):
      print(row)

def filteringData():
  
   conn = sqlite3.connect('database.db')
   conn.text_factory = str
   c = conn.cursor()

   # bowelTable(c, conn)
   # nutritionTable(c, conn)
   # consumerBehaviorTable(c, conn)
   # alcoholUseTable(c, conn)
   # updateRegisters(c)
   queries(c)
   conn.close()

if __name__ == "__main__":
   filteringData()
   sys.stdout.flush()
