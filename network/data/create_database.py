from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import optparse
import subprocess
import random

import sqlite3
import csv

# SEQN - Respondent sequence number
# BHQ010 - Bowel leakage consisted of gas?
# BHQ020 - Bowel leakage consisted of mucus?
# BHQ030 - Bowel leakage consisted of liquid?
# BHQ040 - Bowel leakage consisted of solid stool?
# BHD050 - How often have bowel movements?
def create_bowel_health_table(c, conn):
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
      BHD050_b integer DEFAULT 0
   )''')

   c.execute("CREATE INDEX seqn_bowel on bowel_health (SEQN)")


def populate_bowel_health_table(c, conn, filename):

   with open(filename, mode='r') as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
         c.execute("SELECT SEQN FROM bowel_health WHERE SEQN = '" +
                   row["SEQN"] + "'")
         if c.fetchone() == None:
            c.execute("INSERT INTO bowel_health (SEQN, BHQ010, BHQ020, BHQ030, BHQ040, BHD050) VALUES(?, ?, ?, ?, ?, ?)", (
                row["SEQN"], row["BHQ010"], row["BHQ020"], row["BHQ030"],
                row["BHQ040"], row["BHD050"]))
   conn.commit()

# SEQN - Respondent sequence number
# DBQ010 - Ever breastfed or fed breastmilk
# DBD030 - Age stopped breastfeeding(days)
# DBQ700 - How healthy is the diet
# DBQ197 - Past 30 day milk product consumption
# DBD222A - You drink whole or regular milk
# DBD222U - You drink another type of milk
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
def create_nutrition_table(c, conn):
   c.execute("DROP TABLE IF EXISTS nutrition")
   conn.commit()
   c.execute('''CREATE TABLE IF NOT EXISTS nutrition(
      SEQN integer, 
      DBQ010 integer, 
      DBQ010_b integer DEFAULT 0,
      DBD030 integer, 
      DBD030_b integer DEFAULT 0,
      DBQ700 integer, 
      DBQ700_b integer DEFAULT 0,
      DBQ197 integer, 
      DBQ197_b integer DEFAULT 0,
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
      DBQ925J_b integer DEFAULT 0
   )''')

   c.execute("CREATE INDEX seqn_nutrition on nutrition (SEQN)")


def populate_nutrition_table(c, conn, filename):

   with open(filename, mode='r') as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
         c.execute("SELECT SEQN FROM nutrition WHERE SEQN = '" +
                   row["SEQN"] + "'")
         if c.fetchone() == None:
            c.execute("INSERT INTO nutrition (SEQN, DBQ010, DBD030, DBQ700, DBQ197, DBQ229, DBQ235A, DBQ235B, DBQ235C, DBQ424, DBD895, DBD900, DBD905, DBD910, DBQ915, DBQ920, DBQ925A, DBQ925B, DBQ925C, DBQ925D, DBQ925E, DBQ925F, DBQ925G, DBQ925H, DBQ925I, DBQ925J) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                row["SEQN"],   row["DBQ010"], row["DBD030"],  row["DBQ700"],
                row["DBQ197"], row["DBQ229"], row["DBQ235A"],
                row["DBQ235B"], row["DBQ235C"], row["DBQ424"], row["DBD895"], row["DBD900"],
                row["DBD905"], row["DBD910"], row["DBQ915"], row["DBQ920"], row["DBQ925A"], row["DBQ925B"],
                row["DBQ925C"], row["DBQ925D"], row["DBQ925E"], row["DBQ925F"], row["DBQ925G"], row["DBQ925H"],
                row["DBQ925I"], row["DBQ925J"]))
   conn.commit()


# SEQN - Respondent sequence number
# HSQ470 - no. of days physical health was not good
# HSQ480 - no. of days mental health was not good
# HSQ490 - inactive days due to phys./mental hlth
# HSQ493 - Pain make it hard for usual activities
# HSQ496 - How many days feel anxious
# HSQ500 - SP have head cold or chest cold
# HSQ510 - SP have stomach or intestinal illness?
def create_current_health_status_table(c, conn):

   c.execute("DROP TABLE IF EXISTS health_status")
   conn.commit()
   c.execute('''CREATE TABLE IF NOT EXISTS health_status(
         SEQN integer,
         HSQ470 integer,
         HSQ470_b integer DEFAULT 0,
         HSQ480 integer,
         HSQ480_b integer DEFAULT 0,
         HSQ490 integer,
         HSQ490_b integer DEFAULT 0,
         HSQ493 integer,
         HSQ493_b integer DEFAULT 0,
         HSQ496 integer,
         HSQ496_b integer DEFAULT 0,
         HSQ500 integer,
         HSQ500_b integer DEFAULT 0,
         HSQ510 integer,
         HSQ510_b integer DEFAULT 0)''')

   c.execute("CREATE INDEX seqn_health on health_status (SEQN)")

def populate_current_health_status_table(c, conn, filename):

   with open(filename, mode='r') as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
         c.execute("SELECT SEQN FROM health_status WHERE SEQN = '" +
                   row["SEQN"] + "'")
         if c.fetchone() == None:
            c.execute("INSERT INTO health_status(SEQN, HSQ470, HSQ480, HSQ490, HSQ493, HSQ496, HSQ500, HSQ510) VALUES(?, ?, ?, ?, ?, ?, ?, ?)", (row["SEQN"],
                  row["HSQ470"], row["HSQ480"], row["HSQ490"], row["HSQ493"], row["HSQ496"], row["HSQ500"], row["HSQ510"]))

   conn.commit()

# SEQN - Respondent sequence number
# CBD010 - Anyone in the family on a special diet
# CBQ020 - Fruits available at home
# CBQ030 - Dark green vegetables available at home
# CBQ040 - Salty snacks available at home
# CBQ050 - Fat-free/low fat milk available at home
# CBQ060 - Soft drinks available at home
def create_consumer_behavior_table(c, conn):
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
      CBQ060_b integer DEFAULT 0
   )''')

   c.execute("CREATE INDEX seqn_consumer on consumer_behavior (SEQN)")


def populate_consumer_behavior_table(c, conn, filename):

   with open(filename, mode='r') as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
         c.execute("SELECT SEQN FROM consumer_behavior WHERE SEQN = '" +
                   row["SEQN"] + "'")
         if c.fetchone() == None:
            # print(row)
            c.execute("INSERT INTO consumer_behavior (SEQN, CBD010, CBQ020, CBQ030, CBQ040, CBQ050, CBQ060) VALUES (?, ?, ?, ?, ?, ?, ?)", (
                row["SEQN"],   row["CBD010"], row["CBQ020"], row["CBQ030"], row["CBQ040"],
                row["CBQ050"], row["CBQ060"]))
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
def create_alcohol_use_table(c, conn):
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

   c.execute("CREATE INDEX seqn_alcohol on alcohol_use (SEQN)")


def populate_alcohol_use_table(c, conn, filename):

   with open(filename, mode='r') as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
         c.execute("SELECT SEQN FROM alcohol_use WHERE SEQN = '" +
                   row["SEQN"] + "'")
         if c.fetchone() == None:

            c.execute("INSERT INTO alcohol_use (SEQN, ALQ101, ALQ110, ALQ120Q, ALQ120U, ALQ130 , ALQ140Q , ALQ140U , ALQ150) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                row["SEQN"], row["ALQ101"], row["ALQ110"], row["ALQ120Q"], row["ALQ120U"], row["ALQ130"], row["ALQ140Q"], row["ALQ140U"], row["ALQ150"]))
   conn.commit()


# SEQN, LBXVIDMS, LBDVIDLC, LBXVD2MS, LBDVD2LC, LBXVD3MS, LBDVD3LC, LBXVE3MS, LBDVE3LC
def create_current_vitamin_D_table(c, conn):
   c.execute("DROP TABLE IF EXISTS vitamin_d")
   conn.commit()
   c.execute('''CREATE TABLE vitamin_d(
      SEQN int,
      LBXVIDMS integer,
      LBXVIDMS_b integer DEFAULT 0,
      LBDVIDLC integer,
      LBDVIDLC_b integer DEFAULT 0,
      LBXVD2MS integer,
      LBXVD2MS_b integer DEFAULT 0,
      LBDVD2LC integer,
      LBDVD2LC_b integer DEFAULT 0,
      LBXVD3MS integer,
      LBXVD3MS_b integer DEFAULT 0,
      LBDVD3LC integer,
      LBDVD3LC_b integer DEFAULT 0,
      LBXVE3MS integer,
      LBXVE3MS_b integer DEFAULT 0,
      LBDVE3LC integer,
      LBDVE3LC_b integer DEFAULT 0
          )''')

   c.execute("CREATE INDEX seqn_vitamin_d on vitamin_d (SEQN)")

def populate_current_vitamin_D_table(c, conn, filename):
   with open(filename, mode='r') as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
         c.execute("SELECT SEQN FROM vitamin_d WHERE SEQN = '" +
                   row["SEQN"] + "'")
         if c.fetchone() == None:

            c.execute("INSERT INTO vitamin_d (SEQN, LBXVIDMS, LBDVIDLC, LBXVD2MS, LBDVD2LC, LBXVD3MS, LBDVD3LC, LBXVE3MS, LBDVE3LC) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                row["SEQN"], row["LBXVIDMS"], row["LBDVIDLC"], row["LBXVD2MS"], row["LBDVD2LC"], row["LBXVD3MS"], row["LBDVD3LC"], row["LBXVE3MS"], row["LBDVE3LC"]))
   conn.commit()

# SEQN - Respondent sequence number
# LBXFER - Ferritin(ng/mL)
# LBDFERSI - Ferritin(ug/L)
def create_current_ferritin_table(c, conn):

   c.execute("DROP TABLE IF EXISTS ferritin")
   conn.commit()
   c.execute('''CREATE TABLE ferritin(
      SEQN int,
      LBXFER integer,
      LBXFER_b integer DEFAULT 0
   )''')

   c.execute("CREATE INDEX seqn_ferritin on ferritin (SEQN)")

def populate_current_ferritin_table(c, conn, filename):
   
   with open(filename, mode='r') as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
         c.execute("SELECT SEQN FROM ferritin WHERE SEQN = '" +
                   row["SEQN"] + "'")
         if c.fetchone() == None:
            c.execute("INSERT INTO ferritin(SEQN, LBXFER) VALUES(?, ?)", (
               row["SEQN"], row["LBXFER"]))
   conn.commit()


# SEQN - Respondent sequence number
# SDDSRVYR - Data Release Number
# RIDSTATR - Interview/Examination Status
# RIDEXMON - Six month time period
# RIAGENDR - Gender
# RIDAGEYR - Age at Screening Adjudicated - Recode
# RIDAGEMN - Age in Months at Screening - Recode
# RIDAGEEX - Age in Months at Exam - Recode
# RIDRETH1 - Race/Ethnicity - Recode
# DMQMILIT - Served in the US Armed Forces
# DMDBORN2 - Country of Birth - Recode
# DMDCITZN - Citizenship Status
# DMDYRSUS - Length of time in US
# DMDEDUC3 - Education Level - Children/Youth 6-19
# DMDEDUC2 - Education Level - Adults 20+
# DMDSCHOL - Now attending school?
# DMDMARTL - Marital Status
# DMDHHSIZ - Total number of people in the Household
# DMDFMSIZ - Total number of people in the Family
# INDHHIN2 - Annual Household Income
# INDFMIN2 - Annual Family Income
# INDFMPIR - Ratio of family income to poverty
# RIDEXPRG - Pregnancy Status at Exam - Recode
# DMDHRGND - HH Ref Person Gender
# DMDHRAGE - HH Ref Person Age
# DMDHRBR2 - HH Ref Person Country of Birth
# DMDHREDU - HH Ref Person Education Level
# DMDHRMAR - HH Ref Person Marital Status
# DMDHSEDU - HH Ref Person's Spouse Education Level
# SIALANG - Language of SP Interview
# SIAPROXY - Proxy used in SP Interview?
# SIAINTRP - Interpreter used in SP Interview?
# FIALANG - Language of Family Interview
# FIAPROXY - Proxy used in Family Interview?
# FIAINTRP - Interpreter used in Family Interview?
# MIALANG - Language of MEC Interview
# MIAPROXY - Proxy used in MEC Interview?
# MIAINTRP - Interpreter used in MEC Interview?
# AIALANG - Language of ACASI Interview
# WTINT2YR - Full Sample 2 Year Interview Weight
# WTMEC2YR - Full Sample 2 Year MEC Exam Weight
# SDMVPSU - Masked Variance Pseudo-PSU
# SDMVSTRA - Masked Variance Pseudo-Stratum
def create_basic_information_table(c, conn):

   c.execute("DROP TABLE IF EXISTS basic_information")
   conn.commit()
   c.execute('''CREATE TABLE basic_information(
      SEQN int,
      RIAGENDR integer
   )''')

   c.execute("CREATE INDEX seqn_info on basic_information (SEQN)")


def populate_basic_information_table(c, conn, filename):
   with open(filename, mode='r') as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
         c.execute("SELECT SEQN FROM basic_information WHERE SEQN = '" +
                   row["SEQN"] + "'")
         if c.fetchone() == None:
            c.execute("INSERT INTO basic_information(SEQN, RIAGENDR) VALUES(?, ?)", (row["SEQN"], row["RIAGENDR"]))
   conn.commit()

def updateRegisters(c, conn):

   c.execute("UPDATE bowel_health SET BHQ010_b = 1 WHERE BHQ010 BETWEEN 1 AND 5")
   c.execute("UPDATE bowel_health SET BHQ020_b = 1 WHERE BHQ020 BETWEEN 1 AND 5")
   c.execute("UPDATE bowel_health SET BHQ030_b = 1 WHERE BHQ030 BETWEEN 1 AND 5")
   c.execute("UPDATE bowel_health SET BHQ040_b = 1 WHERE BHQ040 BETWEEN 1 AND 5")
   c.execute("UPDATE bowel_health SET BHD050_b = 1 WHERE BHD050 BETWEEN 1 AND 63")

   c.execute("UPDATE nutrition SET DBQ010_b = 1 WHERE DBQ010 = 2")
   c.execute("UPDATE nutrition SET DBQ700_b = 1 WHERE DBQ700 > 3")
   c.execute("UPDATE nutrition SET DBQ197_b = 1 WHERE DBQ197 > 1")
   c.execute("UPDATE nutrition SET DBQ229_b = 1 WHERE DBQ229 < 7")
   c.execute("UPDATE nutrition SET DBQ235A_b = 1 WHERE DBQ235A < 7")
   c.execute("UPDATE nutrition SET DBD895_b = 1 WHERE DBD895 BETWEEN 1 AND 21")
   c.execute("UPDATE nutrition SET DBD900_b = 1 WHERE DBD900 BETWEEN 1 AND 5555")
   c.execute("UPDATE nutrition SET DBD905_b = 1 WHERE DBD905 BETWEEN 1 AND 150")
   c.execute("UPDATE nutrition SET DBD910_b = 1 WHERE DBD910 BETWEEN 1 AND 180")
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

   c.execute("UPDATE consumer_behavior SET CBD010_b = 1 WHERE CBD010 = 1")
   c.execute("UPDATE consumer_behavior SET CBQ020_b = 1 WHERE CBQ020 IN (4,5)")
   c.execute("UPDATE consumer_behavior SET CBQ030_b = 1 WHERE CBQ030 IN (4,5)")
   c.execute("UPDATE consumer_behavior SET CBQ040_b = 1 WHERE CBQ040 IN (1,2,3)")
   c.execute("UPDATE consumer_behavior SET CBQ050_b = 1 WHERE CBQ050 IN (4,5)")
   c.execute("UPDATE consumer_behavior SET CBQ060_b = 1 WHERE CBQ060 IN (1,2,3)")

   c.execute("UPDATE alcohol_use SET ALQ101_b = 1 WHERE ALQ101 = 1")
   c.execute("UPDATE alcohol_use SET ALQ120Q_b = 1 WHERE ALQ120Q BETWEEN 1 AND 366")
   c.execute("UPDATE alcohol_use SET ALQ120U_b = 1 WHERE ALQ120U < 4")
   c.execute("UPDATE alcohol_use SET ALQ130_b = 1 WHERE ALQ130 BETWEEN 1 AND 36")
   c.execute("UPDATE alcohol_use SET ALQ140Q_b = 1 WHERE ALQ140Q BETWEEN 1 AND 304")
   c.execute("UPDATE alcohol_use SET ALQ140U_b = 1 WHERE ALQ140U < 4")
   c.execute("UPDATE alcohol_use SET ALQ150_b = 1 WHERE ALQ150 = 1")

   c.execute("UPDATE health_status SET HSQ470_b = 1 WHERE HSQ470 = 1")
   c.execute("UPDATE health_status SET HSQ480_b = 1 WHERE HSQ480 BETWEEN 10 AND 30")
   c.execute("UPDATE health_status SET HSQ490_b = 1 WHERE HSQ490 BETWEEN 10 AND 30")
   c.execute("UPDATE health_status SET HSQ493_b = 1 WHERE HSQ493 BETWEEN 10 AND 30")
   c.execute("UPDATE health_status SET HSQ496_b = 1 WHERE HSQ496 BETWEEN 10 AND 30")
   c.execute("UPDATE health_status SET HSQ500_b = 1 WHERE HSQ500 = 1")
   c.execute("UPDATE health_status SET HSQ510_b = 1 WHERE HSQ510 IN (1,9)")

   #female 10-120 ng/ml
   #male 20-250 ng/ml
   c.execute("UPDATE ferritin SET LBXFER = 80 WHERE LBXFER = 'nan'")
   conn.commit()
   gender = "SELECT RIAGENDR from basic_information where basic_information.SEQN = ferritin.SEQN AND LBXFER != 'nan' AND ((RIAGENDR = 2 AND (LBXFER < 10 OR LBXFER > 120)) OR (RIAGENDR = 1 AND (LBXFER < 20 OR LBXFER > 250)))"
   c.execute("UPDATE ferritin SET LBXFER_b = 1 WHERE EXISTS ("+gender+")")

   c.execute("UPDATE vitamin_d SET LBXVIDMS_b = 1 WHERE LBXVIDMS BETWEEN 0 AND 30")
   c.execute("UPDATE vitamin_d SET LBDVIDLC_b = 1 WHERE LBDVIDLC BETWEEN 0 AND 30")
   c.execute("UPDATE vitamin_d SET LBXVD2MS_b = 1 WHERE LBXVD2MS BETWEEN 0 AND 30")
   c.execute("UPDATE vitamin_d SET LBDVD2LC_b = 1 WHERE LBDVD2LC BETWEEN 0 AND 30")
   c.execute("UPDATE vitamin_d SET LBXVD3MS_b = 1 WHERE LBXVD3MS BETWEEN 0 AND 30")
   c.execute("UPDATE vitamin_d SET LBDVD3LC_b = 1 WHERE LBDVD3LC BETWEEN 0 AND 30")
   c.execute("UPDATE vitamin_d SET LBXVE3MS_b = 1 WHERE LBXVE3MS BETWEEN 0 AND 30")
   c.execute("UPDATE vitamin_d SET LBDVE3LC_b = 1 WHERE LBDVE3LC BETWEEN 0 AND 30")

def complete_profile(c, conn):

   # for row in c.execute("select count(bowel_health.SEQN) from nutrition, bowel_health where nutrition.SEQN = bowel_health.SEQN"):
   #    print(row)
   # for row in c.execute("select count(nutrition.SEQN) from consumer_behavior, nutrition where nutrition.SEQN = consumer_behavior.SEQN"):
   #    print(row)
   # for row in c.execute("select * from consumer_behavior, bowel_health where consumer_behavior.SEQN = bowel_health.SEQN"):
   #    print(row)
   # BHQ010, BHQ020, BHQ030, BHQ040, BHQD50, BHQ060, BHQ070, BHQ080, BHQ090, BHQ0100, BHQ0110, CBD010, CBQ020, CBQ030, CBQ040, CBQ050, CBQ060, CBQ140, CBD160, DBQ010, DBD030, DBD041, DBD050, DBD055, DBD061, DBQ073A, DBQ073D, DBQ073E, DBQ073U, DBQ700, DBQ197, DBQ223A, DBQ223U, DBQ229, DBQ235A, DBQ235B, DBQ235C, DBQ424, DBD895, DBD900, DBD905, DBD910, DBQ915, DBQ920, DBQ925A, DBQ925B, DBQ925C, DBQ925D, DBQ925E, DBQ925F, DBQ925G, DBQ925H, DBQ925I, DBQ925J, DBQ930, DBQ935, DBQ940, DBQ945

   c.execute("DROP TABLE IF EXISTS patients")
   # # c.execute("CREATE TABLE patients as select * from consumer_behavior, bowel_health, alcohol_use where consumer_behavior.SEQN = bowel_health.SEQN")
   bowel_health = "BHQ010_b, BHQ020_b, BHQ030_b, BHQ040_b, BHD050_b "
   alcohol_use = "ALQ101_b, ALQ120Q_b, ALQ120U_b, ALQ130_b, ALQ140Q_b, ALQ140U_b, ALQ150_b "
   consumer_behavior = "CBD010_b, CBQ020_b, CBQ030_b, CBQ040_b, CBQ050_b, CBQ060_b "
   nutrition = "DBQ010_b, DBQ700_b, DBQ197_b, DBQ229_b, DBQ235A_b, DBD895_b, DBD900_b, DBD905_b, DBD910_b, DBQ920_b, DBQ925A_b, DBQ925B_b, DBQ925C_b, DBQ925D_b, DBQ925E_b, DBQ925F_b, DBQ925G_b, DBQ925H_b, DBQ925I_b, DBQ925J_b"

   registers = bowel_health+","+alcohol_use+","+consumer_behavior+","+nutrition

   #selecting patients with bowel disease
   query = "CREATE TABLE patients AS SELECT bowel_health.SEQN as SEQN, "+registers + \
       " FROM consumer_behavior join bowel_health on bowel_health.SEQN = consumer_behavior.SEQN join nutrition on nutrition.SEQN = consumer_behavior.SEQN join alcohol_use on alcohol_use.SEQN = consumer_behavior.SEQN join health_status on health_status.SEQN = consumer_behavior.SEQN WHERE health_status.HSQ510_b = 1"

   c.execute(query)
   c.execute("ALTER TABLE patients ADD COLUMN bowel_profile text")
   c.execute("ALTER TABLE patients ADD COLUMN alcohol_profile text")
   c.execute("ALTER TABLE patients ADD COLUMN consumer_profile text")
   c.execute("ALTER TABLE patients ADD COLUMN nutrition_profile text")
   c.execute("CREATE INDEX seqn_patient on patients (SEQN)")

   for row in c.execute("SELECT count(*) FROM patients"):
      print(row)

   str1 = bowel_health.replace(",", " ||")
   str2 = alcohol_use.replace(",", " ||")
   str3 = consumer_behavior.replace(",", " ||")
   str4 = nutrition.replace(",", " ||")

   query = "SELECT "+str1+" AS bowel_profile, "+str2 + " AS alcohol_profile, "+str3+" AS consumer_profile, "+str4+" AS nutrition_profile, SEQN FROM patients"
   # print(query)
   queries = []
   for row in c.execute(query):
      sql = "UPDATE patients SET bowel_profile = '" + str(row[0])+"' , alcohol_profile = '"+str(
          row[1])+"' , consumer_profile = '"+str(row[2])+"' , nutrition_profile = '"+ str(row[3])+"' WHERE SEQN = '" + str(row[4]) + "'"
      queries.append(sql)
      # print(row[0])
      # print(row[1])
      # print(sql)

   for sql in queries:
      c.execute(sql)

   conn.commit()

def create_database():

   conn = sqlite3.connect('database.db')
   conn.text_factory = str
   c = conn.cursor()

   # create_bowel_health_table(c, conn)
   # # populate_bowel_health_table(c, conn, '2005-2006/BHQ_D.csv')
   # populate_bowel_health_table(c, conn, '2007-2008/BHQ_E.csv')
   # populate_bowel_health_table(c, conn, '2009-2010/BHQ_F.csv')
  

   # create_nutrition_table(c, conn)
   # # populate_nutrition_table(c, conn, '2005-2006/DBQ_D.csv')
   # populate_nutrition_table(c, conn,'2007-2008/DBQ_E.csv')
   # populate_nutrition_table(c, conn,'2009-2010/DBQ_F.csv')

   # create_consumer_behavior_table(c, conn)
   # # populate_consumer_behavior_table(c, conn, '2005-2006/CBQ_D.csv')
   # populate_consumer_behavior_table(c, conn,'2007-2008/CBQ_E.csv')
   # populate_consumer_behavior_table(c, conn,'2009-2010/CBQ_F.csv')

   # create_alcohol_use_table(c, conn)
   # # populate_alcohol_use_table(c, conn, '2005-2006/ALQ_D.csv')
   # populate_alcohol_use_table(c, conn, '2007-2008/ALQ_E.csv')
   # populate_alcohol_use_table(c, conn, '2009-2010/ALQ_F.csv')

   # create_current_health_status_table(c, conn)
   # # populate_current_health_status_table(c, conn, '2005-2006/HSQ_D.csv')
   # populate_current_health_status_table(c, conn, '2007-2008/HSQ_E.csv')
   # populate_current_health_status_table(c, conn, '2009-2010/HSQ_F.csv')

   # create_current_vitamin_D_table(c, conn)
   # populate_current_vitamin_D_table(c, conn, '2007-2008/VID_E.csv')
   # populate_current_vitamin_D_table(c, conn, '2009-2010/VID_F.csv')

   create_current_ferritin_table(c, conn)
   populate_current_ferritin_table(c, conn, '2007-2008/FERTIN_E.csv')
   populate_current_ferritin_table(c, conn, '2009-2010/FERTIN_F.csv')

   # create_basic_information_table(c, conn)
   # populate_basic_information_table(c, conn, '2007-2008/DEMO_E.csv')
   # populate_basic_information_table(c, conn, '2009-2010/DEMO_F.csv')

   updateRegisters(c, conn)

   complete_profile(c, conn)  # auxiliar queries

   conn.close()


if __name__ == "__main__":
   create_database()
   sys.stdout.flush()
