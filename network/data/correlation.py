from __future__ import absolute_import
from __future__ import print_function

from collections import defaultdict

import os
import sys
import optparse
import subprocess
import random

import sqlite3
import csv

import networkx as nx
import matplotlib.pyplot as plt


# import numpy
# from scipy.cluster import hierarchy
# from scipy.spatial import distance

def draw_charts(c):

   # pain condition
   query = "SELECT COUNT(*) FROM patients JOIN health_status ON health_status.SEQN = patients.SEQN WHERE health_status.HSQ493_b = 1"
  
   x = ["have pain", "do not have pain"]
   y = []
   for row in c.execute(query):
      y.append(int(row[0]))
   query = "SELECT COUNT(*) FROM patients JOIN health_status ON health_status.SEQN = patients.SEQN WHERE health_status.HSQ493_b = 0"
   for row in c.execute(query):
       y.append(int(row[0]))
   
   plt.bar(x, y,  width=0.1,  color=['red', 'green'])
   plt.xlabel('Pain make it hard for usual activities')
   plt.grid(True)
   plt.ylabel('Individuals')
   plt.show()

   #anxiety
   query = "SELECT COUNT(*) FROM patients JOIN health_status ON health_status.SEQN = patients.SEQN WHERE health_status.HSQ496_b = 1"

   x = ["anxious", "not anxious"]
   y = []
   for row in c.execute(query):
      y.append(int(row[0]))
   query = "SELECT COUNT(*) FROM patients JOIN health_status ON health_status.SEQN = patients.SEQN WHERE health_status.HSQ496_b = 0"
   for row in c.execute(query):
       y.append(int(row[0]))

   plt.bar(x, y,  width=0.1,  color=['red', 'green'])
   plt.xlabel('Feeling anxious')
   plt.ylabel('Individuals')
   plt.grid(True)
   plt.show()

   # gender
   query = "SELECT COUNT(*) FROM patients JOIN basic_information ON basic_information.SEQN = patients.SEQN WHERE basic_information.RIAGENDR = 1"

   x = ["male", "female"]
   y = []
   for row in c.execute(query):
      y.append(int(row[0]))
   query = "SELECT COUNT(*) FROM patients JOIN basic_information ON basic_information.SEQN = patients.SEQN WHERE basic_information.RIAGENDR = 2"
   for row in c.execute(query):
       y.append(int(row[0]))

   plt.bar(x, y,  width=0.1,  color=['red', 'green'])
   plt.xlabel('Gender')
   plt.ylabel('Individuals')
   plt.grid(True)
   plt.show()


   #bowel health
   



def correlation_alcohol_and_bowel(c):

   view_name = "alcohol_and_bowel"
   c.execute("DROP VIEW IF EXISTS "+view_name)
   
   labels = "p.bowel_profile, p.alcohol_profile, p.consumer_profile, p.nutrition_profile"
   
   view = "CREATE VIEW "+view_name+" AS SELECT count(*) AS nPatients, "+labels+" FROM patients p GROUP BY "
   c.execute(view+" p.alcohol_profile, p.bowel_profile")

   queries = []
   for row in c.execute("SELECT alcohol_profile, bowel_profile, nPatients FROM alcohol_and_bowel WHERE nPatients >= 1"):
      queries.append(row)
      print(row)
   with open('q1.csv', mode='w') as result_file:
      result_writer = csv.writer(result_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      for query in queries:
         result_writer.writerow(query)


def correlation_nutrition_and_bowel(c):

   view_name = "nutrition_and_bowel"
   c.execute("DROP VIEW IF EXISTS "+view_name)

   labels = "p.bowel_profile, p.alcohol_profile, p.consumer_profile, p.nutrition_profile"

   view = "CREATE VIEW "+view_name + \
       " AS SELECT count(*) AS nPatients, "+labels+" FROM patients p GROUP BY "
   c.execute(view+" p.nutrition_profile, p.bowel_profile")

   queries = []
   for row in c.execute("SELECT nutrition_profile, bowel_profile, nPatients FROM nutrition_and_bowel WHERE nPatients >= 1"):
      queries.append(row)
      print(row)
   with open('q2.csv', mode='w') as result_file:
      result_writer = csv.writer(
          result_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      for query in queries:
         result_writer.writerow(query)

def correlation_ferritin(c):

   view_name = "ferritin_"
   c.execute("DROP VIEW IF EXISTS "+view_name)

   labels = "p.bowel_profile, p.alcohol_profile, p.consumer_profile, p.nutrition_profile"

   view = "CREATE VIEW "+view_name + \
       " AS SELECT count(DISTINCT p.SEQN) AS nPatients, LBXFER, "+labels + \
       " FROM patients p " + \
       " JOIN ferritin ON ferritin.SEQN = p.SEQN AND  ferritin.LBXFER_b = 1 GROUP BY "
   c.execute(view+" p.nutrition_profile, p.bowel_profile")

   queries = []
   for row in c.execute("SELECT nutrition_profile, bowel_profile, nPatients, LBXFER FROM ferritin_ WHERE nPatients >= 1"):
      queries.append(row)
      print(row)
   # for row in c.execute("SELECT nutrition_profile, bowel_profile FROM patients p JOIN ferritin ON ferritin.SEQN = p.SEQN AND  ferritin.LBXFER_b = 1"):
   #    print(row)
   with open('q5.csv', mode='w') as result_file:
      result_writer = csv.writer(
          result_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      for query in queries:
         result_writer.writerow(query)

def correlation_consumer_and_bowel(c):

   view_name = "consumer_and_bowel"
   c.execute("DROP VIEW IF EXISTS " + view_name)

   labels = "p.bowel_profile, p.alcohol_profile, p.consumer_profile, p.nutrition_profile"

   view = "CREATE VIEW "+view_name + \
       " AS SELECT count(*) AS nPatients, "+labels+" FROM patients p GROUP BY "
   c.execute(view+" p.consumer_profile, p.bowel_profile")

   queries = []
   for row in c.execute("SELECT consumer_profile, bowel_profile, nPatients FROM consumer_and_bowel WHERE nPatients >= 1"):
      queries.append(row)
      print(row)
   with open('q3.csv', mode='w') as result_file:
      result_writer = csv.writer(
          result_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      for query in queries:
         result_writer.writerow(query)


def correlation_consumer_and_nutrition(c):

   view_name = "consumer_and_nutrition"
   c.execute("DROP VIEW IF EXISTS "+view_name)

   labels = "p.bowel_profile, p.alcohol_profile, p.consumer_profile, p.nutrition_profile"

  
   view = "CREATE VIEW "+view_name + \
       " AS SELECT count(*) AS nPatients, "+labels+" FROM patients p GROUP BY "
   c.execute(view+" p.consumer_profile, p.nutrition_profile")

   queries = []
   for row in c.execute("SELECT consumer_profile, nutrition_profile, nPatients FROM consumer_and_nutrition WHERE nPatients >= 1"):
      queries.append(row)
      print(row)
   with open('q4.csv', mode='w') as result_file:
      result_writer = csv.writer(
          result_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      for query in queries:
         result_writer.writerow(query)

def correlation_food_allergies(c):

   view_name = "food_allergies"
   c.execute("DROP VIEW IF EXISTS " + view_name)

   labels = "p.bowel_profile, p.alcohol_profile, p.consumer_profile, p.nutrition_profile"

  
   view = "CREATE VIEW "+view_name + \
       " AS SELECT count(*) AS nPatients, "+labels + \
       " FROM patients p, nutrition n WHERE p.SEQN = n.SEQN AND n.DBQ920 = 1 GROUP BY "
   c.execute(view+" p.consumer_profile, p.nutrition_profile")

   queries = []
   for row in c.execute("SELECT count(*) FROM food_allergies"):
      queries.append(row)
      print(row)
   # with open('q5.csv', mode='w') as result_file:
   #    result_writer = csv.writer(
   #        result_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
   #    for query in queries:
   #       result_writer.writerow(query)

# ALQ150 - Ever have 5 or more drinks every day?
# DBQ700 - How healthy is the diet
# BHQ010 - Bowel leakage consisted of gas?
# BHQ020 - Bowel leakage consisted of mucus?
# BHQ030 - Bowel leakage consisted of liquid?
# BHQ040 - Bowel leakage consisted of solid stool?
# BHD050 - How often have bowel movements?
# HSQ493 - Pain make it hard for usual activities
# def specific_profile_correlation(c):
def profile_correlation(c, conn):

   registers = "BHQ010_b, BHQ020_b, BHQ030_b, BHQ040_b, BHD050_b "

   str1 = registers.replace(",", " ||")

   cond1 = "bowel_health.BHQ010_b = 1 OR bowel_health.BHQ020_b = 1 OR bowel_health.BHQ030_b = 1 OR bowel_health.BHQ040_b = 1 OR bowel_health.BHD050_b = 1"
   cond2 = "health_status.HSQ470_b = 1 OR health_status.HSQ496_b = 1 OR health_status.HSQ510_b = 1 OR health_status.HSQ490_b OR health_status.HSQ493_b = 1"
   c.execute("DROP VIEW IF EXISTS bowel_profile")
   c.execute("CREATE VIEW bowel_profile AS SELECT "+str1 +
             " AS profile, bowel_health.SEQN, HSQ510 FROM bowel_health JOIN health_status ON bowel_health.SEQN = health_status.SEQN WHERE ("+cond1+") AND ("+cond2+")")

   
   query = "SELECT a.SEQN AS source, b.SEQN AS target, a.profile AS weight FROM bowel_profile a JOIN bowel_profile AS b ON a.profile = b.profile JOIN basic_information AS bi ON bi.SEQN = b.SEQN WHERE (a.SEQN < b.SEQN) AND (a.HSQ510 = 1 OR b.HSQ510 = 1 ) AND bi.RIAGENDR = 1"
   queries = []
   
   for row in c.execute(query):
      queries.append(row)
   with open('bowel_correlation.csv', mode='w') as result_file:
      result_writer = csv.writer(result_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      result_writer.writerow(["SOURCE", "TARGET", "WEIGHT"])
      for query in queries:
         result_writer.writerow(query)

   query = "SELECT a.SEQN AS node, a.HSQ510 FROM bowel_profile a"
   queries = []
   
   for row in c.execute(query):
      queries.append(row)
   with open('nodes.csv', mode='w') as result_file:
      result_writer = csv.writer(result_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
      result_writer.writerow(["ID", "HAS"])
      for query in queries:
         result_writer.writerow(query)

   conn.commit()


# def create_hc(G):
#     """Creates hierarchical cluster of graph G from distance matrix"""
#     path_length = nx.all_pairs_shortest_path_length(G)
#     distances = numpy.zeros((len(G), len(G)))
#     for u, p in path_length:
#         for v, d in p.items():
#             distances[u][v] = d
#     # Create hierarchical cluster
#     Y = distance.squareform(distances)
#     Z = hierarchy.complete(Y)  # Creates HC using farthest point linkage
#     # This partition selection is arbitrary, for illustrive purposes
#     membership = list(hierarchy.fcluster(Z, t=1.15))
#     # Create collection of lists for blockmodel
#     partition = defaultdict(list)
#     for n, p in zip(list(range(len(G))), membership):
#         partition[p].append(n)
#     return list(partition.values())



def network_profile_correlation(c):

   G = nx.Graph()
   # g = nx.petersen_graph()

   # registers = "BHQ010_b, BHQ020_b, BHQ030_b, BHQ040_b, BHD050_b "

   # str1 = registers.replace(",", " ||")

   # c.execute("DROP VIEW IF EXISTS bowel_profile")
   # c.execute("CREATE VIEW bowel_profile AS SELECT " +
   #           str1+" AS profile, SEQN FROM bowel_health")
   # for row in c.execute("SELECT COUNT(*) from bowel_profile"):
   #    print(row)

   with open('bowel_correlation.csv', mode='r') as edges: 
      count = 0
      for edge in edges:
         # print(edge)
         e = edge.split(",")
         G.add_edge(e[0],e[1])
         count += 1
   print(count)
  # position is stored as node attribute data for random_geometric_graph
   G = nx.convert_node_labels_to_integers(G)
   pos = nx.spring_layout(G, k=20, iterations=20)

   # find node near center (0.5,0.5)
   dmin = 1
   ncenter = 0
   for n in pos:
      x, y = pos[n]
      d = (x - 0.5)**2 + (y - 0.5)**2
      if d < dmin:
         ncenter = n
         dmin = d

   # color by path length from node near center
   p = dict(nx.single_source_shortest_path_length(G, ncenter))

   plt.figure(figsize=(20, 10))
   # node_color = [float(H.degree(v)) for v in H]
   nx.draw_networkx_edges(G, pos, nodelist=[ncenter], alpha=0.5)
   nx.draw_networkx_nodes(G, pos, nodelist=list(p.keys()),
                        node_size=80,
                        line_color='grey',
                        node_color=list(p.values()))

   
   # plt.show()
   plt.axis('off')
   plt.savefig("net.pdf")

def correlation_profile():

   conn = sqlite3.connect('database.db')
   conn.text_factory = str
   c = conn.cursor()

   # condition =  "WHERE BHQ010_b = 1  OR BHQ020_b = 1  OR  BHQ030_b = 1  OR  BHQ040_b = 1  OR BHD050_b = 1  OR  BHQ060_b = 1  OR  BHQ070_b = 1  OR  BHQ080_b = 1  OR  BHQ090_b = 1  OR  BHQ110_b = 1" 
   # correlation_alcohol_and_bowel(c)#Query 1
   # correlation_nutrition_and_bowel(c)#Query 2
   # correlation_consumer_and_bowel(c)#Query 3
   # correlation_consumer_and_nutrition(c)#Query 4
   # correlation_food_allergies(c)#Query 5
   # correlation_ferritin(c)

   # profile_correlation(c, conn)

   # network_profile_correlation(c)

   draw_charts(c)

   conn.commit()

if __name__ == "__main__":
   correlation_profile()
   sys.stdout.flush()
