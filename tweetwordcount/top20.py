import sys
import psycopg2
import bisect 
import numpy as np
from matplotlib import pyplot as plt

#connect to db
conn = psycopg2.connect(database="Tcount", user="postgres", password="", host="localhost", port="5432")
cur = conn.cursor()

sorted_recs = []
cur.execute('SELECT word, count from "Tweetwordcount"')
records = cur.fetchall()

#sort the records by count of word occurrences (lowest to highest)
for rec in records:
    bisect.insort(sorted_recs, (rec[1], rec[0]))

#grab the twenty largest records (at the end of the list)
top_20 = sorted_recs[-20:]
for t in top_20[::-1]:
    print "(%s , %d)" %(t[1], t[0])

#slice the original list into two (separate the tuples)
counts, words = zip(*top_20)

#set up the bar chart

fig = plt.figure()

width = .35
ind = np.arange(len(counts))
plt.bar(ind, counts, width=width)
plt.xticks(ind + width / 2, words, rotation="vertical")
plt.xlabel("Top 20 words")
plt.ylabel("Counts")
plt.savefig("plot.png")
 
        
