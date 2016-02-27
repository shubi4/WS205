
#read from in-memory RDD

#add up the base and consistency scores, and sort by ascending (worst scores) and descending (best scores)
surveys3 = surveys2.map(lambda x: (x[0].replace('"', ''), try_parse_int(x[25].replace('"', '')) + try_parse_int(x[26].replace('"', ''))))
surveys_best = surveys3.join(hosp_trimmed).sortBy(lambda x: x[1][0], False)
surveys_worst = surveys3.join(hosp_trimmed).sortBy(lambda x: x[1][0])
surveys_best.take(10)
surveys_worst.take(10) 


