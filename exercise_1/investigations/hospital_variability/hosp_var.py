# read from in-memory RDD and calculate min and max of each score
# subtract them to get the variability


ef_trimmed2 = ef_trimmed.map(lambda r: (r[0], r[2]))
ef_min = ef_trimmed2.reduceByKey(lambda x,y: min(try_parse_int(x,1000),try_parse_int(y,1000)))
ef_max = ef_trimmed2.reduceByKey(lambda x,y: max(try_parse_int(x),try_parse_int(y)))
res = ef_max.join(ef_min).map(lambda x: (x[0], x[1][0] - x[1][1])).join(hosp_trimmed).sortBy(lambda x: x[1][0], False)

res.take(10)
