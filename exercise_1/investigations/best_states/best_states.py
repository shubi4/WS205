# to get the best state measures, sum up the scores over all measures for each state, then sort
# in descending order


#read from im-memory RDD
ef_state_trimmed2 = ef_state_trimmed.map(lambda r: (r[0], r[2]))
ef_state_keyed = ef_state_trimmed2.reduceByKey(lambda x,y: try_parse_int(x) + try_parse_int(y))
res_state = ef_state_keyed.sortBy(lambda x : x[1], False)
res_state.take(10)

