
def try_parse_int(s):
    try:
        return int(s)
    except ValueError:
        return 0

#read from in-memory rdd; get just the prov id and score; add all the scores and find the top 10

ef_trimmed2 = ef_trimmed.map(lambda r: (r[0], r[2]))
ef_keyed = ef_trimmed2.reduceByKey(lambda x,y: try_parse_int(x) + try_parse_int(y))
res = ef_keyed.join(hosp_trimmed).sortBy(lambda x : x[1][0], False)
res.take(10)
