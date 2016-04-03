
#extract state id, measure id, score
ef_state = sc.textFile("hdfs:///user/w205/hospital_compare/effective_care_state/")
ef_state_trimmed = ef_state.map(lambda r: r.split(",")).map(lambda r: (r[0].replace('"', ''), r[3].replace('"', ''),  r[4].replace('"', '')))
ef_state_trimmed.saveAsTextFile("hdfs:///user/w205/hospital_compare/effective_trimmed_state.csv")

