#converts score of form "N out of M" to "N"
def cleanup(rec):
	out = []
        for i in range(len(rec)):
            out.append(rec[i].split(" ")[0])
        return tuple(out)

#extract hospital id, survey scores
surveys = sc.textFile("hdfs:///user/w205/hospital_compare/surveys")
surveys_trimmed =  surveys.map(lambda r : r.split(",")).map(lambda r :  tuple(r[i] for i in [0]) + tuple(r[i] for i in range(7,33)))

surveys2 = surveys_trimmed.map(lambda r: cleanup(r))
surveys2.saveAsTextFile("hdfs:///user/w205/hospital_compare/surveys.csv")
