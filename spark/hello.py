import sys
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: wordcount <input file> [<output file>]", file=sys.stderr)
        sys.exit(-1)

    master = "local"
    sc = SparkContext(master, "WordCount")

    lines = sc.textFile(sys.argv[1])

# ["id1 5 10","id2 7 10","id3 8 10"] => ["id1","5","10"..]
# 
# 

    ratings= lines.map(lambda x: (x.split('\t')[0],round(float(x.split('\t')[1]))))
    # ratings = lines.flatMap(lambda x: [x.split('\t')[0],round(float(x.split('\t')[1]))])
    # words = lines.flatMap(lambda x: x.split(' '))
    ratingToCount= ratings.flatMap(lambda x: x[1])

    ones =  ratingToCount.map(lambda x: (x, 1))
    counts = ones.reduceByKey(add)

    if len(sys.argv) == 3:
        counts.repartition(1).saveAsTextFile(sys.argv[2])
    else:
        output = counts.collect()
        for (word, count) in output:
            print("%s: %i" % (word, count))
