import sys
from operator import add
import math
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: wordcount <input file> [<output file>]", file=sys.stderr)
        sys.exit(-1)

    master = "local"
    sc = SparkContext(master, "WordCount")

    lines = sc.textFile(sys.argv[1])
    ratings = lines.flatMap(lambda x: [x.split('\t')[0],math.round(x.split('\t')[1])])

    # words = lines.flatMap(lambda x: x.split(' '))
    ones =  ratings.map(lambda x: (x[1], 1))
    counts = ones.reduceByKey(add)

    if len(sys.argv) == 3:
        counts.repartition(1).saveAsTextFile(sys.argv[2])
    else:
        output = counts.collect()
        for (word, count) in output:
            print("%s: %i" % (word, count))
