import sys
from operator import add

from pyspark import SparkContext


def count_ratings_occurences(file_name):
    lines = sc.textFile(file_name)
    words = lines.flatMap(lambda x: str(round(float(x.split('\t')[1]))))
    ones =  words.map(lambda x: (x, 1))
    counts = ones.reduceByKey(add)

    if len(sys.argv) == 3:
        counts.repartition(1).saveAsTextFile(sys.argv[2])
    else:
        output = counts.collect()
        return output
    
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: wordcount <input file> [<output file>]", file=sys.stderr)
        sys.exit(-1)

    master = "local"
    sc = SparkContext(master, "WordCount")

    rating_count= count_ratings_occurences(sys.argv[1])
    for (word, count) in rating_count:
        print("%s: %i" % (word, count))


    
