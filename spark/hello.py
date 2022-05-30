import sys
from operator import add
from bloomfilter import BloomFilter


from pyspark import SparkContext
p = 0.1 #false positive probability


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

def insert_ratings_in_bloom_filters(file_name, bloomFilters):
    lines = sc.textFile(file_name)
    print("ciaoooooo")
    ratings = lines.map(lambda x: ( x.split('\t')[0],round(float(x.split('\t')[1]))))
    # ratings.map(lambda rating: bloomFilters[rating[1]-1].add(rating[0]))

    output = ratings.map(lambda rating: (rating[1]-1,bloomFilters[rating[1]-1].add(rating[0]))).reduceByKey(lambda bit_arr, acc: bit_arr | acc).collect()
    return output
    
    
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: wordcount <input file> [<output file>]", file=sys.stderr)
        sys.exit(-1)

    master = "local"
    sc = SparkContext(master, "WordCount")

    rating_count= count_ratings_occurences(sys.argv[1])
    #   1,2,3,4,5,6,7,8,9,10
    N= [1,1,1,1,1,1,1,1,1,1]

    for (word, count) in rating_count:
        N[int(word)-1]= count
        print("%s: %i" % (word, count))

    total_elements= sum(N)
    bloomFilters = [BloomFilter(N[i],p,"Rate "+ str(i+1)) for i in range(len(N))]
    print("HO creato i miei bei bloom filters")
    output = insert_ratings_in_bloom_filters(sys.argv[1],bloomFilters) 
    print(output)
    print("funziona? "+ str(bloomFilters[1].check("tt0000001")))
    print("funziona? "+ str(bloomFilters[2].check("tt0000001")))
    print("funziona? "+ str(bloomFilters[3].check("tt0000001")))
    print("funziona? "+ str(bloomFilters[4].check("tt0000001")))
    print("funziona? "+ str(bloomFilters[5].check("tt0000001")))
    print("funziona? "+ str(bloomFilters[6].check("tt0000001")))
    print("funziona? "+ str(bloomFilters[7].check("tt0000001")))
    
