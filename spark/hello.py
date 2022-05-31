import sys
from operator import add
from bloomfilter import BloomFilter
import math
import mmh3
from bitarray import bitarray
from pyspark import SparkContext
p = 0.01 #false positive probability


def count_ratings_occurences(file_name):
    lines = sc.textFile(file_name)
    words = lines.flatMap(lambda x: str(round(0.0001+float(x.split('\t')[1]))))
    ones =  words.map(lambda x: (x, 1))
    counts = ones.reduceByKey(add)

    if len(sys.argv) == 3:
        counts.repartition(1).saveAsTextFile(sys.argv[2])
    else:
        output = counts.collect()
        return output


def add_item_to_bloom_filter(hash_count,size,item):
    digests = []
    bit_array = bitarray(size)
    bit_array.setall(0)
    for i in range(hash_count):
        # create digest for given item.
        # i work as seed to mmh3.hash() function
        # With different seed, digest created is different
        digest = mmh3.hash(item, i) % size
        digests.append(digest)
        bit_array[digest] = True
    return bit_array

def check_item_in_bloom_filter(hash_count, size, bit_array, item):
    for i in range(hash_count):
        digest = mmh3.hash(item, i) % size
        print(bit_array,digest)
        if bit_array[digest] == False:
            # if any of bit is False then,its not present
            # in filter
            # else there is probability that it exist
            return False
    return True

def get_size(n, p):
    m = -(n * math.log(p))/(math.log(2)**2)
    return int(m)

def get_hash_count(size, n):
    k = (size/n) * math.log(2)
    return int(k)

def insert_ratings_in_bloom_filters(file_name, SIZES, HASH_COUNTS):
    lines = sc.textFile(file_name)
    ratings = lines.map(lambda x: ( x.split('\t')[0],round(0.0001+float(x.split('\t')[1]))))
    output = ratings.map(lambda rating: (rating[1],add_item_to_bloom_filter(HASH_COUNTS[rating[1]],SIZES[rating[1]],rating[0]))).reduceByKey(lambda bit_arr, acc: bit_arr | acc).collect()
    return output
    
def calculate_false_positive_rate(file_name, hash_count, size, bit_array, rate ):
    lines = sc.textFile(file_name)
    # (id1,3),(id2,4)...
    ratings = lines.map(lambda x: ( x.split('\t')[0],round(0.0001+float(x.split('\t')[1]))))
    filtered_ratings = ratings.filter(lambda rating: rating[1] != rate)
    false_positives = filtered_ratings.map(lambda rating: (check_item_in_bloom_filter(hash_count, size, bit_array, rating[0]),1))
    # (true,1),(false,1),(false,1),..
    counts = false_positives.reduceByKey(add)
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
    #   0,1,2,3,4,5,6,7,8,9,10
    N = [1,1,1,1,1,1,1,1,1,1,1]

    for (word, count) in rating_count:
        N[int(word)]= count
        print("%s: %i" % (word, count))

    SIZES = [get_size(n, p) for n in N]
    HASH_COUNTS = [get_hash_count(size, n) for size, n in zip(SIZES, N)]
    total_elements= sum(N)
    #bloomFilters = [BloomFilter(N[i],p,"Rate "+ str(i+1)) for i in range(len(N))]
    results = insert_ratings_in_bloom_filters(sys.argv[1], SIZES, HASH_COUNTS) 
    print(list(results))
    bloomFilterRDD = sc.sparkContext.parallelize(list(results))
    false_positive_rate = bloomFilterRDD.map(lambda bloomFilter: calculate_false_positive_rate(sys.argv[1], HASH_COUNTS[bloomFilter[0]], SIZES[bloomFilter[0]], bloomFilter[1], bloomFilter[0])).collect()
    print(false_positive_rate)
    # (1, 0101010101),(2,100101100101), ... 
    # bloomFilter6 = list( filter(lambda x: x[0] == 6, results))[0]
    
    
    # output = calculate_false_positive_rate(sys.argv[1], HASH_COUNTS[6], SIZES[6], bloomFilter6[1], 6)
    # print(output)
