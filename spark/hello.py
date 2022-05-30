import sys
from operator import add
from bloomfilter import BloomFilter
import math
import mmh3
from bitarray import bitarray
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

def insert_ratings_in_bloom_filters(file_name, N, SIZES, HASH_COUNTS):
    lines = sc.textFile(file_name)
    ratings = lines.map(lambda x: ( x.split('\t')[0],round(float(x.split('\t')[1]))))
    output = ratings.map(lambda rating: (rating[1]-1,add_item_to_bloom_filter(HASH_COUNTS[rating[1]-1],SIZES[rating[1]-1],rating[0]))).reduceByKey(lambda bit_arr, acc: bit_arr | acc).collect()
    return output
    
    
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: wordcount <input file> [<output file>]", file=sys.stderr)
        sys.exit(-1)

    master = "local"
    sc = SparkContext(master, "WordCount")

    rating_count= count_ratings_occurences(sys.argv[1])
    #   1,2,3,4,5,6,7,8,9,10
    N = [1,1,1,1,1,1,1,1,1,1]
    SIZES = [get_size(n, p) for n in N]
    HASH_COUNTS = [get_hash_count(size, n) for size, n in zip(SIZES, N)]

    for (word, count) in rating_count:
        N[int(word)-1]= count
        print("%s: %i" % (word, count))

    total_elements= sum(N)
    #bloomFilters = [BloomFilter(N[i],p,"Rate "+ str(i+1)) for i in range(len(N))]
    print("HO creato i miei bei bloom filters")
    results = insert_ratings_in_bloom_filters(sys.argv[1], N, SIZES, HASH_COUNTS) 
        

    # (1, 0101010101),(2,100101100101), ... 
    bloomFilter5 = results.find(lambda x: x[0] == 5)
    print("funziona? "+ str( check_item_in_bloom_filter(HASH_COUNTS[5], SIZES[5], bloomFilter5[1], "tt0000001")))
    

