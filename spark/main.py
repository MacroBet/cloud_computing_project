import sys
from operator import add
import math
import mmh3
from bitarray import bitarray
from pyspark import SparkContext


###### BLOOM FILTERS PURE FUNCTIONS ######

def add_item_to_bloom_filter(hash_count,size,item):
    """"create digest for given item. I work as seed to mmh3.hash() function with different seed, digest created is different"""
    digests = []
    bit_array = bitarray(size)
    bit_array.setall(0)
    for i in range(hash_count):
        digest = mmh3.hash(item, i) % size
        digests.append(digest)
        bit_array[digest] = True
    return bit_array



def check_item_in_bloom_filter(hash_count, size, bit_array, item):
    """if any of bit is False then,its not present in filter else there is probability that it exist"""
    for i in range(hash_count):
        digest = mmh3.hash(item, i) % size
        if bit_array[digest] == False:
            return False
    return True


def get_size(n, p):
    """Get the size of the bitarray used in bloom filter
    Args:
        n: number of elements that bloom filter is expected to contain
        p: The account that will create the auction application.
    Returns:
        The size o
    """
    return int(-(n * math.log(p))/(math.log(2)**2))

def get_hash_count(size, n):
    return int((size/n) * math.log(2))


#false positive probability
p = 0.01 

def _rating(rating_raw):
    return round(0.0001+float(rating_raw))

def _rating_str(rating_raw):
    return str(round(0.0001+float(rating_raw)))

def count_ratings_occurences(lines):
    return lines.map(lambda x: (_rating_str(x.split('\t')[1]),1)).filter(lambda x: x[0]!="0").reduceByKey(add)

rating_extractor= lambda x: ( x.split('\t')[0],round(0.0001+float(x.split('\t')[1])))
    

def insert_ratings_in_bloom_filters(lines, SIZES, HASH_COUNTS):
    ratings = lines.map(rating_extractor)
    output = ratings.map(lambda rating: (rating[1],add_item_to_bloom_filter(HASH_COUNTS[rating[1]],SIZES[rating[1]],rating[0]))).reduceByKey(lambda bit_arr, acc: bit_arr | acc)
    return output.collect()
 
def check_item_in_bloom_filters(item, bloomFilters, HASH_COUNTS, SIZES):
    false_positive = []
    for bloom_filter in bloomFilters:
        rating = bloom_filter[0]
        bit_array = bloom_filter[1]
        if(rating!=item[1] and check_item_in_bloom_filter(HASH_COUNTS[rating], SIZES[rating], bit_array, item[0])):
            false_positive.append((rating,1))
    return false_positive

def calculate_false_positive_rate(lines, bloomFilters, HASH_COUNTS, SIZES ):
    ratings = lines.map(rating_extractor)
    false_positives = ratings.flatMap(lambda rating: check_item_in_bloom_filters(rating, bloomFilters, HASH_COUNTS, SIZES))
    counts = false_positives.reduceByKey(add)
    return counts.collect()

# 1. read file and count occurences of each rounded ratings
# => n1,..,n10
# 2. create 10 empty arrays of size ni 
# 3. fix false positive param and others too
# 4. iterate over all ratings and insert into bloom filters
# 5. iterate over all ratings and check if they are in the bloom filters

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: BloomFilter <input file> [<output file>]", file=sys.stderr)
        sys.exit(-1)

    master = "local"
    sc = SparkContext(master, "BloomFilter")
    lines = sc.textFile(sys.argv[1])
    if (sys.argv[2]!=None): p = float(sys.argv[2])

    print("False positive probability: ",p)
    

    rating_count= count_ratings_occurences(lines).collect()
    #   0,1,2,3,4,5,6,7,8,9,10
    N = [1,1,1,1,1,1,1,1,1,1,1]

    for (word, count) in rating_count:
        N[int(word)]= count
        print("%s: %i" % (word, count))

    total_elements= sum(N)
    SIZES = [get_size(n, p) for n in N]
    HASH_COUNTS = [get_hash_count(size, n) for size, n in zip(SIZES, N)]

    bloomFilters = insert_ratings_in_bloom_filters(lines, SIZES, HASH_COUNTS)
    false_positive_count = calculate_false_positive_rate(lines, bloomFilters, HASH_COUNTS, SIZES)
   
    print("FALSE POSITIVE RATES")
    print(false_positive_count)
    # 10 => reduce =>10000
    false_positive_rates=[]
    tot_fp = 0 
    for rate in false_positive_count:
        rating = rate[0]
        fp = rate[1]
        tot_fp += fp
        n = N[rating]
        false_positive_rates.append({'rating':rating,'false_positive_rate':fp/(total_elements-n), 'total_elements':n, 'false_positives':fp})
    print(false_positive_rates)
    print("TOTAL FALSE POSITIVE RATE: %f" % (tot_fp/total_elements))

