import sys
from operator import add
import math
import mmh3
from bitarray import bitarray
from pyspark import SparkContext
import time


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
        p: desired false positive rate
    Returns:
        The size of bitarray
    """
    return int(-(n * math.log(p))/(math.log(2)**2))

def get_hash_count(size, n):
    """Get the number of hash function to use in bloom filter
    Args:
        size: size of the bitarray used in bloom filter
        n: number of elements that bloom filter is expected to contain
    Returns:
        The size of bitarray
    """
    return int((size/n) * math.log(2))


#false positive probability
p = 0.01 

def _rating(rating_raw):
    return round(0.0001+float(rating_raw))

def _rating_str(rating_raw):
    return str(round(0.0001+float(rating_raw)))

def count_ratings_occurences(lines):
    return lines.map(lambda x: (_rating_str(x.split('\t')[1]),1)).filter(lambda x: x[0]!="0").reduceByKey(add)

#  return tuple like (id, rating)
rating_extractor = lambda x: ( x.split('\t')[0],_rating(x.split('\t')[1]))
    

def insert_ratings_in_bloom_filters(lines, SIZES, HASH_COUNTS):
    return lines.map(rating_extractor).\
    map(lambda rating: (rating[1],add_item_to_bloom_filter(HASH_COUNTS[rating[1]],SIZES[rating[1]],rating[0]))).\
    reduceByKey(lambda bit_arr, acc: bit_arr | acc)
 
def check_item_in_bloom_filters(item, bloomFilters, HASH_COUNTS, SIZES):
    """check_item_in_bloom_filters 
    Args:
        
    Returns:
        example of return value: [("3",1),("7",1)]
    """
    false_positive = []
    for bloom_filter in bloomFilters:
        rating = bloom_filter[0]
        bit_array = bloom_filter[1]
        if(rating!=item[1] and check_item_in_bloom_filter(HASH_COUNTS[rating], SIZES[rating], bit_array, item[0])):
            false_positive.append((rating,1))
    return false_positive

def calculate_false_positive_count(lines, bloomFilters, HASH_COUNTS, SIZES ):
    """calculate_false_positive_count 
    Args:
        
    Example:
        n di => [("3",1),("7",1)]
        n = 2 [("3",1),("7",1)], [("2",1),("3",1)]
        false_positives = [("3",1),("7",1),("2",1),("3",1)]
    """
    return lines.map(rating_extractor)\
    .flatMap(lambda rating: check_item_in_bloom_filters(rating, bloomFilters, HASH_COUNTS, SIZES))\
    .reduceByKey(add)
    

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: BloomFilter <input file> [<output file>]", file=sys.stderr)
        sys.exit(-1)
    if (sys.argv[2]!=None): p = float(sys.argv[2])
    print("False positive probability: ",p)
    master = "local"
    sc = SparkContext(master, "BloomFilter")
    N = [1,1,1,1,1,1,1,1,1,1,1]
    
    # 1. read file creating the RDD
    lines = sc.textFile(sys.argv[1])

    # 2. count occurences of each ratings (rounded)
    start_time = time.time()
    rating_count= count_ratings_occurences(lines).collect()
    print("--- Counted ratings in %s seconds ---" % (time.time() - start_time))

    # 2.1. assign result of parallel counts
    for (rating, count) in rating_count:
        N[int(rating)]= count

    # 2.2. configure bloom filter parameters 
    total_elements= sum(N)
    SIZES = [get_size(n, p) for n in N]
    HASH_COUNTS = [get_hash_count(size, n) for size, n in zip(SIZES, N)]

    # 3. insert elements in bloom filter
    start_time = time.time()
    bloomFilters = insert_ratings_in_bloom_filters(lines, SIZES, HASH_COUNTS).collect()
    print("--- Created bloom filters in %s seconds ---" % (time.time() - start_time))

    # 4. compute false positive count
    start_time = time.time()
    false_positive_count = calculate_false_positive_count(lines, bloomFilters, HASH_COUNTS, SIZES).collect()
    print("--- Tested bloom filters in %s seconds ---" % (time.time() - start_time))

   
    print("FALSE POSITIVE RATES")
    print(false_positive_count)
    # 10 => reduce =>10000
    false_positive_rates=[]
    tot_fpr = 0 
    
    for rate in false_positive_count:
        rating = rate[0]
        fpc = rate[1]
        n = N[rating]
        fpr = fpc/(total_elements-n) # of rating 
        tot_fpr += fpr
        false_positive_rates.append({'rating':rating,'false_positive_rate':fpr, 'total_elements':n, 'false_positives':fpc})
    
    print(false_positive_rates)
    print("TOTAL FALSE POSITIVE RATE: %f" % (tot_fpr/10))

