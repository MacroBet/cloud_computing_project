import sys
from operator import add,or_
import math
import mmh3
from bitarray import bitarray
from pyspark import SparkConf, SparkContext
import time


###### BLOOM FILTERS PURE FUNCTIONS ######

def add_item_to_bloom_filter(hash_count,size,item):
    """"
    Create a bit array of the requested size and insert the item parsed using mmh3.hash() with different seeds
    Args:
        hash_count: number of hash functions to use
        size: bit array size
        item: string key to be inserted
    Returns:
        The bitarray containing the item hashed
    """
    digests = []
    bit_array = bitarray(size)
    bit_array.setall(0)
    for i in range(hash_count):
        digest = mmh3.hash(item, i) % size
        digests.append(digest)
        bit_array[digest] = True
    return bit_array

def createBloomFilter(hash,size,items): 
    """"
    Create the bitarray containing all the hashed items
    Args:
        hash_count: number of hash functions to use
        size: bit array size
        items: string keys to be inserted
    Returns:
        The bitarray containing all the hashed items
    """
    bit_array = bitarray(size)
    bit_array.setall(0)
    for item in items:
        for i in range(hash):
            digest = mmh3.hash(item, i) % size
            bit_array[digest] = True
    return bit_array

def check_item_in_bloom_filter(hash_count, size, bit_array, item):
    """
    Verify that each one of the bit generated from the mmh3.hash() is contained in the bit_array
    Args:
        hash_count: number of hash functions to use
        size: bit array size
        bit_array: bit array rapresenting the bloom filter
        item: string key to be verified
    Returns:
        True or False
    """
    for i in range(hash_count):
        digest = mmh3.hash(item, i) % size
        if bit_array[digest] == False:
            return False
    return True

def get_size(n, p):
    """
    Get the size of the bitarray used in bloom filter
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
        if(rating!=item[0] and check_item_in_bloom_filter(HASH_COUNTS[rating], SIZES[rating], bit_array, item[1])):
            false_positive.append((rating,1))
    return false_positive

###### END OF PURE FUNCTIONS ######

#  return tuple like (id, rating)
rating_extractor = lambda x: ( round(0.0001+float(x.split('\t')[1])),x.split('\t')[0])

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: BloomFilter <input file> <fp> <local?>", file=sys.stderr)
        sys.exit(-1)
    p = 0.01 
    N = [1,1,1,1,1,1,1,1,1,1,1]
    local = False
    if (sys.argv[2]!=None): p = float(sys.argv[2])
    if (len(sys.argv)==4 and sys.argv[3]!=None): local = bool(sys.argv[3])
    print("False positive probability: ",p)
    conf = (SparkConf().setAppName("Bloom Filter").setMaster("local" if local else "yarn"))
    sc = SparkContext(conf=conf)
    
    # 1. read file creating the RDD
    #    ratings is a tuple array => [(id, rating),...]
    ratings = sc.textFile(sys.argv[1]).map(rating_extractor).filter(lambda x: x[0]!=0).sortByKey().cache()

    # 2. count occurences of each rating
    start_time = time.time()
    rating_count= ratings.map(lambda rating: (rating[0],1)).reduceByKey(add).collect()
    print("--- Counted ratings in %s seconds ---" % (time.time() - start_time))

    print(rating_count)
    # 2.1. assign result of parallel counts
    for (rating, count) in rating_count:
        N[int(rating)]= count

    # 2.2. configure bloom filter parameters 
    total_elements= sum(N)
    SIZES = [get_size(n, p) for n in N]
    HASH_COUNTS = [get_hash_count(size, n) for size, n in zip(SIZES, N)]
    B_SIZES = sc.broadcast(SIZES)
    B_HASH_COUNTS = sc.broadcast(HASH_COUNTS)

    # 3. insert elements in bloom filter
    start_time = time.time()

    # old slow approach 
    # bloomFilters = ratings.map(lambda rating: (rating[0],add_item_to_bloom_filter(B_HASH_COUNTS.value[rating[0]],B_SIZES.value[rating[0]],rating[1])))\
    #                .reduceByKey(or_).collect()

    bloomFilters = ratings.groupByKey().map(lambda ratings: (ratings[0],createBloomFilter(B_HASH_COUNTS.value[ratings[0]],B_SIZES.value[ratings[0]],ratings[1])))\
                   .collect()

    print("--- Created bloom filters in %s seconds ---" % (time.time() - start_time))
     
    # 4. compute false positive count
    start_time = time.time()
    """Example:
        n di => [("3",1),("7",1)]
        n = 2 [("3",1),("7",1)], [("2",1),("3",1)]
        false_positives = [("3",1),("7",1),("2",1),("3",1)]
    """
    false_positive_count = ratings.flatMap(lambda rating: check_item_in_bloom_filters(rating, bloomFilters, B_HASH_COUNTS.value, B_SIZES.value))\
                            .reduceByKey(add).collect()
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

