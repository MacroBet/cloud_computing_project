import sys
from operator import add
from bloomfilter import BloomFilter
import math
import mmh3
from bitarray import bitarray
from pyspark import SparkContext
p = 0.01 #false positive probability

# 6: 12
# 7: 5
# 5: 11 
# 4: 3
# 1: 2
# 2: 1
# 3: 1
# 8: 1
# 9: 1

# 6: 12
# 7: 5
# 5: 10 -> ok
# 1: 3 -> ko doveva mettere 10 
# 4: 3
# 2: 1
# 3: 1
# 8: 1
# 9: 1

def count_ratings_occurences(lines):
    counts = lines.map(lambda x: str(round(0.0001+float(x.split('\t')[1])))).filter(lambda x: x!="0" ).map(lambda x: (x, 1)).reduceByKey(add)
    return counts.collect()

# create digest for given item. I work as seed to mmh3.hash() function with different seed, digest created is different
def add_item_to_bloom_filter(hash_count,size,item):
    digests = []
    bit_array = bitarray(size)
    bit_array.setall(0)
    for i in range(hash_count):
        digest = mmh3.hash(item, i) % size
        digests.append(digest)
        bit_array[digest] = True
    return bit_array

# if any of bit is False then,its not present in filter else there is probability that it exist
def check_item_in_bloom_filter(hash_count, size, bit_array, item):
    for i in range(hash_count):
        digest = mmh3.hash(item, i) % size
        if bit_array[digest] == False:
            return False
    return True

def get_size(n, p):
    return int(-(n * math.log(p))/(math.log(2)**2))

def get_hash_count(size, n):
    return int( (size/n) * math.log(2))

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
    false_positives = ratings.map(lambda rating: check_item_in_bloom_filters(rating, bloomFilters, HASH_COUNTS, SIZES)).flatMap(lambda x: x)
    counts = false_positives.reduceByKey(add)
    return counts.collect()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: wordcount <input file> [<output file>]", file=sys.stderr)
        sys.exit(-1)

    master = "local"
    sc = SparkContext(master, "WordCount")
    lines = sc.textFile(sys.argv[1])

    rating_count= count_ratings_occurences(lines)
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
        false_positive_rates.append({'rating':rating,'false_positive_rate':fp/n, 'total_elements':n, 'false_positives':fp})
    print(false_positive_rates)
    print("TOTAL FALSE POSITIVE RATE: %f" % (tot_fp/total_elements))
