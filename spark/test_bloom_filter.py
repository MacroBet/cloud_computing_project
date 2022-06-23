from bloomfilter import BloomFilter
from random import shuffle
from random import randint

N_BLOOM_FILTERS=10
p = 0.1 #false positive probability

ratings = [["id"+str(i),randint(0, 9)] for i in range(1000)]

N= [0,0,0,0,0,0,0,0,0,0]
for rating in ratings:
    N[rating[1]]+=1

n = len(ratings) #no of items to add

bloomFilters = [BloomFilter(N[i],p) for i in range(N_BLOOM_FILTERS)]
for rating in (ratings):
    bloomFilters[rating[1]].add(rating[0])


# fp/total => false positive rate

false_positive_rate = 0
for idx,bf in enumerate(bloomFilters):
    for rating in (ratings):
        # true positive
        if(bf.check(rating[0]) and rating[1] == idx):
            print("{} True Positive {}".format(rating[0],idx))

        # false positive 
        elif (bf.check(rating[0]) and rating[1] != idx):
            false_positive_rate += 1
            print("{} False Positive {}".format(rating[0],idx))

print("False Positive Rate: {}".format(false_positive_rate/(n*N_BLOOM_FILTERS)))
