# 1. read file and count occurences of each rounded ratings
# => n1,..,n10
# 2. create 10 empty arrays of size ni 
# 3. fix false positive param and others too
# 4. iterate over all ratings and insert into bloom filters
# 5. iterate over all ratings and check if they are in the bloom filters

# Python 3 program to build Bloom Filter
# Install mmh3 and bitarray 3rd party module first
# pip install mmh3
# pip install bitarray
import math
import mmh3
from bitarray import bitarray

# TODO handle items_count=0
class BloomFilter(object):
	def __init__(self, items_count, fp_prob, name):
		self.fp_prob = fp_prob
		self.name = name
		self.size = self.get_size(items_count, fp_prob)
		self.hash_count = self.get_hash_count(self.size, items_count)
		print("Creating bloom filter with size: {0} and hash count: {1}".format(self.size, self.hash_count))
		# Bit array of given size
		self.bit_array = bitarray(self.size)

		# initialize all bits as 0
		self.bit_array.setall(0)

	def add(self, item):
		digests = []
		print(self.name,item)
		for i in range(self.hash_count):
			# create digest for given item.
			# i work as seed to mmh3.hash() function
			# With different seed, digest created is different
			digest = mmh3.hash(item, i) % self.size
			digests.append(digest)
			print("DIGEST ADD",item,digest)
			# set the bit True in bit_array
			self.bit_array[digest] = True
		return self.bit_array

	def check(self, item):
		print(self.name,item,self.bit_array)
		for i in range(self.hash_count):
			digest = mmh3.hash(item, i) % self.size
			print("DIGEST CHECK",item,digest)
			if self.bit_array[digest] == False:

				# if any of bit is False then,its not present
				# in filter
				# else there is probability that it exist
				return False
		return True

	def set_bit_array(self, bit_array):
		self.bit_array = bit_array
		return self

	@classmethod
	def get_size(self, n, p):
		m = -(n * math.log(p))/(math.log(2)**2)
		return int(m)

	@classmethod
	def get_hash_count(self, m, n):
		k = (m/n) * math.log(2)
		return int(k)


