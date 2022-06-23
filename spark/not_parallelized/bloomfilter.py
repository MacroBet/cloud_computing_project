
import math
import mmh3
from bitarray import bitarray

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
		for i in range(self.hash_count):
			# create digest for given item.
			# i work as seed to mmh3.hash() function
			# With different seed, digest created is different
			digest = mmh3.hash(item, i) % self.size
			digests.append(digest)
			self.bit_array[digest] = True
		return self.bit_array

	def check(self, item):
		for i in range(self.hash_count):
			digest = mmh3.hash(item, i) % self.size
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


