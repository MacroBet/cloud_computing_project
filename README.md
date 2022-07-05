# Bloom Filters implementation on Hadoop and Spark

In this project we want to investigate in deep on how a Bloom Filter can be implemented in a parallelized way
using the map-reduce approach whit Hadoop and Spark.
Moreover we want to test our implementation to prove the literature formulas we have about bloom filters,
finally we’ll make a performance comparison between Hadoop and Spark. We have to say that now-days, Spark
offers much more flexibility than Hadoop and it is 10 times faster to code on it.

To play with Bloom Filters we used an IMDb datasets with more than 1.5M ratings of movies, starting from
this dataset we wanted to create 10 Bloom Filters each one representing the movies with a certain rating.
Once realized the algorithm to built such bloom filters, we compute the false positive rating for each bloom
filter to extract the final average rate.
The ratings dataset was not balanced, as you can see in the distribution above. For such reason we will see
later how the dataset composition influence our results for the false positive rate.
We concluded our paper with performance analysis and a simple correlation study between the p value and the
false positive rate.

For the environment configuration please refer to https://github.com/tonellotto/cloud-computing/tree/master/notes

## Hadoop machines configuration

- IP, name node, data node, hostname
- 172.16.4.175, yes, yes, hadoop-namenode
- 172.16.4.148, no, yes, hadoop-datanode-2
- 172.16.4.183, no, yes, hadoop-datanode-3
- 172.16.4.181, no, yes, hadoop-datanode-4

## Test implementation

Once the environment will be configured just run the deploy bash script inside spark and hadoop folder
