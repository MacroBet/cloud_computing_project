git pull

mvn clean package
cd target
hadoop jar bloomfilter-1.0-SNAPSHOT.jar it.unipi.hadoop.Main title.ratings.txt output_10
hadoop fs -cat output/part-r-00000
hadoop fs -cat output_3/part-r-00000