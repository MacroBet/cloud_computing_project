git pull
hadoop fs -rm -r output
hadoop fs -rm -r output_2
hadoop fs -rm -r output_3
mvn clean package
cd target
hadoop jar bloomfilter-1.0-SNAPSHOT.jar it.unipi.hadoop.Main title.ratings.txt output 150000 0.0001 
hadoop fs -cat output/part-r-00000
hadoop fs -cat output_3/part-r-00000

