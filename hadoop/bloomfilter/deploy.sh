git pull
hadoop fs -rm -r output
hadoop fs -rm -r output_2
hadoop fs -rm -r output_3
hadoop fs -rm -r output_3.1
hadoop fs -rm -r output_3.2
hadoop fs -rm -r output_3.3
hadoop fs -rm -r output_3.4
hadoop fs -rm -r output_3.5
hadoop fs -rm -r output_3.6
hadoop fs -rm -r output_3.7
hadoop fs -rm -r output_3.8
hadoop fs -rm -r output_3.9
hadoop fs -rm -r output_3.10
mvn clean package
cd target
hadoop jar bloomfilter-1.0-SNAPSHOT.jar it.unipi.hadoop.Main title.ratings.txt output 100000 0.01 
hadoop jar bloomfilter-1.0-SNAPSHOT.jar it.unipi.hadoop.Test title.ratings.txt output 100000
hadoop fs -cat output/part-r-00000
hadoop fs -cat output_3.1/part-r-00000
hadoop fs -cat output_3.2/part-r-00000
hadoop fs -cat output_3.3/part-r-00000
hadoop fs -cat output_3.4/part-r-00000
hadoop fs -cat output_3.5/part-r-00000
hadoop fs -cat output_3.6/part-r-00000
hadoop fs -cat output_3.7/part-r-00000
hadoop fs -cat output_3.8/part-r-00000
hadoop fs -cat output_3.9/part-r-00000
hadoop fs -cat output_3.10/part-r-00000

