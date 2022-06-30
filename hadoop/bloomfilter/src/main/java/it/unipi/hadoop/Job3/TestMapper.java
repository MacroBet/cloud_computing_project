package it.unipi.hadoop.Job3;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import it.unipi.hadoop.BloomFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile;

public class TestMapper  extends Mapper<Object, Text, Text,Text> {

  private BloomFilter[] bloomFilters = new BloomFilter[10];
  private int[] false_positives = new int[10];
  private int[] non_false_positives = new int[10];

  public void setup(Context context) throws IOException, InterruptedException {
      
      try {
            Path pt = new Path("hdfs://hadoop-namenode:9820/user/hadoop/output_2/part-r-00000");// Location of file in HDFS
            SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), Reader.file(pt));
            boolean hasNext;
            do {

              Text key = new Text();
              BloomFilter bf = new BloomFilter();
              hasNext = reader.next(key, bf);
              // DECREMENT RATING TO GET INDEX
              bloomFilters[Integer.parseInt(key.toString())-1] = bf;
            } while(hasNext);

        } catch (Exception e) { e.getStackTrace(); }
  }

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      Integer rating;
      while (itr.hasMoreTokens()) {
        
        String ratingRaw = itr.nextToken().toString();
        String movieId = ratingRaw.split("\t")[0];
        // DECREMENT RATING TO GET INDEX
        rating = Math.round(Float.parseFloat(ratingRaw.split("\t")[1]))-1;
        for(int i = 0; i < 10; i++){
          // Test only against bloomfilters where the id is not present
          if(i != rating){
            if(bloomFilters[i].check(movieId))
              false_positives[i]++; 
            else
              non_false_positives[i]++;
          }
        }
      }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      for(int i =0;i<false_positives.length;i++){
          double FPrate = (double) false_positives[i] / (double)(false_positives[i]+non_false_positives[i]);
          // INCREMENT INDEX TO GET RATING
          context.write(new Text(String.valueOf(i+1)), new Text(String.valueOf(FPrate)));
      } 
    }
}
