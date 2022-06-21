package it.unipi.hadoop.Job1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import it.unipi.hadoop.*;

public class CreateParametersReducer extends Reducer<Text, IntWritable, Text, Text> {
    
    private static final float p_rate = (float) 0.001;
    private Text result = new Text();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;

      for (IntWritable val : values) {
        sum += val.get();
       
      }
      int m = BloomFilterUtility.get_size(sum, p_rate);
      int k = BloomFilterUtility.get_hash_count(m, sum);

      String res = new String(Integer.toString(m) + "\t" + Integer.toString(k));
      result.set(res);
      context.write(key, result); // output (ratings m k )
    }
  }
 
