package it.unipi.hadoop.Job1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import it.unipi.hadoop.*;
public class CreateParametersReducer extends Reducer<Text, IntWritable, Text, Text> {
    
    private Text result = new Text();
    private String res = new String();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;

      for (IntWritable val : values) {
        sum += val.get();
       
      }
      int m = BloomFilterUtility.get_size(sum, 0.1);//context.getConfiguration().getDouble("mapreduce.input.p_rate", 0.1));
      int k = BloomFilterUtility.get_hash_count(m, sum);

      res = (Integer.toString(m) + "\t" + Integer.toString(k) + "\t" + Integer.toString(sum));
      result.set(res);
      context.write(key, result); // output (ratings m k )
    }
  }
 
