package it.unipi.hadoop.Job1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Combiner extends Reducer <Text, IntWritable, Text, IntWritable> {

    public void combine(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      
        int sum = 0;

        for (IntWritable val : values) {
            sum += val.get();
       
        }
  
        context.write(key, new IntWritable(sum));
    }
}
