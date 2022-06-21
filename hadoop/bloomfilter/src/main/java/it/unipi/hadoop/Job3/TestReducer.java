package it.unipi.hadoop.Job3;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TestReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {


    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        
        double sum = 0.0;
        int n = 0;
        for (IntWritable val : values) {
            sum += val.get();
            n++;
           
          }
            
          context.write(key, new DoubleWritable(sum/n));  
      }

    
}
