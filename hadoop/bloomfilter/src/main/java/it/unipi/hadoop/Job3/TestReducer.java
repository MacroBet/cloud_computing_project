package it.unipi.hadoop.Job3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import it.unipi.hadoop.*;

public class TestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        
        int sum = 0;
        int n = 0;
        for (IntWritable val : values) {
            sum += val.get();
            n++;
            context.write(key, val);  
          }
            
          //context.write(key, new IntWritable(sum/n));  
      }


    
}
