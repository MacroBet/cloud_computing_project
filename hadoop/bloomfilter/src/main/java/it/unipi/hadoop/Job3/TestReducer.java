package it.unipi.hadoop.Job3;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import it.unipi.hadoop.BloomFilter;

public class TestReducer extends Reducer<BloomFilter, Text, Text, DoubleWritable> {


    public void reduce(BloomFilter key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        int falsePositive = 0;
        int n = 0;
        String out = "";
        for (Text val : values) {
            String movieId = val.toString().split("\t")[0];
            out = val.toString().split("\t")[1];
            if(key.check(movieId))
              falsePositive++;
            n++;
          }
            
        context.write(new Text(out), new DoubleWritable(falsePositive/n)); 
 
      }

    
}
