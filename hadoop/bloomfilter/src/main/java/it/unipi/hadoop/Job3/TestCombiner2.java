package it.unipi.hadoop.Job3;

import java.io.IOException;
import java.util.StringTokenizer;

import javax.lang.model.util.ElementScanner6;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import it.unipi.hadoop.BloomFilter;

public class TestCombiner2 extends Reducer<Text, Text, Text, Text> {
 
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          
          double falsePositive = 0.0;
          int n = 0;
          for(Text val : values){
            if(val.toString().equals("1")) 
              falsePositive++;
            n++;
          }
          context.write(key, new Text((String.valueOf(falsePositive/n)))); 
   
        }
  
    */
}
