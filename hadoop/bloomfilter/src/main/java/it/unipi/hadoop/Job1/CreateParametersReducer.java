package it.unipi.hadoop.Job1;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import java.io.OutputStream;

import java.io.InputStreamReader;
import java.io.FileInputStream;
import org.apache.commons.codec.digest.MurmurHash3;
import it.unipi.hadoop.*;

public class CreateParametersReducer extends Reducer<Text, IntWritable, Text, Text> {
    
    private static final float p_rate = (float) 0.01;
    private Text result = new Text();

    public void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      int sum = 0;

      for (IntWritable val : values) {
        sum += val.get();
       
      }
      int m = BloomFilterUtility.get_size(sum, p_rate);
      int k = BloomFilterUtility.get_hash_count(m, sum);

      String res = new String(Integer.toString(m) + " " + Integer.toString(k));
      result.set(res);
      context.write(key, result); // output (ratings m k )
    }
  }
 
