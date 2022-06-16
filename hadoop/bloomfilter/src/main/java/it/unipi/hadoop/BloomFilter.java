package it.unipi.hadoop;

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
import java.io.IOException;
import java.io.OutputStream;
import it.unipi.hadoop.BloomFilterCreator;
import it.unipi.hadoop.Job1.*;

import java.io.InputStreamReader;
import java.io.FileInputStream;
import org.apache.commons.codec.digest.MurmurHash3;
import it.unipi.hadoop.*;


public class BloomFilter {

  public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: bloom filter <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job1 = Job.getInstance(conf1, "tokenizer of data");
    // job1.setInputFormatClass(FileInputFormat.class);
    job1.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job1, new Path(args[0]));
    job1.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 600000);

    job1.setJarByClass(BloomFilter.class);
    job1.setMapperClass(RatingMapper.class);
    //job1.setCombinerClass(CreateBloomFilterReducer.class);
    job1.setReducerClass(CreateParametersReducer.class);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class); // set output values for mapper
    
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    
    // for (int i = 0; i < otherArgs.length - 1; ++i) {
    //   FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
    // }

    FileOutputFormat.setOutputPath(job1,
        new Path(otherArgs[otherArgs.length - 1]));
    Boolean countSuccess = job1.waitForCompletion(true);
    if(!countSuccess) {
      System.exit(0);
    }
    
  
    System.exit(0);
    



    // Configuration conf2 = new Configuration();
    // Job job2 = Job.getInstance(conf2, "bloom filter creator");

    // Configuration conf3 = new Configuration();
    // Job job3 = Job.getInstance(conf3, "testing bloom filter");
    
  }
  }

