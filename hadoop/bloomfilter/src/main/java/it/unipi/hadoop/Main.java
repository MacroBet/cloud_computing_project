package it.unipi.hadoop;


import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import it.unipi.hadoop.Job1.*;
import it.unipi.hadoop.Job2.BloomFiltersMapper;
import it.unipi.hadoop.Job2.BloomFiltersReducer;
import it.unipi.hadoop.Job3.TestMapper1;
import it.unipi.hadoop.Job3.TestMapper2;
import it.unipi.hadoop.Job3.TestReducer1;
import it.unipi.hadoop.Job3.TestReducer2;


public class Main {

  public static HashMap<Text, BloomFilter> bloomFilter_param = new HashMap<Text, BloomFilter>();
  private static long startTime;
  private static long stopTime;

  public static void main(String[] args) throws Exception {
    
    Configuration conf1 = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: bloom filter <in> [<in>...] <out>");
      System.exit(2);
    }
    
    Job job1 = Job.getInstance(conf1, "tokenizer of data");
    job1.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job1, new Path(args[0]));
    job1.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 100000);

    job1.setJarByClass(Main.class);
    job1.setMapperClass(RatingMapper.class);
    job1.setCombinerClass(Job1Combiner.class);
    job1.setReducerClass(CreateParametersReducer.class);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class); // set output values for mapper
    
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
   
    FileOutputFormat.setOutputPath(job1,
        new Path(otherArgs[otherArgs.length - 1]));
    Boolean countSuccess = job1.waitForCompletion(true);
    if(!countSuccess) { /////////cambiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      System.exit(0);
    }
    
    
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "test");
    job2.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job2, new Path(args[0]));
    
    job2.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 100000);
    
    job2.setJarByClass(Main.class);
    job2.setMapperClass(BloomFiltersMapper.class);
    job2.setCombinerClass(BloomFiltersReducer.class);
    job2.setReducerClass(BloomFiltersReducer.class);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(BloomFilter.class); 
    
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(BloomFilter.class);
    
    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1] + "_2"));
    job2.setOutputFormatClass(SequenceFileOutputFormat.class);
    Boolean countSuccess2 = job2.waitForCompletion(true);
    if(!countSuccess2) {
      System.exit(0);
    }

    
    String outputTempDir = otherArgs[otherArgs.length - 1] + "_3";
    Configuration conf3 = new Configuration();
    Job job3 = Job.getInstance(conf3, "bloom filter creator");
    job3.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job3, new Path(args[0]));
    job3.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 50000);
    job3.setJarByClass(Main.class);
    job3.setMapperClass(TestMapper1.class);
    job3.setReducerClass(TestReducer1.class);

    job3.setMapOutputKeyClass(Text.class);
    job3.setMapOutputValueClass(Text.class); 
    
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job3, new Path(outputTempDir));
    Boolean countSuccess3 = job3.waitForCompletion(true);
    if(countSuccess3) {
      System.out.println("++++++++++++++");
      Job job3_1 = Job.getInstance(conf3, "JOB_3.1");
      job3_1.setJarByClass(Main.class);
      job3_1.setMapperClass(TestMapper2.class);
      job3_1.setReducerClass(TestReducer2.class);
      job3_1.setInputFormatClass(NLineInputFormat.class);
      //job3_1.setCombinerClass(TestCombiner2.class);
      job3_1.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 100000);
      job3_1.setMapOutputKeyClass(Text.class);
      job3_1.setMapOutputValueClass(Text.class); 
      job3_1.setOutputKeyClass(Text.class);
      job3_1.setOutputValueClass(DoubleWritable.class);
  
      NLineInputFormat.addInputPath(job3_1, new Path(outputTempDir));
      FileOutputFormat.setOutputPath(job3_1, new Path(otherArgs[otherArgs.length - 1] + "_3.1"));
      Boolean countSuccess3_1 = job3_1.waitForCompletion(true);
      if(!countSuccess3_1) {
        System.exit(0);
      }
    }
    
    System.exit(0);

  }

  private static void Job2(String[] otherArgs, String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "test");
    job2.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job2, new Path(args[0]));
    
    job2.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", Integer.parseInt(otherArgs[2]));
    job2.getConfiguration().setDouble("mapreduce.input.p_rate", Double.parseDouble(otherArgs[3]));
    job2.setJarByClass(Main.class);
    job2.setMapperClass(BloomFiltersMapper.class);
    job2.setCombinerClass(BloomFiltersReducer.class);
    job2.setReducerClass(BloomFiltersReducer.class);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(BloomFilter.class); 
    
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(BloomFilter.class);
    
    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1] + "_2"));
    job2.setOutputFormatClass(SequenceFileOutputFormat.class);
    Boolean countSuccess2 = job2.waitForCompletion(true);
    if(!countSuccess2) {
      System.exit(0);
    }
    
  }

  

}

  


