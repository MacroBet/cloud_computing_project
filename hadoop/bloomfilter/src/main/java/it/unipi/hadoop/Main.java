package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import it.unipi.hadoop.Job1.*;
import it.unipi.hadoop.Job2.BloomFiltersMapper;
import it.unipi.hadoop.Job2.BloomFiltersReducer;
import it.unipi.hadoop.Job3.TestMapper;
import it.unipi.hadoop.Job3.TestReducer;
import org.apache.hadoop.fs.FileSystem;



public class Main {

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
    job1.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 600000);

    job1.setJarByClass(Main.class);
    job1.setMapperClass(RatingMapper.class);
    //job1.setCombinerClass(CreateBloomFilterReducer.class);
    job1.setReducerClass(CreateParametersReducer.class);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class); // set output values for mapper
    
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
   
    FileOutputFormat.setOutputPath(job1,
        new Path(otherArgs[otherArgs.length - 1]));
    Boolean countSuccess = job1.waitForCompletion(true);
    if(!countSuccess) {
      System.exit(0);
    }
    
  
    //System.exit(0);
    

    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "test");
    job2.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job2, new Path(args[0]));
    job2.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 600000);

    job2.setJarByClass(Main.class);
    job2.setMapperClass(BloomFiltersMapper.class);
    //job2.setCombinerClass(CreateBloomFilterReducer.class);
    job2.setReducerClass(BloomFiltersReducer.class);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(BloomFilter.class); // set output values for mapper
    
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(BloomFilter.class);

    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1] + "_2"));
    Boolean countSuccess2 = job2.waitForCompletion(true);
    if(!countSuccess2) {
      System.exit(0);
    }
    
    ArrayList<BloomFilter> bloomFilter_param = new ArrayList<BloomFilter> ();

    try {
      System.out.println("Readeing Sequence File");
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      Path pt = new Path("hdfs://hadoop-namenode:9820/user/hadoop/output_2/part-r-00000");// Location of file in HDFS
      SequenceFile.Reader reader = null; 
      try{  
      reader = new SequenceFile.Reader(conf, Reader.file(pt)); 
      
        boolean hasNext;
        do {
          System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
          Text key = new Text();
          BloomFilter bf = new BloomFilter();
          hasNext = reader.next(key, bf);
          System.out.println(hasNext);
          System.out.println(bf.get_size());
          bloomFilter_param.add(bf);

        } while(hasNext);
      }
        finally {
          IOUtils.closeStream(reader);
      }


  } catch (Exception e) { e.printStackTrace(); }

    /* 
    Configuration conf3 = new Configuration();
    Job job3 = Job.getInstance(conf3, "bloom filter creator");
    job3.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job3, new Path(args[0]));
    job3.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 600000);

    job3.setJarByClass(Main.class);
    job3.setMapperClass(TestMapper.class);
    //job3.setCombinerClass(CreateBloomFilterReducer.class);
    job3.setReducerClass(TestReducer.class);

    job3.setMapOutputKeyClass(Text.class);
    job3.setMapOutputValueClass(IntWritable.class); // set output values for mapper
    
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(IntWritable.class);

    FileOutputFormat.setOutputPath(job3, new Path(otherArgs[otherArgs.length - 1] + "_3"));
    Boolean countSuccess3 = job3.waitForCompletion(true);
    if(!countSuccess3) {
      System.exit(0);
    }
    */
    System.exit(0);

    
  }
}

