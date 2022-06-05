package it.unipi.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

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

public class BloomFilter {

  public int get_size(int n, float p) {
    return (int) (-(n * Math.log(p)) / Math.pow((Math.log(2)), 2));
  }

  public int get_hash_count(int size, int n) {
    return (int) ((size / n) * Math.log(2));
  }

  public static class RatingMapper
      extends Mapper<Object, Text, Text, Text> {

    private final static Text one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      while (itr.hasMoreTokens()) {
        String ratingRaw = itr.nextToken().toString();
        int rating = Math.round(Float.parseFloat(ratingRaw.split("\t")[1]));
        Text movieId = new Text(ratingRaw.split("\t")[0]);
        word.set("" + rating);
        context.write(word, movieId);
      }
    }
  }

  public static class CreateBloomFilterReducer
      extends Reducer<Text, Text, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      int sum = 0;
      ArrayList<String> ratings = new ArrayList<String>();
      for (IntWritable val : values) {
        sum += 1
        ratings.add(val.get());
      }
      // TODO calculate sizes given sum
      // for (String rating : ratings) {
      //   // result.set(rating);
      //   // ADD TO BLOOM FILTER 
      // }
      result.set(sum);
      context.write(key, result);
    }
  }

  // public BitArray insert_ratings_in_bloom_filters(Iterable<String> lines, Iterable<Integer> SIZES,
  //     Iterable<Integer> HASH_COUNTS) {
  //   // return BitArray;
  // }

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
    job1.setCombinerClass(CreateBloomFilterReducer.class);
    job1.setReducerClass(CreateBloomFilterReducer.class);
    // job1.setMapOutputKeyClass(theClass); // set output values for mapper
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    
    // for (int i = 0; i < otherArgs.length - 1; ++i) {
    //   FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
    // }

    FileOutputFormat.setOutputPath(job1,
        new Path(otherArgs[otherArgs.length - 1]));
    Boolean countSuccess = job1.waitForCompletion(true);
    if(!countSuccess) {
      System.exit(0);
    }
    

    // try {
    //   Configuration conf = new Configuration();
    //   FileSystem fs = FileSystem.get(conf);
    //   // Hadoop DFS Path - Input file
    //   Path inFile = new Path(otherArgs[otherArgs.length - 1]);
        
    //   // Check if input is valid
    //   if (!fs.exists(inFile)) {
    //     System.out.println("Input file not found");
    //     throw new IOException("Input file not found");
    //   }
			
    //   // open and read from file
    //   FSDataInputStream in = fs.open(inFile);
    //   // system.out as output stream to display 
    //   //file content on terminal 
    //   OutputStream out = System.out;
    //   byte buffer[] = new byte[256];
    //   try {
    //     int bytesRead = 0;
    //     while ((bytesRead = in.read(buffer)) > 0) {
    //       out.write(buffer, 0, bytesRead);
    //     }
    //   } catch (IOException e) {
    //     System.out.println("Error while copying file");
    //   } finally {
    //      // Closing streams
    //     in.close();
        
    //     out.close();
    //   }      
    // } catch (IOException e) {
    //   // TODO Auto-generated catch block
    //   e.printStackTrace();
    // }		 
  
    System.exit(0);
    



    // Configuration conf2 = new Configuration();
    // Job job2 = Job.getInstance(conf2, "bloom filter creator");

    // Configuration conf3 = new Configuration();
    // Job job3 = Job.getInstance(conf3, "testing bloom filter");
    
  }
}
