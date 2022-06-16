package it.unipi.hadoop.Job1;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text word = new Text();

    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      while (itr.hasMoreTokens()) {
        String ratingRaw = itr.nextToken().toString();
        int rating = Math.round(Float.parseFloat(ratingRaw.split("\t")[1]));
        Text movieId = new Text(ratingRaw.split("\t")[0]);
        word.set("" + rating);
        // context.write(word, movieId);
        context.write(word, one);
      }
    }
  }



