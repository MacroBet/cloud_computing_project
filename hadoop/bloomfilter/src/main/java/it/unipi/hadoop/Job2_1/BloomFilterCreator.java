package it.unipi.hadoop;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import it.unipi.hadoop.*;

public class BloomFilterCreator {
  public static class RatingMapper
      extends Mapper<Object, Text, Text, Text> {
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
      extends Reducer<Text, Text, Text, BloomFilter> {

    public void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
      int sum = 0;
      ArrayList<String> ratings = new ArrayList<String>();
      for (Text val : values) {
        sum += 1;
        ratings.add(val.toString());
      }
      BloomFilter bloomFilter = new BloomFilter(sum);
      for (String rating : ratings) {
        bloomFilter.add(rating);
      }

      context.write(key, bloomFilter);
    }
  }
}
