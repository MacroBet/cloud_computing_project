package it.unipi.hadoop.Job3;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TestReducer2 extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    Double falsePositive = 0.0;
    int n = 0;
    for (Text val : values) {
        falsePositive += Double.parseDouble(val.toString());
        n++;
    }

    context.write(key, new Text(String.valueOf(falsePositive/n)));
  }
}
