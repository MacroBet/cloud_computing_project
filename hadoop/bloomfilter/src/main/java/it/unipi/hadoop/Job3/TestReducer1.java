package it.unipi.hadoop.Job3;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TestReducer1 extends Reducer<Text, Text, Text, Text> {

  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    double fp = 0.0;
    int n = 0;
    for (Text val : values) {
      if(val.toString().equals("1"))
        fp++;
      if(val.toString().equals("0"))
        n++;
      else
        context.write(key, new Text(val));
    }
    if(n != 0)
      context.write(key, new Text(String.valueOf(fp/n)));

  }
}
