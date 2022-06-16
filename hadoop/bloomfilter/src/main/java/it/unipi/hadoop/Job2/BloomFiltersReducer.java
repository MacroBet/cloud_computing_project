package it.unipi.hadoop.Job2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.google.common.collect.Iterables;
import it.unipi.hadoop.*;

public class BloomFiltersReducer extends Reducer<Text, BloomFilter, Text, Text> {

    public void reduce(Text key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {
            
        BloomFilter temp_BloomFilter = new BloomFilter(Iterables.get(values, 0).get_size(), 
                                                                Iterables.get(values, 0).get_hash_count());
        for (BloomFilter bloomFilter : values) {

            temp_BloomFilter.get_bitSet().or(bloomFilter.get_bitSet());
        }

        context.write(key, new Text(temp_BloomFilter.toString()));
    
      }
     
}
