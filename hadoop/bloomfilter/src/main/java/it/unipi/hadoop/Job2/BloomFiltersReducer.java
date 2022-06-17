package it.unipi.hadoop.Job2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import it.unipi.hadoop.*;

public class BloomFiltersReducer extends Reducer<Text, BloomFilter, Text, BloomFilter> {


    public void reduce(Text key, Iterable<BloomFilter> values, Context context) throws IOException, InterruptedException {
            
      BloomFilter temp_BloomFilter= new BloomFilter(1,1);
        int i =0;
        for (BloomFilter bloomFilter : values) {
           if(i==0){
            temp_BloomFilter = new BloomFilter(bloomFilter.get_size(), bloomFilter.get_hash_count());
           }
            temp_BloomFilter.or(bloomFilter);
           i++;
        }

        context.write(key, temp_BloomFilter);
    
      }


     
}
