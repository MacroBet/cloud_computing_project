package it.unipi.hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.VIntWritable;
import java.util.BitSet;
import org.apache.hadoop.util.hash.MurmurHash;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BloomFilter implements Writable, WritableComparable<BloomFilter> {
  private VIntWritable size;
  private VIntWritable hashCount;
  private BytesWritable bytes;
  private BitSet bitset;

  private int get_size(int n, float p) {
    return (int) (-(n * Math.log(p)) / Math.pow((Math.log(2)), 2));
  }

  private int get_hash_count(int size, int n) {
    return (int) ((size / n) * Math.log(2));
  }

  public BloomFilter() {
    bytes = new BytesWritable();
    size = new VIntWritable();
    hashCount = new VIntWritable();
  }

  public BloomFilter(int n) {
    int size = get_size(n, (float) 0.01);
    int hashCount = get_hash_count(size, n);
    this.size = new VIntWritable(size);
    this.hashCount = new VIntWritable(hashCount);
    this.bitset = new BitSet(size);
    this.bytes = new BytesWritable(bitset.toByteArray());
  }

  public BloomFilter(BloomFilter b) {

    this.size = new VIntWritable(b.get_size());
    this.hashCount = new VIntWritable(b.get_hash_count());
    this.bitset = b.get_bitset();
    this.bytes = b.bytes;

  }

  public BloomFilter(int size, int hashCount) {
    this.size = new VIntWritable(size);
    this.hashCount = new VIntWritable(hashCount);
    this.bitset = new BitSet(size);
    this.bytes = new BytesWritable(bitset.toByteArray());
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {

    bytes.write(dataOutput);
    size.write(dataOutput);
    hashCount.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    bytes.readFields(dataInput);
    size.readFields(dataInput);
    hashCount.readFields(dataInput);
    this.bitset = BitSet.valueOf(bytes.getBytes());
  }

 
  public void or(BloomFilter bloomFilter) {
    this.bitset.or(bloomFilter.get_bitset());
    this.bytes = new BytesWritable(bitset.toByteArray());
  }

  private long getUnsignedInt(int x) {
    return x & 0x00000000ffffffffL;
  }

  public void add(String s) {
    int hashCount = this.hashCount.get();
    int size = this.size.get();
    for (int i = 0; i < hashCount; i++) {

      String id = s.replace("t", "0");
      int digest = (int) (getUnsignedInt(MurmurHash.getInstance().hash(id.getBytes(), i)) % size);
      this.bitset.set(digest);

    }
    this.bytes = new BytesWritable(bitset.toByteArray());
  }

  public boolean check(String s) {
    int hashCount = this.hashCount.get();
    //BitSet bitset = BitSet.valueOf(this.bytes.getBytes());
    int size = this.size.get();
    for (int i = 0; i < hashCount; i++) {
      String id = s.replace("t", "0");
      int digest = (int) (getUnsignedInt(MurmurHash.getInstance().hash(id.getBytes(), i)) % size);
      // System.out.println("DIGEST PRESENTE: " + digest);
    //  if (!bitset.get(digest)) {
        // System.out.println("DIGEST ASSENTE: " + digest);
      //  return false;
    //  }
    }
    return true;
  }

  public int get_hash_count() { return this.hashCount.get(); }

  public int get_size() { return this.size.get(); }

  public BitSet get_bitset() { return this.bitset; }

  public BytesWritable gBytesWritable() { return this.bytes; }

  public void set_hash_count(VIntWritable k) { this.hashCount = k; }

  public void set_size(VIntWritable m) { this.hashCount = m; }

  @Override
  public int compareTo(BloomFilter o) {
    int i = size.compareTo(o.size);
    if (i != 0) return i;

    i = hashCount.compareTo(o.hashCount);
    if (i != 0) return i;

    i = bytes.compareTo(o.bytes);
    if (i != 0) return i;

    return 0;
  
  }

}