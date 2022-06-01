package it.unipi.hadoop;

public class BitArray {

    private static final int ALL_ONES = 0xFFFFFFFF;
    private static final int WORD_SIZE = 32;
    private int bits[] = null;

    public BitArray(int size) {
        bits = new int[size / WORD_SIZE + (size % WORD_SIZE == 0 ? 0 : 1)];
    }

    public boolean getBit(int pos) {
        return (bits[pos / WORD_SIZE] & (1 << (pos % WORD_SIZE))) != 0;
    }

    public void setBit(int pos, boolean b) {
        int word = bits[pos / WORD_SIZE];
        int posBit = 1 << (pos % WORD_SIZE);
        if (b) {
            word |= posBit;
        } else {
            word &= (ALL_ONES - posBit);
        }
        bits[pos / WORD_SIZE] = word;
    }

}
