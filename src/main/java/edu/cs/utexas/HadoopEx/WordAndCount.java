package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class WordAndCount implements Comparable<WordAndCount> {
    private final Text word;
    private final IntWritable count;

    public WordAndCount(Text word, IntWritable count) {
        this.word = word;
        this.count = count;
    }

    public Text getWord() {
        return word;
    }

    public IntWritable getCount() {
        return count;
    }

    @Override
    /**
     * compares two data objects by their counts
     * 
     * @param count2
     * @return 0 if both equal, positive if this < count2, negative otherwise;
     *         reverse of natural order
     */
    public int compareTo(WordAndCount count2) {
        return count2.getCount().compareTo(this.count);
    }
}