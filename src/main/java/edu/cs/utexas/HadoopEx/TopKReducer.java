package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private PriorityQueue<WordAndCount> pq;

    public void setup(Context context) {
        pq = new PriorityQueue<>(10);
    }

    public void reduce(Text text, Iterable<IntWritable> vals, Context context)
            throws IOException, InterruptedException {
        for (IntWritable val : vals) { // gives it to you as iterable even if only 1, need to make it a new text for
                                       // hadoop to understand
            pq.add(new WordAndCount(new Text(text), new IntWritable(val.get())));
        }

        while (pq.size() > 10) {
            pq.poll();
        }
    }
}
