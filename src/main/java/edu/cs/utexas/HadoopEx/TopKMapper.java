package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.log4j.Logger;

public class TopKMapper extends Mapper<Text, Text, Text, IntWritable> {
    private Logger logger = Logger.getLogger(TopKMapper.class);

    private PriorityQueue<WordAndCount> pq;

    public void setup(Context context) {
        pq = new PriorityQueue<>();
    }

    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        WordAndCount elem = new WordAndCount(new Text(key), new IntWritable(Integer.parseInt(value.toString())));
        pq.add(elem);
        while (pq.size() > 10) {
            pq.poll();
        }
    }

    public void cleanup(Context context)
            throws IOException, InterruptedException {
        for (WordAndCount k : pq) {
            context.write(k.getWord(), k.getCount());
        }
    }
}
