package org.mp3;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WordCountReducerQues1 extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output,
                       Reporter reporter) throws IOException {
        int aggregrate = 0;

        // Calculate the total count by summing up all values
        while (values.hasNext()) {
            aggregrate += values.next().get();
        }

        // Output the key along with its associated total count
        output.collect(key,new IntWritable(aggregrate));
    }
}