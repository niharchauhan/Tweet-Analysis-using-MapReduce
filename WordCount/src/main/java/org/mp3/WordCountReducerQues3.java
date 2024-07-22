package org.mp3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducerQues3 extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values,OutputCollector<Text,IntWritable> output,
                       Reporter reporter) throws IOException {

        int aggregate = 0;

        // Calculate the total count by summing up all values
        while (values.hasNext()) {
            aggregate += values.next().get();
        }

        // Output the key along with its total count only when the aggregate exceeds 0.
        if (aggregate > 0) {
            // Output the key-value pair with the total count
            output.collect(key, new IntWritable(aggregate));
        }
    }
}