package org.mp3;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordCountMapperQues1 extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text documentWord = new Text();

    public void map(LongWritable key, Text value,OutputCollector<Text,IntWritable> output,
                    Reporter reporter) throws IOException {

        String documentRowString = value.toString();

        // Tokenize the document row to extract individual words
        StringTokenizer tokenizer = new StringTokenizer(documentRowString);

        // Process each word in the document and emit it with a count of 1
        while (tokenizer.hasMoreTokens()) {
            // Extract the next word from the tokenizer
            String word = tokenizer.nextToken();

            // Set the current word as the output key
            documentWord.set(word);

            // Output the word with a count of 1
            output.collect(documentWord, one);
        }
    }
}