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
public class WordCountMapperQues2 extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
    private Text documentWord = new Text();
    private final static IntWritable one = new IntWritable(1);
    private final static String LETTER_T = "T";

    public void map(LongWritable key, Text value,OutputCollector<Text,IntWritable> output,
                    Reporter reporter) throws IOException {

        String documentRowString = value.toString();

        // Tokenize the document row to extract individual tokens
        StringTokenizer tokenizer = new StringTokenizer(documentRowString);

        // Assuming the timestamp is in the format "T yyyy-MM-dd HH:mm:ss"
        while (tokenizer.hasMoreTokens() && LETTER_T.equals(tokenizer.nextToken())) {
            // Extract the next token as the date string
            String dateString = tokenizer.nextToken();

            // Extract the next token as the time string
            String timeString = tokenizer.nextToken();

            // Split the time string into time divisions using ":" as the delimiter
            String[] timeDivisions = timeString.split(":");

            // Check if the number of time divisions is equal to 3
            if (timeDivisions.length == 3) {
                // Extract the hour from the time divisions
                String hour = timeDivisions[0];

                // Set the current hour as the output key
                documentWord.set(hour);

                // Output the hour with a count of 1
                output.collect(documentWord, one);
            }
        }
    }
}