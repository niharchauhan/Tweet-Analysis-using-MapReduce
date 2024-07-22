package org.mp3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class WordCountMapperQues3 extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private final static String SLEEP = "sleep";
    private final static String T_STARTING = "T\t";
    private final static String W_STARTING = "W\t";

    private String postTime = "";
    private StringBuilder stringFormation = new StringBuilder();
    private Text documentWord = new Text();

    public void map(LongWritable key, Text value,OutputCollector<Text,IntWritable> output,
                    Reporter reporter) throws IOException {

        String documentRowString = value.toString();

        // Check if the document row starts with a specific prefix
        if (documentRowString.startsWith(W_STARTING)) {
            // Reset the stringFormation and append the substring starting from index 2
            stringFormation.setLength(0);
            stringFormation.append(documentRowString.substring(2));
        } else if (documentRowString.startsWith(T_STARTING) ) {
            // Split the documentRowString into divisions and set postTime if divisions length is greater than 1
            String[] divisions = documentRowString.split(" ");
            if (divisions.length > 1) {
                postTime = divisions[1];
            }
        } else if (documentRowString.isEmpty() && !postTime.isEmpty()) {
            // Extract post from stringFormation
            String post = stringFormation.toString();

            // Check if post contains the keyword "sleep"
            if (post.toLowerCase().contains(SLEEP)) {
                String[] timeDivisions = postTime.split(":");
                if (timeDivisions.length > 1) {
                    int hourPart = Integer.parseInt(timeDivisions[0]);
                    documentWord.set(String.valueOf(hourPart));
                    output.collect(documentWord, one);
                }
            }

            // Reset postTime and stringFormation
            postTime = "";
            stringFormation.setLength(0);
        }
    }
}