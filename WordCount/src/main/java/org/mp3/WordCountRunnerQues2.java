package org.mp3;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCountRunnerQues2 {
    public static void main(String[] args) throws IOException{
        // Create a JobConf object for configuring the MapReduce job
        JobConf conf = new JobConf(WordCountRunnerQues2.class);
        conf.setJobName("WordCountQues2");

        // Specify the output key and value classes for the Mapper and Reducer
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        // Set the Mapper class to be used in the job
        conf.setMapperClass(WordCountMapperQues2.class);

        // Set the Combiner class to be used during the Map phase
        conf.setCombinerClass(WordCountReducerQues2.class);

        // Set the Reducer class to be used in the job
        conf.setReducerClass(WordCountReducerQues2.class);

        // Specify the input format for reading input data (assuming text input)
        conf.setInputFormat(TextInputFormat.class);

        // Specify the output format for writing the results (assuming text output)
        conf.setOutputFormat(TextOutputFormat.class);

        // Set the input and output paths based on the command line arguments
        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));

        // Run the MapReduce job using the configured JobConf
        JobClient.runJob(conf);
    }
}