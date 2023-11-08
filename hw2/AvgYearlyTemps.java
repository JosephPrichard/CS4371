import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AvgYearlyTemps {

    public static class AvgMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

        private final Text yearText = new Text();
        private final FloatWritable tempWriteable = new FloatWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String region = tokens[0];
            String year = tokens[6];

            if (region.equals("Asia")) {
                try {
                    float temp = Float.parseFloat(tokens[7]);
                    yearText.set(year);
                    tempWriteable.set(temp);
                    context.write(yearText, tempWriteable);
                } catch(NumberFormatException ignored) {
                    System.out.println("Ignored");
                }
            }
        }
    }

    public static class AvgReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private final Text yearText = new Text();
        private final FloatWritable avgWriteable = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float average = 0;
            int count = 0;
            for (FloatWritable value : values) {
                average = average * count / (count + 1) + value.get() / (count + 1);
                count += 1;
            }

            yearText.set(key);
            avgWriteable.set(average);
            context.write(yearText, avgWriteable);
        }
    }


    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: <class-name> <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "avgtempsyear");
        job.setJarByClass(AvgYearlyTemps.class);
        job.setMapperClass(AvgMapper.class);
        job.setReducerClass(AvgReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}