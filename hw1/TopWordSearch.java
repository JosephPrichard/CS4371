import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;

public class TopWordSearch {

    private final static int N = 10;
    private final static Comparator<SimpleEntry<String, Integer>> entryComparator
        = Comparator.comparing(SimpleEntry::getValue, Comparator.reverseOrder());

    public static class CountMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text wordText = new Text(); // type of output key

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word : words) {
                // normalize words
                word = word.replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
                if (word.isEmpty()) {
                    continue;
                }

                wordText.set(word); // set word as each input keyword
                context.write(wordText, one); // create a pair <keyword, 1>
            }
        }
    }

    public static class CountReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0; // initialize the sum for each keyword
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result); // create a pair <keyword, number of occurrences>
        }
    }

    public static class TopMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

        // local rank for this mapper
        private final List<SimpleEntry<String, Integer>> ranked = new ArrayList<>();
        private final static IntWritable count = new IntWritable();
        private final Text wordText = new Text();

        public void map(LongWritable key, Text value, Context context) {
            String[] entry = value.toString().split("\t");
            String word = entry[0];
            Integer count = Integer.parseInt(entry[1]);
            // add the new element to the rank list, and then resort it
            ranked.add(new SimpleEntry<>(word, count));
            ranked.sort(entryComparator);
            // remove the oldest element
            if (ranked.size() > N) {
                ranked.remove(ranked.size() - 1);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (SimpleEntry<String, Integer> entry : ranked) {
                wordText.set(entry.getKey());
                count.set(entry.getValue());
                context.write(wordText, count);
            }
        }
    }

    public static class TopReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

        // absolute rank for this reducer (all the keys)
        private final List<SimpleEntry<String, Integer>> ranked = new ArrayList<>();
        private final static IntWritable count = new IntWritable();
        private final Text wordText = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            // we should only have a single value since key is word, and we already grouped by words
            int value = 0;
            for (IntWritable val : values) {
                value = val.get();
            }
            // add the new element to the rank list, and then resort it
            ranked.add(new SimpleEntry<>(key.toString(), value));
            ranked.sort(entryComparator);
            // remove the oldest element
            if (ranked.size() > N) {
                ranked.remove(ranked.size() - 1);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (SimpleEntry<String, Integer> entry : ranked) {
                wordText.set(entry.getKey());
                count.set(entry.getValue());
                context.write(wordText, count);
            }
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: TopWordSearch <in> <out>");
            System.exit(2);
        }

        String temp = "wordcount-job-output-" + UUID.randomUUID();

        Job job1 = Job.getInstance(conf, "wordcount");
        job1.setJarByClass(TopWordSearch.class);
        job1.setMapperClass(CountMapper.class);
        job1.setReducerClass(CountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(temp));
        if(!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "topwordsearch");
        job2.setJarByClass(TopWordSearch.class);
        job2.setMapperClass(TopMapper.class);
        job2.setReducerClass(TopReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(temp));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
