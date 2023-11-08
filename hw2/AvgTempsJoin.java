import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class AvgTempsJoin {
    public static class AvgMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text cityText = new Text();
        private final Text tempWriteable = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String country = tokens[1];
            String city = tokens[3];
            String temp = tokens[7];

            cityText.set(country + ", " + city);
            tempWriteable.set(temp);
            context.write(cityText, tempWriteable);
        }
    }

    public static class CapitalMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text cityText = new Text();
        private final Text valueText = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String country = tokens[0].replaceAll("\"", "");
            String capital = tokens[1].replaceAll("\"", "");;

            cityText.set(country + ", " + capital);
            valueText.set("Capital");
            context.write(cityText, valueText);
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        private final Text cityText = new Text();
        private final Text tempText = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean isCapital = false;
            float tempAverage = 0;
            int tempCount = 0;
            for (Text value : values) {
                String valueStr = value.toString();
                try {
                    float temp = Float.parseFloat(valueStr);
                    tempAverage = tempAverage * tempCount / (tempCount + 1) + temp / (tempCount + 1);
                    tempCount += 1;
                } catch(NumberFormatException ignored) {
                    if (valueStr.equals("Capital")) {
                        isCapital = true;
                    }
                }
            }
            if (tempCount >= 1 && isCapital) {
                cityText.set(key);
                tempText.set(String.valueOf(tempAverage));
                context.write(cityText, tempText);
            }
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: <class-name> <in> <in1> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "avgtempsjoin");
        job.setJarByClass(AvgTempsJoin.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AvgMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CapitalMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
