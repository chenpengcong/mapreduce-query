package cn.cpc.mrquery.job.groupby;

import cn.cpc.mrquery.writable.groupby.GBKeyWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;


/*
 * SELECT field1, field2, count(1) FROM table GROUP BY (field1, field2);
 */

public class GroupByJob extends Configured implements Tool {

    private static final String DELIMITER = ",";

    public static class GroupByMapper extends Mapper<LongWritable, Text, GBKeyWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(DELIMITER, -1);
            Text field1 = new Text(tokens[0]);
            Text field2 = new Text(tokens[1]);
            GBKeyWritable gbKey = new GBKeyWritable();
            gbKey.setGroup1(field1);
            gbKey.setGroup2(field2);
            context.write(gbKey, new IntWritable(1));
        }
    }

    public static class GroupByReducer extends Reducer<GBKeyWritable, IntWritable, NullWritable, Text> {
        @Override
        protected void reduce(GBKeyWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String field1 = key.getGroup1().toString();
            String field2 = key.getGroup2().toString();
            Iterator<IntWritable> iterator = values.iterator();
            int count = 0;
            while (iterator.hasNext()) {
                count += iterator.next().get();
            }
            String output = new StringBuilder()
                    .append(field1)
                    .append(DELIMITER)
                    .append(field2)
                    .append(DELIMITER)
                    .append(count).toString();

            context.write(NullWritable.get(), new Text(output));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("GroupByJob");
        job.setJarByClass(GroupByJob.class);
        job.setMapperClass(GroupByMapper.class);
        job.setReducerClass(GroupByReducer.class);
        job.setMapOutputKeyClass(GBKeyWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) {
            throw new RuntimeException("groupby failed");
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Configuration(), new GroupByJob(), args);
        System.exit(code);
    }

}
