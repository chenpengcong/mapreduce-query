package cn.cpc.mrquery.job.countdistinct;

import cn.cpc.mrquery.mr.countdistinct.CDGroupComparator;
import cn.cpc.mrquery.mr.countdistinct.CDPartitioner;
import cn.cpc.mrquery.writable.countdistinct.CDKeyWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
 * SELECT group_field, count(distinct distinct_field) FROM table GROUP BY group_field;
 */

public class CountDistinctJob extends Configured implements Tool {

    private static final String DELIMITER = ",";

    public static class CountDistinctMapper extends Mapper<LongWritable, Text, CDKeyWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(DELIMITER, -1);

            Text groupField = new Text(tokens[0]);
            Text distinctField = new Text(tokens[1]);
            CDKeyWritable cdKey = new CDKeyWritable();
            cdKey.setGroupField(groupField);
            cdKey.setDistinctField(distinctField);

            context.write(cdKey, distinctField);
        }
    }

    public static class CountDistinctReducer extends Reducer<CDKeyWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(CDKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String groupField = key.getGroupField().toString();
            Iterator<Text> iterator = values.iterator();
            String lastVal = "";
            long count = 0;
            while (iterator.hasNext()) {
                String distinctField = iterator.next().toString();
                if (!distinctField.equals(lastVal)) {
                    count++;
                    lastVal = distinctField;
                }
            }
            String output = new StringBuilder()
                    .append(groupField)
                    .append(DELIMITER)
                    .append(count)
                    .toString();
            context.write(NullWritable.get(), new Text(output));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("CountDistinctJob");
        job.setJarByClass(CountDistinctJob.class);
        job.setMapperClass(CountDistinctMapper.class);
        job.setReducerClass(CountDistinctReducer.class);
        job.setMapOutputKeyClass(CDKeyWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setGroupingComparatorClass(CDGroupComparator.class);
        job.setPartitionerClass(CDPartitioner.class);


        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) {
            throw new RuntimeException("CountDistinct failed");
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Configuration(), new CountDistinctJob(), args);
        System.exit(code);
    }


}
