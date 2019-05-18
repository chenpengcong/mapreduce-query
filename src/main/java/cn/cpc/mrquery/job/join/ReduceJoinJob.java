package cn.cpc.mrquery.job.join;

import cn.cpc.mrquery.mr.join.RJGroupComparator;
import cn.cpc.mrquery.mr.join.RJPartitioner;
import cn.cpc.mrquery.writable.join.RJKeyWritable;
import cn.cpc.mrquery.writable.join.RJValWritable;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/*
 * SELECT table1.related_field, table1.val, table2.val FROM table1 LEFT JOIN table2 ON table1.related_field = table2.related_field;
 */
public class ReduceJoinJob extends Configured implements Tool {

    private static final int TABLE1_TAG = 1;
    private static final int TABLE2_TAG = 0;
    private static final String DELIMITER = ",";

    public static class ReduceJoinMapper1 extends Mapper<LongWritable, Text, RJKeyWritable, RJValWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(DELIMITER, -1);
            Text relatedField = new Text(tokens[0]);
            Text val = new Text(tokens[1]);
            IntWritable tag = new IntWritable(TABLE1_TAG);

            RJKeyWritable rjKey = new RJKeyWritable();
            rjKey.setRelatedField(relatedField);
            rjKey.setSortedField(tag);

            RJValWritable rjVal = new RJValWritable();
            rjVal.setSortedField(tag);
            rjVal.setVal(val);

            context.write(rjKey,rjVal);
        }
    }

    public static class ReduceJoinMapper2 extends Mapper<LongWritable, Text, RJKeyWritable, RJValWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(DELIMITER, -1);
            Text relatedField = new Text(tokens[0]);
            Text val = new Text(tokens[1]);
            IntWritable tag = new IntWritable(TABLE2_TAG);

            RJKeyWritable rjKey = new RJKeyWritable();
            rjKey.setRelatedField(relatedField);
            rjKey.setSortedField(tag);

            RJValWritable rjVal = new RJValWritable();
            rjVal.setSortedField(tag);
            rjVal.setVal(val);

            context.write(rjKey,rjVal);
        }
    }

    public static class ReduceJoinReducer extends Reducer<RJKeyWritable, RJValWritable, NullWritable, Text> {
        @Override
        protected void reduce(RJKeyWritable key, Iterable<RJValWritable> values, Context context) throws IOException, InterruptedException {
            String relatedField = key.getRelatedField().toString();
            Iterator<RJValWritable> iterator = values.iterator();
            String table2Val = "";
            while (iterator.hasNext()) {
                RJValWritable rjVal = iterator.next();
                int sortedFiled = rjVal.getSortedField().get();
                switch (sortedFiled) {
                    case TABLE1_TAG:
                        String table1Val = rjVal.getVal().toString();
                        String result = new StringBuilder()
                                .append(relatedField)
                                .append(DELIMITER)
                                .append(table1Val)
                                .append(DELIMITER)
                                .append(table2Val)
                                .toString();
                        context.write(NullWritable.get(), new Text(result));
                        break;
                    case TABLE2_TAG:
                        table2Val = rjVal.getVal().toString();
                        break;
                        default:
                            break;
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("ReduceJoinJob");
        job.setJarByClass(ReduceJoinJob.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setMapOutputKeyClass(RJKeyWritable.class);
        job.setMapOutputValueClass(RJValWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setGroupingComparatorClass(RJGroupComparator.class);
        job.setPartitionerClass(RJPartitioner.class);

        Path table1Path = new Path(args[0]);
        Path table2Path = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        MultipleInputs.addInputPath(job, table1Path, TextInputFormat.class, ReduceJoinMapper1.class);
        MultipleInputs.addInputPath(job, table2Path, TextInputFormat.class, ReduceJoinMapper2.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) {
            throw new RuntimeException("reduce join failed");
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Configuration(), new ReduceJoinJob(), args);
        System.exit(code);
    }
}
