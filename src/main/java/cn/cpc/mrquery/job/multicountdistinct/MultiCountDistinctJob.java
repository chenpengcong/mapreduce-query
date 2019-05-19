package cn.cpc.mrquery.job.multicountdistinct;

import cn.cpc.mrquery.mr.multicountdistinct.MCDGroupComparator;
import cn.cpc.mrquery.mr.multicountdistinct.MCDPartitioner;
import cn.cpc.mrquery.writable.multicountdistinct.MCDKeyWritable;
import cn.cpc.mrquery.writable.multicountdistinct.MCDValWritable;
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
 * SELECT group_field, count(distinct distinct_field1), count(distinct distinct_field2) FROM table GROUP BY group_field;
 */


public class MultiCountDistinctJob extends Configured implements Tool {

    private static final String DELIMITER = ",";
    private static final int DISTINCT_FIELD1_SN = 0;
    private static final int DISTINCT_FIELD2_SN = 1;

    public static class MultiCountDistinctMapper extends Mapper<LongWritable, Text, MCDKeyWritable, MCDValWritable> {

        private static final IntWritable DISTINCT_FIELD1_SN_Writable = new IntWritable(DISTINCT_FIELD1_SN);
        private static final IntWritable DISTINCT_FIELD2_SN_Writable = new IntWritable(DISTINCT_FIELD2_SN);


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(DELIMITER, -1);

            Text groupField = new Text(tokens[0]);
            Text distinctField1 = new Text(tokens[1]);
            Text distinctField2 = new Text(tokens[2]);

            MCDKeyWritable field1Key = new MCDKeyWritable();
            field1Key.setGroupField(groupField);
            field1Key.setSerialNum(DISTINCT_FIELD1_SN_Writable);
            field1Key.setDistinctField(distinctField1);

            MCDValWritable field1Val = new MCDValWritable();
            field1Val.setSerialNum(DISTINCT_FIELD1_SN_Writable);
            field1Val.setDistinctField(distinctField1);
            context.write(field1Key, field1Val);

            MCDKeyWritable field2Key = new MCDKeyWritable();
            field2Key.setGroupField(groupField);
            field2Key.setSerialNum(DISTINCT_FIELD2_SN_Writable);
            field2Key.setDistinctField(distinctField2);

            MCDValWritable field2Val = new MCDValWritable();
            field2Val.setSerialNum(DISTINCT_FIELD2_SN_Writable);
            field2Val.setDistinctField(distinctField2);
            context.write(field2Key, field2Val);
        }
    }

    public static class MultiCountDistinctReducer extends Reducer<MCDKeyWritable, MCDValWritable, NullWritable, Text> {
        @Override
        protected void reduce(MCDKeyWritable key, Iterable<MCDValWritable> values, Context context) throws IOException, InterruptedException {
            String groupField = key.getGroupField().toString();
            Iterator<MCDValWritable> iterator = values.iterator();
            int lastSerialNum = -1;
            String lastDistinctField = "";
            long count1 = 0;
            long count2 = 0;

            while (iterator.hasNext()) {
                MCDValWritable mcdVal = iterator.next();
                int serialNum = mcdVal.getSerialNum().get();
                String distinctField = mcdVal.getDistinctField().toString();

                if (serialNum != lastSerialNum) {
                    lastDistinctField = "";
                    lastSerialNum = serialNum;
                }

                switch (serialNum) {
                    case DISTINCT_FIELD1_SN:
                        if (!distinctField.equals(lastDistinctField)) {
                            lastDistinctField = distinctField;
                            count1++;
                        }
                        break;
                    case DISTINCT_FIELD2_SN:
                        if (!distinctField.equals(lastDistinctField)) {
                            lastDistinctField = distinctField;
                            count2++;
                        }
                        break;
                }
            }
            String output = new StringBuilder()
                    .append(groupField)
                    .append(DELIMITER)
                    .append(count1)
                    .append(DELIMITER)
                    .append(count2)
                    .toString();
            context.write(NullWritable.get(), new Text(output));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("MultiCountDistinctJob");
        job.setJarByClass(MultiCountDistinctJob.class);
        job.setMapperClass(MultiCountDistinctMapper.class);
        job.setReducerClass(MultiCountDistinctReducer.class);
        job.setMapOutputKeyClass(MCDKeyWritable.class);
        job.setMapOutputValueClass(MCDValWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setGroupingComparatorClass(MCDGroupComparator.class);
        job.setPartitionerClass(MCDPartitioner.class);


        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) {
            throw new RuntimeException("MultiCountDistinct failed");
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new Configuration(), new MultiCountDistinctJob(), args);
        System.exit(code);
    }
}
