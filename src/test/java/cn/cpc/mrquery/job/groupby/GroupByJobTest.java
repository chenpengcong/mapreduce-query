package cn.cpc.mrquery.job.groupby;

import cn.cpc.mrquery.job.join.ReduceJoinJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import static junit.framework.Assert.assertEquals;

public class GroupByJobTest {
    @Test
    public void test() throws Exception {
        Path inputPath = new Path("input/groupby");
        Path outputPath = new Path("output/groupby");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        FileSystem fs = FileSystem.getLocal(conf);
        if (fs.exists(inputPath)) {
            fs.delete(inputPath, true);
        }
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        createInput(fs, new Path(inputPath, "part-0"));
        GroupByJob groupByJob = new GroupByJob();
        groupByJob.setConf(conf);
        int res = groupByJob.run(new String[]{inputPath.toString(), outputPath.toString()});
        assertEquals(0, res);
        checkOutput(fs, new Path(outputPath, "part-r-00000"));
    }

    private void createInput(FileSystem fs, Path inputFile) throws IOException {
        ArrayList<String> inputList = new ArrayList<>();
        inputList.add("foo,bar");
        inputList.add("bar,baz");
        inputList.add("foo,bar");
        inputList.add("foo,baz");
        inputList.add("foo,bar");
        inputList.add("bar,baz");
        DataOutputStream file = fs.create(inputFile);
        for(String inp : inputList) {
            file.writeBytes(inp + "\n");
        }
        file.close();

    }

    private void checkOutput(FileSystem fs, Path outputFile) throws IOException {
        BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(outputFile)));
        assertEquals("bar,baz,2", bf.readLine());
        assertEquals("foo,bar,3", bf.readLine());
        assertEquals("foo,baz,1", bf.readLine());
        bf.close();
    }
}
