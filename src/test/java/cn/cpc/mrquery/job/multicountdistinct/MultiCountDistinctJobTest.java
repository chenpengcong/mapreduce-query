package cn.cpc.mrquery.job.multicountdistinct;

import cn.cpc.mrquery.job.countdistinct.CountDistinctJob;
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

public class MultiCountDistinctJobTest {
    @Test
    public void test() throws Exception {
        Path inputPath = new Path("input/multicountdistinct");
        Path outputPath = new Path("output/multicountdistinct");

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
        MultiCountDistinctJob multiCountDistinctJob = new MultiCountDistinctJob();
        multiCountDistinctJob.setConf(conf);
        int res = multiCountDistinctJob.run(new String[]{inputPath.toString(), outputPath.toString()});
        assertEquals(0, res);
        checkOutput(fs, new Path(outputPath, "part-r-00000"));
    }

    private void createInput(FileSystem fs, Path inputFile) throws IOException {
        ArrayList<String> inputList = new ArrayList<>();
        inputList.add("1,foo,bar,");
        inputList.add("1,baz,foo");
        inputList.add("1,bar,bar");
        inputList.add("3,bar,baz");
        inputList.add("3,bar,foo");
        inputList.add("2,baz,baz");
        inputList.add("2,bar,foo");
        inputList.add("2,baz,bar");
        DataOutputStream file = fs.create(inputFile);
        for(String inp : inputList) {
            file.writeBytes(inp + "\n");
        }
        file.close();

    }

    private void checkOutput(FileSystem fs, Path outputFile) throws IOException {
        BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(outputFile)));
        assertEquals("1,3,2", bf.readLine());
        assertEquals("2,2,3", bf.readLine());
        assertEquals("3,1,2", bf.readLine());
        bf.close();
    }
}
