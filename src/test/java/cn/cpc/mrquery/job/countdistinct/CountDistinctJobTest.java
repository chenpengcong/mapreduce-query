package cn.cpc.mrquery.job.countdistinct;

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

public class CountDistinctJobTest {
    @Test
    public void test() throws Exception {
        Path inputPath = new Path("input/countdistinct");
        Path outputPath = new Path("output/countdistinct");

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
        CountDistinctJob countDistinctJob = new CountDistinctJob();
        countDistinctJob.setConf(conf);
        int res = countDistinctJob.run(new String[]{inputPath.toString(), outputPath.toString()});
        assertEquals(0, res);
        checkOutput(fs, new Path(outputPath, "part-r-00000"));
    }

    private void createInput(FileSystem fs, Path inputFile) throws IOException {
        ArrayList<String> inputList = new ArrayList<>();
        inputList.add("1,foo");
        inputList.add("1,baz");
        inputList.add("1,bar");
        inputList.add("3,bar");
        inputList.add("2,baz");
        inputList.add("2,bar");
        inputList.add("2,baz");
        DataOutputStream file = fs.create(inputFile);
        for(String inp : inputList) {
            file.writeBytes(inp + "\n");
        }
        file.close();

    }

    private void checkOutput(FileSystem fs, Path outputFile) throws IOException {
        BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(outputFile)));
        assertEquals("1,3", bf.readLine());
        assertEquals("2,2", bf.readLine());
        assertEquals("3,1", bf.readLine());
        bf.close();
    }
}
