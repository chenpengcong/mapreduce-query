package cn.cpc.mrquery.job.join;

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

public class ReduceJoinJobTest {
    @Test
    public void test() throws Exception {
        Path table1Path = new Path("input/join/table1");
        Path table2Path = new Path("input/join/table2");
        Path outputPath = new Path("output/join/");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        FileSystem fs = FileSystem.getLocal(conf);
        if (fs.exists(table1Path)) {
            fs.delete(table1Path, true);
        }
        if (fs.exists(table2Path)) {
            fs.delete(table2Path, true);
        }
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        createInput(fs, new Path(table1Path, "part-0"), new Path(table2Path, "part-0"));
        ReduceJoinJob reduceJoinJob = new ReduceJoinJob();
        reduceJoinJob.setConf(conf);
        int res = reduceJoinJob.run(new String[]{table1Path.toString(), table2Path.toString(), outputPath.toString()});
        assertEquals(0, res);
        checkOutput(fs, new Path(outputPath, "part-r-00000"));
    }

    private void createInput(FileSystem fs, Path table1File, Path table2File) throws IOException {
        ArrayList<String> inputList = new ArrayList<>();
        inputList.add("foo,table1_foo");
        inputList.add("bar,table1_bar");
        inputList.add("foo,table1_foo");
        inputList.add("foo,table1_foo");
        inputList.add("baz,table1_baz");

        DataOutputStream file = fs.create(table1File);
        for(String inp : inputList) {
            file.writeBytes(inp + "\n");
        }
        file.close();

        inputList.clear();
        inputList.add("foo,table2_foo");
        inputList.add("bar,table2_bar");
        inputList.add("baz,table2_baz");

        file = fs.create(table2File);
        for(String inp : inputList) {
            file.writeBytes(inp + "\n");
        }
        file.close();

    }

    private void checkOutput(FileSystem fs, Path outputFile) throws IOException {
        BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(outputFile)));
        assertEquals("bar,table1_bar,table2_bar", bf.readLine());
        assertEquals("baz,table1_baz,table2_baz", bf.readLine());
        assertEquals("foo,table1_foo,table2_foo", bf.readLine());
        assertEquals("foo,table1_foo,table2_foo", bf.readLine());
        assertEquals("foo,table1_foo,table2_foo", bf.readLine());
        bf.close();
    }
}
