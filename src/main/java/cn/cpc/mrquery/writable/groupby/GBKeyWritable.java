package cn.cpc.mrquery.writable.groupby;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class GBKeyWritable implements WritableComparable<GBKeyWritable> {

    private Text group1;
    private Text group2;

    public GBKeyWritable() {
        group1 = new Text();
        group2 = new Text();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        group1.write(out);
        group2.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        group1.readFields(in);
        group2.readFields(in);
    }

    public Text getGroup1() {
        return group1;
    }

    public void setGroup1(Text group1) {
        this.group1 = group1;
    }

    public Text getGroup2() {
        return group2;
    }

    public void setGroup2(Text group2) {
        this.group2 = group2;
    }

    @Override
    public int compareTo(GBKeyWritable o) {
        int cmpRes = group1.compareTo(o.group1);
        if (cmpRes != 0) {
            return cmpRes;
        }
        cmpRes = group2.compareTo(o.group2);
        return cmpRes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(group1, group2);
    }
}
