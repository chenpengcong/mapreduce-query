package cn.cpc.mrquery.writable.multicountdistinct;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MCDKeyWritable implements WritableComparable<MCDKeyWritable> {

    private Text groupField;
    private IntWritable serialNum;
    private Text distinctField;


    public MCDKeyWritable() {
        groupField = new Text();
        serialNum = new IntWritable();
        distinctField = new Text();
    }

    @Override
    public int compareTo(MCDKeyWritable o) {
        int cmpRes = groupField.compareTo(o.groupField);
        if (cmpRes != 0) {
            return cmpRes;
        }
        cmpRes = serialNum.compareTo(o.serialNum);
        if (cmpRes != 0) {
            return cmpRes;
        }
        cmpRes = distinctField.compareTo(o.distinctField);
        return cmpRes;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        groupField.write(out);
        serialNum.write(out);
        distinctField.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        groupField.readFields(in);
        serialNum.readFields(in);
        distinctField.readFields(in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupField, serialNum, distinctField);
    }

    public Text getGroupField() {
        return groupField;
    }

    public void setGroupField(Text groupField) {
        this.groupField = groupField;
    }

    public IntWritable getSerialNum() {
        return serialNum;
    }

    public void setSerialNum(IntWritable serialNum) {
        this.serialNum = serialNum;
    }

    public Text getDistinctField() {
        return distinctField;
    }

    public void setDistinctField(Text distinctField) {
        this.distinctField = distinctField;
    }
}
