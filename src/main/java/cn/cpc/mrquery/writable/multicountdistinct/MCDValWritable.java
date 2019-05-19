package cn.cpc.mrquery.writable.multicountdistinct;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MCDValWritable implements WritableComparable<MCDValWritable> {
    private IntWritable serialNum;
    private Text distinctField;

    public MCDValWritable() {
        serialNum = new IntWritable();
        distinctField = new Text();
    }

    @Override
    public int compareTo(MCDValWritable o) {
        int cmpRes = serialNum.compareTo(o.serialNum);
        if (cmpRes != 0) {
            return cmpRes;
        }
        cmpRes = distinctField.compareTo(o.distinctField);
        return cmpRes;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        serialNum.write(out);
        distinctField.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        serialNum.readFields(in);
        distinctField.readFields(in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serialNum, distinctField);
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
