package cn.cpc.mrquery.writable.join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class RJValWritable implements WritableComparable<RJValWritable> {
    private IntWritable sortedField;
    private Text val;

    public RJValWritable() {
        sortedField = new IntWritable();
        val = new Text();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        sortedField.write(out);
        val.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sortedField.readFields(in);
        val.readFields(in);
    }

    @Override
    public int compareTo(RJValWritable o) {
        int cmpRes = sortedField.compareTo(o.sortedField);
        if (cmpRes != 0) {
            return cmpRes;
        }
        cmpRes = val.compareTo(o.val);
        return cmpRes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sortedField, val);
    }

    public IntWritable getSortedField() {
        return sortedField;
    }

    public void setSortedField(IntWritable sortedField) {
        this.sortedField = sortedField;
    }

    public Text getVal() {
        return val;
    }

    public void setVal(Text val) {
        this.val = val;
    }
}
