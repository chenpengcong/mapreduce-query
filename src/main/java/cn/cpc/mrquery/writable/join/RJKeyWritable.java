package cn.cpc.mrquery.writable.join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class RJKeyWritable implements WritableComparable<RJKeyWritable> {

    private Text relatedField;
    private IntWritable sortedField;

    public RJKeyWritable() {
        relatedField = new Text();
        sortedField = new IntWritable();
    }

    @Override
    public int compareTo(RJKeyWritable o) {
        int cmpRes = relatedField.compareTo(o.relatedField);
        if (cmpRes != 0) {
            return cmpRes;
        }
        cmpRes = sortedField.compareTo(o.sortedField);
        return cmpRes;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        relatedField.write(out);
        sortedField.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        relatedField.readFields(in);
        sortedField.readFields(in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(relatedField, sortedField);
    }

    public Text getRelatedField() {
        return relatedField;
    }

    public void setRelatedField(Text relatedField) {
        this.relatedField = relatedField;
    }

    public IntWritable getSortedField() {
        return sortedField;
    }

    public void setSortedField(IntWritable sortedField) {
        this.sortedField = sortedField;
    }
}
