package cn.cpc.mrquery.writable.countdistinct;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CDKeyWritable implements WritableComparable<CDKeyWritable> {
    private Text groupField;
    private Text distinctField;

    public CDKeyWritable() {
        groupField = new Text();
        distinctField = new Text();
    }

    @Override
    public int compareTo(CDKeyWritable o) {
        int cmpRes = groupField.compareTo(o.groupField);
        if (cmpRes != 0) {
            return cmpRes;
        }
        cmpRes = distinctField.compareTo(o.distinctField);
        return cmpRes;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        groupField.write(out);
        distinctField.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        groupField.readFields(in);
        distinctField.readFields(in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupField, distinctField);
    }

    public Text getGroupField() {
        return groupField;
    }

    public void setGroupField(Text groupField) {
        this.groupField = groupField;
    }

    public Text getDistinctField() {
        return distinctField;
    }

    public void setDistinctField(Text distinctField) {
        this.distinctField = distinctField;
    }
}
