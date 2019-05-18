package cn.cpc.mrquery.mr.join;

import cn.cpc.mrquery.writable.join.RJKeyWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RJGroupComparator extends WritableComparator {

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        RJKeyWritable r1 = (RJKeyWritable)a;
        RJKeyWritable r2 = (RJKeyWritable)b;
        int cmpRes = r1.getRelatedField().compareTo(r2.getRelatedField());
        return cmpRes;
    }

    public RJGroupComparator() {
        super(RJKeyWritable.class, true);
    }
}
