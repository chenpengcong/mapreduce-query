package cn.cpc.mrquery.mr.multicountdistinct;

import cn.cpc.mrquery.writable.multicountdistinct.MCDKeyWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MCDGroupComparator extends WritableComparator {
    public MCDGroupComparator() {
        super(MCDKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        MCDKeyWritable m1 = (MCDKeyWritable)a;
        MCDKeyWritable m2 = (MCDKeyWritable)b;
        int cmpRes = m1.getGroupField().compareTo(m2.getGroupField());
        return cmpRes;
    }
}
