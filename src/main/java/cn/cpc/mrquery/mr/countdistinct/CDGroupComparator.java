package cn.cpc.mrquery.mr.countdistinct;

import cn.cpc.mrquery.writable.countdistinct.CDKeyWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CDGroupComparator extends WritableComparator {
    public CDGroupComparator() {
        super(CDKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CDKeyWritable c1 = (CDKeyWritable)a;
        CDKeyWritable c2 = (CDKeyWritable)b;
        int cmpRes = c1.getGroupField().compareTo(c2.getGroupField());
        return cmpRes;
    }
}
