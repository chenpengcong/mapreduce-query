package cn.cpc.mrquery.mr.countdistinct;

import cn.cpc.mrquery.writable.countdistinct.CDKeyWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Objects;

public class CDPartitioner extends Partitioner<CDKeyWritable, Writable> {
    @Override
    public int getPartition(CDKeyWritable cdKeyWritable, Writable writable, int i) {
        return Math.abs(Objects.hash(cdKeyWritable.getGroupField()) * 127) % i;
    }
}
