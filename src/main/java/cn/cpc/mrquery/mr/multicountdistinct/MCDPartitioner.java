package cn.cpc.mrquery.mr.multicountdistinct;

import cn.cpc.mrquery.writable.multicountdistinct.MCDKeyWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Objects;

public class MCDPartitioner extends Partitioner<MCDKeyWritable, Writable> {
    @Override
    public int getPartition(MCDKeyWritable mcdKeyWritable, Writable writable, int i) {
        return Math.abs(Objects.hash(mcdKeyWritable.getGroupField()) % i);
    }
}
