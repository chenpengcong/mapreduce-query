package cn.cpc.mrquery.mr.join;

import cn.cpc.mrquery.writable.join.RJKeyWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Objects;

public class RJPartitioner extends Partitioner<RJKeyWritable, Writable> {
    @Override
    public int getPartition(RJKeyWritable rjKeyWritable, Writable writable, int i) {
        return Math.abs(Objects.hash(rjKeyWritable.getRelatedField()) * 127) % i;
    }
}
