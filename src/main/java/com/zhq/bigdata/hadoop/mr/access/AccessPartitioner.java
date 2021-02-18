package com.zhq.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AccessPartitioner extends Partitioner<Text,Access> {
    @Override
    public int getPartition(Text text, Access access, int numPartitions) {
        if(text.toString().startsWith("13")){
            return 0;
        }else if(text.toString().startsWith("15")){
            return 1;
        }
        return 2;
    }
}
