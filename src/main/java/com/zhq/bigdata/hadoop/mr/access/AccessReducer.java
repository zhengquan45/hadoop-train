package com.zhq.bigdata.hadoop.mr.access;


import com.zhq.bigdata.hadoop.hdfs.Context;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AccessReducer extends Reducer<Text,Access, NullWritable,Access> {
    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
        long ups = 0L;
        long downs = 0L;
        for (Access value : values) {
            ups+=value.getUp();
            downs +=value.getDown();
        }
        context.write(NullWritable.get(),new Access(key.toString(),ups,downs));
    }
}
