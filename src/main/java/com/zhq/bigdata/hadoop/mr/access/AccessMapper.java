package com.zhq.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AccessMapper extends Mapper<LongWritable,Text, Text,Access> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        String phone = words[1];
        long up = Long.parseLong(words[words.length - 3]);
        long down = Long.parseLong(words[words.length - 2]);
        context.write(new Text(phone),new Access(phone,up,down));
    }
}
