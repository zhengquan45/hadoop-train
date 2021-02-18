package com.zhq.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * KEYIN: Map任务读数据的key类型,offset每行数据起始位置的偏移量,Long
 * VALUEIN: Map任务读数据的value类型,其实就是一行行的字符串,String
 *
 * KEYOUT: map方法自定义实现输出的key类型
 * VALUEOUT: map方法自定义实现输出的value类型
 *
 * LongWritable Long
 * Text         String
 * IntWritable  Int
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //value对应行数按照制定分隔符分割
        String[] words = value.toString().split("\t");

        for (String word : words) {
            context.write(new Text(word),new IntWritable(1));
        }


    }
}
