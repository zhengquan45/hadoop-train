package com.zhq.bigdata.hadoop.mr.project.v1;

import com.zhq.bigdata.hadoop.mr.project.utils.ContentUtils;
import com.zhq.bigdata.hadoop.mr.project.utils.LogParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class PageStatApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(PageStatApp.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);


        Path inputPath = new Path("input/raw/");
        Path outputPath = new Path("output/v1/pageStat");

        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath,true);
        }

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable,Text, Text,LongWritable>{
       private final LongWritable ONE = new LongWritable(1);
       private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
           logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> map = logParser.parse(value.toString());

            String url = map.get("url");
            if (StringUtils.isNotBlank(url)) {
                String pageId = ContentUtils.getPageId(url);
                if(StringUtils.isNotBlank(pageId)) {
                    context.write(new Text(pageId), ONE);
                }
            }
        }
    }

    static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value : values) {
                count++;
            }
            context.write(key,new LongWritable(count));
        }
    }
}
