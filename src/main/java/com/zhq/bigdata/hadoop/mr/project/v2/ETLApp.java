package com.zhq.bigdata.hadoop.mr.project.v2;

import com.zhq.bigdata.hadoop.mr.project.utils.ContentUtils;
import com.zhq.bigdata.hadoop.mr.project.utils.LogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class ETLApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(ETLApp.class);
        job.setMapperClass(MyMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);


        Path inputPath = new Path("input/raw/");
        Path outputPath = new Path("input/etl/");

        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath,true);
        }

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        job.waitForCompletion(true);
    }

    static class MyMapper extends Mapper<LongWritable,Text, NullWritable,Text>{
       private LogParser logParser;

        @Override
        protected void setup(Context context) {
           logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> map = logParser.parse(value.toString());
            String ip = map.get("ip");
            String url = map.get("url");
            String sessionId = map.get("sessionId");
            String time = map.get("time");
            String country = map.getOrDefault("country", " - ");
            String province = map.getOrDefault("province", " - ");
            String city = map.getOrDefault("city", " - ");
            String pageId = ContentUtils.getPageId(url," - ");

            StringBuilder sb = new StringBuilder();
            sb.append(ip).append("\t");
            sb.append(country).append("\t");
            sb.append(province).append("\t");
            sb.append(city).append("\t");
            sb.append(url).append("\t");
            sb.append(time).append("\t");
            //sb.append(sessionId).append("\t");
            sb.append(pageId);


            context.write(NullWritable.get(),new Text(sb.toString()));
        }
    }

}
