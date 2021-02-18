package com.zhq.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;

/**
 * 使用HDFS API完成 wordCount 统计
 *
 * 需求:统计HDFS上的文件wc，然后将统计结果输出到HDFS
 *
 * 功能拆解:
 * 1、读取HDFS上的文件
 * 2、业务处理(词频统计)
 * 3、结果缓存起来 Context
 * 4、结果输出到HDFS
 */
public class HDFSWcApp01 {
    public static final String HDFS_PATH = "hdfs://hadoop000:8020";
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        //1
        Path input = new Path("/hdfsapi/test/hello.txt");
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), conf, "hadoop");
        Context context = new Context();
        Mapper mapper = new WordCountMapper();
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(input, false);
        while(iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();
            System.out.println(fileStatus.getPath());
            FSDataInputStream inputStream = fileSystem.open(fileStatus.getPath());

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = bufferedReader.readLine())!=null){
                //2
                mapper.map(line,context);
            }

            bufferedReader.close();
            inputStream.close();
        }

        //3
        Map<Object,Object> contextMap = context.getCacheMap();
        //4
        Path output = new Path("/hdfsapi/output/");

        FSDataOutputStream out = fileSystem.create(new Path(output, new Path("wc.txt")));

        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();

        for (Map.Entry<Object, Object> entry : entries) {
            out.write((entry.getKey().toString()+":"+entry.getValue().toString()+"\n").getBytes());
        }

        out.close();
        fileSystem.close();

        System.out.println("HDFS API 词频统计成功");


    }
}
