package com.zhq.bigdata.hadoop.hdfs;

public class WordCountMapper implements Mapper{
    @Override
    public void map(String line, Context context) {
        String[] words = line.split("\t");
        for (String word : words) {
            Object v = context.get(word);
            if (v == null) {
                context.write(word, 1);
            } else {
                context.write(word, Integer.parseInt(v.toString()) + 1);
            }
        }
    }
}
