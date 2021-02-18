package com.zhq.bigdata.hadoop.hdfs;

import com.zhq.bigdata.hadoop.mr.project.utils.IPParser;
import org.junit.jupiter.api.Test;

public class IPTest {

    @Test
    public void testIP(){
        IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp("8.134.48.107");
        System.out.println(regionInfo);
    }
}
