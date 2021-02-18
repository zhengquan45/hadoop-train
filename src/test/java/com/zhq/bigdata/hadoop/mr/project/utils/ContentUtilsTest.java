package com.zhq.bigdata.hadoop.mr.project.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ContentUtilsTest {


    @Test
    void getPageId() {
        String url = "http://www.yihaodian.com/cms/view.do?topicId=19004";
        String pageId = ContentUtils.getPageId(url);
        System.out.println(pageId);
    }
}