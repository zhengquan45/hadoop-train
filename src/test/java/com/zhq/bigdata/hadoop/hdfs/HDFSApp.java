package com.zhq.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;


public class HDFSApp {
    public static final String HDFS_PATH = "hdfs://hadoop000:8020";
    public static Configuration conf = null;
    public static FileSystem fileSystem = null;

    @BeforeAll
    public static void setUp() throws URISyntaxException, IOException, InterruptedException {
        System.out.println("===================setUp===================");
        conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        fileSystem = FileSystem.get(new URI(HDFS_PATH), conf, "hadoop");
    }


    @Test
    public void mkdir() throws IOException {
        //创建一个目录
        boolean flag = fileSystem.mkdirs(new Path("/hdfsapi/test"));
        System.out.println(flag);
    }

    @Test
    public void text() throws IOException {
        //读取数据
        FSDataInputStream in = fileSystem.open(new Path("/NOTICE.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
    }

    @Test
    public void create() throws IOException {
        //写数据
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/b.txt"));
        out.writeUTF("hello pk: replication 1");
        out.flush();
        out.close();
    }

    @Test
    public void rename() throws IOException {
        Path oldPath = new Path("/hdfsapi/test/copy.txt");
        Path newPath = new Path("/hdfsapi/test/hello.txt");
        boolean flag = fileSystem.rename(oldPath, newPath);
        System.out.println(flag);
    }

    @Test
    public void copyFromLocalFile() throws IOException {
        Path srcPath = new Path("/Users/zhengquan/Data/hello.txt");
        Path distPath = new Path("/hdfsapi/test/copy.txt");
        fileSystem.copyFromLocalFile(srcPath, distPath);
    }

    @Test
    public void createWithProgress() throws IOException {
        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(new File("/Users/zhengquan/Data/apache-hive-2.3.8-bin.tar.gz")));
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/test/hive.tgz"),
                new Progressable() {
                    @Override
                    public void progress() {
                        System.out.print(".");
                    }
                });
        IOUtils.copyBytes(inputStream, outputStream, 4396);
    }

    @Test
    public void copyToLocalFile() throws IOException {
        Path srcPath = new Path("/hdfsapi/test/copy.txt");
        Path distPath = new Path("/Users/zhengquan/Data");
        fileSystem.copyToLocalFile(srcPath, distPath);
    }

    @Test
    public void listFiles() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsapi/test"));
        for (FileStatus fileStatus : fileStatuses) {
            String dirStr = fileStatus.isDirectory() ? "文件夹" : "文件";
            String permission = fileStatus.getPermission().toString();
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println("dirStr:" + dirStr + "permission:" + permission + "replication:" + replication + "len:" + len + "path:" + path);
        }
    }


    @Test
    public void listFilesRecursive() throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/hdfsapi/test"), true);
        while(iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();
            String dirStr = fileStatus.isDirectory() ? "文件夹" : "文件";
            String permission = fileStatus.getPermission().toString();
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println("dirStr:" + dirStr + "permission:" + permission + "replication:" + replication + "len:" + len + "path:" + path);
        }
    }

    @Test
    public void getFileBlockInfo() throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/test/hive.tgz"));
        BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation blockLocation : blockLocations) {
            for (String name : blockLocation.getNames()) {
                System.out.println(name + ":" + blockLocation.getOffset() + ":" + blockLocation.getLength());
            }
        }
    }

    @Test
    public void delete() throws IOException {
        boolean flag = fileSystem.delete(new Path("/hdfsapi/test/hive.tgz"), true);
        System.out.println(flag);
    }

    @AfterAll
    public static void tearDown() {
        System.out.println("===================tearDown===================");
        conf = null;
        fileSystem = null;
    }
}
