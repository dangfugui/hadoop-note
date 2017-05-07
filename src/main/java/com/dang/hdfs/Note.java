package com.dang.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by dang on 17-5-7.
 */
public class Note {

    public static void main(String []strs) throws Exception {
//        Configuration conf=new Configuration();
//
//        //也可以在代码中对conf中的配置信息进行手动设置，会覆盖掉配置文件中的读取的值
//        //设置文件系统为hdfs。如果设置这个参数，那么下面的方法里的路径就不用写hdfs://hello110:9000/
//        //conf.set("fs.defaultFS", "hdfs://hello110:9000/");
//
//        //根据配置信息，去获取一个具体文件系统的客户端操作实例对象
//        FileSystem.get(new URI("hdfs://localhost:9000/"), conf,"hadoop");//hdfs://localhost:9000
//
//        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
//        while(iterator.hasNext()){
//            Map.Entry<String, String> ent=iterator.next();
//            System.out.println(ent.getKey()+"  :  "+ent.getValue());
//        }
//
//        System.out.println("-----------------/hdfs-site.xml 文件读取结束----------------");
        String HDFS_PATH = "hdfs://localhost:9000/user/hadoop/hello";
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        final URL url = new URL(HDFS_PATH);
        IOUtils.copyBytes(url.openStream(),System.out,1024,true);
//        //fileSystem();


    }

    private static void fileSystem() throws Exception {
        String HDFS_PATH = "hdfs://localhost:9000";
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration());

    }
}
