package com.dang.hdfs;

import java.io.FileInputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsUtil {

    FileSystem fs=null;

    //@Before 是junit 测试框架
    @Before
    public void init() throws Exception{

            /*
             *
             * 注意注意注意
             * windows上eclipse上运行，hadoop的bin文件下windows环境编译的文件和hadoop版本关系紧密
             * 如果报错，或者api无效，很可能是bin下hadoop文件问题
             *
             */

        //读取classpath下的core/hdfs-site.xml 配置文件，并解析其内容，封装到conf对象中
        //如果没有会读取hadoop里的配置文件
        Configuration conf=new Configuration();

        //也可以在代码中对conf中的配置信息进行手动设置，会覆盖掉配置文件中的读取的值
        //设置文件系统为hdfs。如果设置这个参数，那么下面的方法里的路径就不用写hdfs://hello110:9000/
        //conf.set("fs.defaultFS", "hdfs://hello110:9000/");

        //根据配置信息，去获取一个具体文件系统的客户端操作实例对象
        FileSystem.get(new URI("hdfs://172.0.0.1:9000/"), conf,"hadoop");

        Iterator<Entry<String, String>> iterator = conf.iterator();
        while(iterator.hasNext()){
            Entry<String, String> ent=iterator.next();
            System.out.println(ent.getKey()+"  :  "+ent.getValue());
        }

        System.out.println("-----------------/hdfs-site.xml 文件读取结束----------------");
    }

    @Test
    public void listFiles() throws Exception{

        //RemoteIterator 远程迭代器
        RemoteIterator<LocatedFileStatus> listFiles = null;//fs.listFiles(new Path("/"), true);

        while(listFiles.hasNext()){
            LocatedFileStatus file = listFiles.next();
            Path path = file.getPath();
            //String fileName=path.getName();
            System.out.println(path.toString());
            System.out.println("权限："+file.getPermission());
            System.out.println("组："+file.getGroup());
            System.out.println("文件大小："+file.getBlockSize());
            System.out.println("所属者："+file.getOwner());
            System.out.println("副本数："+file.getReplication());

            BlockLocation[] blockLocations = file.getBlockLocations();
            for(BlockLocation bl:blockLocations){
                System.out.println("块起始位置："+bl.getOffset());
                System.out.println("块长度："+bl.getLength());
                String[] hosts = bl.getHosts();
                for(String h:hosts){
                    System.out.println("块所在DataNode："+h);
                }

            }

            System.out.println("*****************************************");

        }

        System.out.println("-----------------华丽的分割线----------------");

        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for(FileStatus status:listStatus){
            String name=status.getPath().getName();
            //System.out.println(name+(status.isDirectory()?" is Dir":" is File"));
        }
    }

    //比较低层的写法
    @Test
    public void upload() throws Exception{

        System.out.println("-----------------upload----------------");

        Path path = new Path("/testdata/testFs.txt");
        FSDataOutputStream os = fs.create(path);

        FileInputStream is = new FileInputStream("d:/testFS.txt");

        IOUtils.copy(is, os);
    }

    //封装好的方法
    @Test
    public void upload2() throws Exception{

        System.out.println("-----------------upload2----------------");
        //hdfs dfs -copyFromLocal 从本地拷贝命令
        fs.copyFromLocalFile(new Path("d:/testFS-2.txt"), new Path("/testdata/testFs6.txt"));
        //如果windows-hadoop环境不行，可以用下面的api，最后的true是：用java的io去写。
        //fs.copyToLocalFile(false, new Path("d:/testFS-2.txt"), new Path("/testdata/testFs6.txt"), true);
        fs.close();

    }

    @Test
    public void download() throws Exception{

        //hdfs dfs -copyToLocal 拷贝到本地命令
        fs.copyToLocalFile(new Path("/testdata/testFs6.txt") , new Path("d:/testFS3.txt"));
        //如果windows-hadoop环境不行，可以用下面的api，最后的true是：用java的io去写。
        //fs.copyToLocalFile(false, new Path("/testdata/testFs6.txt"), new Path("d:/testFS3.txt"), true);

    }

    @Test
    public void mkdir() throws Exception{

        System.out.println("-----------------mkdir----------------");

        fs.mkdirs(new Path("/testdata/a123"));
        fs.mkdirs(new Path("/testdata/a124"));
    }

    @Test
    public void rm() throws Exception{
        //true：如果是目录也会删除。false，遇到目录则报错
        fs.delete(new Path("hdfs://hello110:9000/testdata/a124"), true);
    }

}

