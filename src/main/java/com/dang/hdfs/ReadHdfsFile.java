package com.dang.hdfs;


import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * 读出文件控制台输出
 * @author hadoop
 *
 */
public class ReadHdfsFile {

    public static final String HDFS_PATH = "hdfs://localhost:9000/user/hadoop/input/capacity-scheduler.xml";
    //这里的路由要正确，对应着上面的截图

    public static void main(String[] args) throws Exception{

        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        final URL url = new URL(HDFS_PATH);
        final InputStream in = url.openStream();

        /**
         * in 输入流
         * out输出流
         * 1024 buffersize 缓存区大小自定义大小
         * close 是否关闭流
         */
        IOUtils.copyBytes(in, System.out, 1024, true);

    }
}