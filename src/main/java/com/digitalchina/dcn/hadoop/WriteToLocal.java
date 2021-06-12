package com.digitalchina.dcn.hadoop;

/**
 * Created by Administrator on 2016/9/11 0011.
 */
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;


public class WriteToLocal {

    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        LocalFileSystem fs = (LocalFileSystem) FileSystem.get(conf);
//        fs.initialize(new URI("file:/f:/"), conf); // put the conf object to filesystem instance
        OutputStream out = fs.create(new Path("file:/f:/home/peter/abc1"));
        for(int i = 0; i < 512*10;i++){
            out.write(97);
        }
        out.close();
        Path file = fs.getChecksumFile(new Path("file:/e:/home/peter/abc1"));
        System.out.println(file.getName());
        fs.close();
    }

}
