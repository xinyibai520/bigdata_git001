package com.baiyuxin.bigdata.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * @author PK哥
 **/
public class CompressionDriver {

    public static void main(String[] args) throws Exception {

//        compress("data/inputformat/test.txt","org.apache.hadoop.io.compress.BZip2Codec");

        decompression("data/inputformat/test.txt.bz2");
    }

    /**
     * 解压缩
     */
    public static void decompression(String filename) throws Exception {
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filename));
        if(null == codec) {
            System.out.println("找不到codec：" + codec.getDefaultExtension());
            return;
        }

        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(filename)));
        FileOutputStream fos = new FileOutputStream(new File(filename + ".ruozedata-decoded"));
        IOUtils.copyBytes(cis, fos,1024*1024*5);

        fos.close();
        cis.close();
    }

    /**
     * 压缩
     */
    public static void compress(String filename, String method) throws Exception {

        FileInputStream fis = new FileInputStream(new File(filename));
        Class<?> codecClass = Class.forName(method);

        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, new Configuration());

        FileOutputStream fos = new FileOutputStream(new File(filename + codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fos);
        IOUtils.copyBytes(fis, cos,1024*1024*5);

        cos.close();
        fos.close();
        fis.close();
    }
}
