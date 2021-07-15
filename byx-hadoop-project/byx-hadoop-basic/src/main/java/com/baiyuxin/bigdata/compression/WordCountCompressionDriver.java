package com.baiyuxin.bigdata.compression;

import com.baiyuxin.bigdata.utils.FileUtils;
import com.baiyuxin.bigdata.wc.WordCountMapper;
import com.baiyuxin.bigdata.wc.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 八股文编程
 **/
public class WordCountCompressionDriver {

    public static void main(String[] args) throws Exception {
        String input = "data/inputformat/test.txt.bz2";
        String output = "out";

        // 1 获取Job
        Configuration configuration = new Configuration();
        configuration.setBoolean("mapreduce.map.output.compress", true); // 开启map输出使用压缩
        configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        configuration.setBoolean("mapreduce.output.fileoutputformat.compress", true);
        configuration.setClass("mapreduce.output.fileoutputformat.compress.codec", BZip2Codec.class, CompressionCodec.class);

        Job job = Job.getInstance(configuration);

        FileUtils.deleteOutput(configuration, output);

        // 2 设置主类
        job.setJarByClass(WordCountCompressionDriver.class);

        // 3 设置Mapper和Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setCombinerClass(WordCountReducer.class);

        // 4 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置Reduce阶段输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        TextInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7 提交Job
        boolean result = job.waitForCompletion(true);
        System.exit(result? 0:1 );
    }
}
