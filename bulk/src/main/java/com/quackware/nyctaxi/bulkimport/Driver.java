package com.quackware.nyctaxi.bulkimport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// args[0] = inputPath
// args[1] = outputPath
// args[2] = tableName
public class Driver {
  public static void main(String[] args) throws Exception {

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    String tableName = args[2];

    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.table.name", tableName);

    Job job = new Job(conf, "NYCTaxi bulk import");
    job.setJarByClass(HBaseMapper.class);

    job.setMapperClass(HBaseMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(KeyValue.class);

    job.setInputFormatClass(TextInputFormat.class);

    HTable table = new HTable(conf, tableName);

    HFileOutputFormat.configureIncrementalLoad(job, table);

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);
  }
}