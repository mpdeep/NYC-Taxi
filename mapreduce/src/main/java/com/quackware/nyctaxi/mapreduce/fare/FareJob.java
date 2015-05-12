package com.quackware.nyctaxi.mapreduce.fare;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

// args[0] = tableName
public class FareJob {
  public static void main(String[] args) throws Exception {

    String tableName = args[0];

    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.table.name", tableName);

    Job job = new Job(conf, "NYCTaxi Fare Job");
    job.setJarByClass(FareMapper.class);

    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);

    TableMapReduceUtil.initTableMapperJob(tableName,
                                          scan,
                                          FareMapper.class, // mapper class
                                          Text.class, // mapper output key
                                          DoubleWritable.class, // mapper output value
                                          job);
    TableMapReduceUtil.initTableReducerJob(tableName,
                                           FareReducer.class,
                                           job);
    job.setNumReduceTasks(0);

    job.waitForCompletion(true);
  }
}