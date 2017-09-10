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
import org.apache.hadoop.hbase.util.Bytes;


// args[0] = tableName
public class FareJob {
  public static void main(String[] args) throws Exception {
    String tableName = args[0];

    HBaseConfiguration conf = new HBaseConfiguration();
    //Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.table.name", tableName);

    Job job = new Job(conf, "NYCTaxi Fare Job");
    job.setJarByClass(FareMapper.class);

    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("f"));
    scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("m"));

    TableMapReduceUtil.initTableMapperJob(tableName,
                                          scan,
                                          FareMapper.class,
                                          Text.class,
                                          DoubleWritable.class,
                                          job);
    TableMapReduceUtil.initTableReducerJob(tableName,
                                           FareReducer.class,
                                           job);

    job.setNumReduceTasks(1);

    job.waitForCompletion(true);
  }
}
