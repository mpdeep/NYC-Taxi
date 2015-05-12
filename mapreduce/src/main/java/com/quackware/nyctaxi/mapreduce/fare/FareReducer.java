package com.quackware.nyctaxi.mapreduce.fare;

import java.io.IOException;
import java.lang.Iterable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class FareReducer
  extends TableReducer<Text, DoubleWritable, ImmutableBytesWritable> {

  final static byte[] AGG_COL_FAM = Bytes.toBytes("a");

  @Override
  protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
    throws IOException, InterruptedException {
    double sum = 0.0;
    for (DoubleWritable val : values) {
      sum += val.get();
    }

    System.out.println("Sum for medallion: " + key.toString() + " is " + sum);
    Put put = new Put(Bytes.toBytes(key.toString()));
    put.add(AGG_COL_FAM, Bytes.toBytes("sum"), Bytes.toBytes(sum));
    context.write(null, put);
  }
}