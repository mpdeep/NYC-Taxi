package com.quackware.nyctaxi.mapreduce.fare;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class FareMapper
  extends TableMapper<Text, DoubleWritable> {

  private Text text = new Text();

  @Override
  protected void map(ImmutableBytesWritable key, Result value, Context context)
    throws IOException, InterruptedException {
      byte[] medallionBytes = value.getValue(Bytes.toBytes("d"), Bytes.toBytes("m"));
      byte[] fareBytes = value.getValue(Bytes.toBytes("d"), Bytes.toBytes("f"));

      String medallion = Bytes.toString(medallionBytes);
      String fareString = Bytes.toDouble(fareBytes);
      double fare = Double.parseDouble(fareString);

      text.set(medallion);
      context.write(text, new DoubleWritable(fare));
    }
}