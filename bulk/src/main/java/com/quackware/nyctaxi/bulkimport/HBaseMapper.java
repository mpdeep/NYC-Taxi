package com.quackware.nyctaxi.bulkimport;

import java.io.IOException;
import java.util.Locale;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HBaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

  SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-DD hh:mm:ss");

  ImmutableBytesWritable hKey = new ImmutableBytesWritable();
  KeyValue kv;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {

  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    if (value.find("medallion") > -1) {
      return;
    }

    String[] fields = null;
    try {
      fields = value.toString().split(",");
    }
    catch(Exception ex) {
      System.out.println(ex.getMessage());
      return;
    }

    if (fields.length == 14) {
      // trip data
      String medallion = fields[0];
    }
    else if (fields.length == 11) {
      // fare data
      String medallion = fields[0];
      String pickupString = fields[3];
      String fareAmount = fields[5];
      Date pickupDate = null;
      try {
        pickupDate = dateFormat.parse(pickupString);
      }
      catch (Exception ex) {
        System.out.println(ex.getMessage());
        return;
      }

      long timestamp = pickupDate.getTime();

      hKey.set(Bytes.toBytes(String.format("%s:%d", medallion, timestamp)));

      kv = new KeyValue(hKey.get(),
                        Bytes.toBytes("d"),
                        Bytes.toBytes("m"),
                        timestamp,
                        Bytes.toBytes(medallion));
      context.write(hKey, kv);

      kv = new KeyValue(hKey.get(),
                        Bytes.toBytes("d"),
                        Bytes.toBytes("f"),
                        timestamp,
                        Bytes.toBytes(fareAmount));
      context.write(hKey, kv);

      kv = new KeyValue(hKey.get(),
                        Bytes.toBytes("d"),
                        Bytes.toBytes("p"),
                        timestamp,
                        Bytes.toBytes(timestamp));
      context.write(hKey, kv);

    }
  }

}