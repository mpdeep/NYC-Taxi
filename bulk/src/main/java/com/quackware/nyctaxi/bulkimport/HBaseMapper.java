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

public class HBaseMapper extends
  Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

  private String tableName = "";

  final static byte[] COL_FAM = Bytes.toBytes("d");
  final static byte[] MEDALLION_COL = Bytes.toBytes("m");
  final static byte[] FARE_COL = Bytes.toBytes("f");
  final static byte[] PICKUP_COL = Bytes.toBytes("p");

  ImmutableBytesWritable hKey = new ImmutableBytesWritable();
  KeyValue kv;

  SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-DD hh:mm:ss");

  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {

    Configuration c = context.getConfiguration();

    tableName = c.get("hbase.table.name");
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
    if (value.find("medallion") > -1) {
      // skip header
      return;
    }

    String[] fields = null;
    try {
      fields = value.toString().split(",");
    }
    catch (Exception ex) {
      System.out.println("Error parsing fields:" + ex.getMessage());
      return;
    }

    if (fields.length == 14) {
      // Trip data
    }
    else if (fields.length == 11) {
      // Fare data
      String medallion = fields[0];
      String pickupString = fields[3];
      String fareAmount = fields[5];
      Date pickupDate = null;
      try {
        pickupDate = dateFormat.parse(pickupString);
      }
      catch (Exception ex) {
        System.out.println("Error parsing date: " + ex.getMessage());
        return;
      }
      Long timestamp = pickupDate.getTime();

      hKey.set(Bytes.toBytes(String.format("%s:%d", medallion, timestamp)));

      // Medallion
      kv = new KeyValue(hKey.get(),
                        COL_FAM,
                        MEDALLION_COL,
                        timestamp,
                        Bytes.toBytes(medallion));
      context.write(hKey, kv);

      // Fare
      kv = new KeyValue(hKey.get(),
                        COL_FAM,
                        FARE_COL,
                        timestamp,
                        Bytes.toBytes(fareAmount));
      context.write(hKey, kv);

      // Pickup
      kv = new KeyValue(hKey.get(),
                        COL_FAM,
                        PICKUP_COL,
                        timestamp,
                        Bytes.toBytes(timestamp));
      context.write(hKey, kv);
    }
  }
}