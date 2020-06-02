package com.HadoopJava;

import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperMaxCloseStock extends Mapper<LongWritable,Text,Text,FloatWritable>{
	public enum value{
		HIGH_VOL
	}

	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		String[] fields = values.toString().split(",");
		String stockId = fields[1];
		Float closePrice = Float.parseFloat(fields[6]);
		int volume = Integer.parseInt(fields[7]);
		if (volume>50000) {
			context.getCounter(value.HIGH_VOL).increment(1);
		}
		context.write(new Text(stockId), new FloatWritable(closePrice));
	}
}
