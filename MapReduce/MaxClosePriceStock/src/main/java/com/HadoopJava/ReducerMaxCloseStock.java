package com.HadoopJava;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class ReducerMaxCloseStock extends Reducer<Text, FloatWritable, Text, FloatWritable>{
	
	public void reduce(Text key, Iterable<FloatWritable> listOfPrices, Context context) throws IOException, InterruptedException {
		Float maxPrice = Float.MIN_VALUE;
		for(FloatWritable item: listOfPrices) {
			maxPrice = Math.max(maxPrice, item.get());
		}
		context.write(key, new FloatWritable(maxPrice));
	}
}
