package com.HadoopJava;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text lines, Context context) throws IOException, InterruptedException {
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		String[] fields = lines.toString().split(" ");
		for(int i=0; i<fields.length; i++) {
			//write <word,filename> as output of mapper
			context.write(new Text(fields[i]), new Text(fileName));
		}
	}
}
