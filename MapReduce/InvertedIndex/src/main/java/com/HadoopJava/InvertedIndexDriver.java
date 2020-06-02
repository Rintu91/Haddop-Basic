package com.HadoopJava;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;



public class InvertedIndexDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Job conf = new Job();
		conf.setJarByClass(InvertedIndexDriver.class);
		conf.setJobName("Inverted Index");
		
		//set input and output path
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		//set mapper and rreducer
		conf.setMapperClass(InvertedIndexMapper.class);
		conf.setReducerClass(InvertedIndexReducer.class);
		
		//set input and output format
		conf.setInputFormatClass(TextInputFormat.class);
		conf.setOutputFormatClass(TextOutputFormat.class);
		
		//set key and value format
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		//start job
		System.exit(conf.waitForCompletion(true)?1:0);
	}
}
