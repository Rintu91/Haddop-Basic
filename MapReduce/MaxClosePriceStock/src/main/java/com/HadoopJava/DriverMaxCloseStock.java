package com.HadoopJava;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class DriverMaxCloseStock {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		//configure mapreduce job
		Job job = new Job();
		job.setJarByClass(DriverMaxCloseStock.class);
		job.setJobName("Find Max Stock Price");
		
		//set input and output format
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//set input and output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//mapper and reducer class
		job.setMapperClass(MapperMaxCloseStock.class);
		job.setReducerClass(ReducerMaxCloseStock.class);
		//use small reducer to reduce within each mapper first
		job.setCombinerClass(ReducerMaxCloseStock.class);
		
		//set output format of mapper
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		System.exit(job.waitForCompletion(true)?1:0);
	}

}
