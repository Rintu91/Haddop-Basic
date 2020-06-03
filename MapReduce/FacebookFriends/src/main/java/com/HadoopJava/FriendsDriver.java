package com.HadoopJava;
import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;

public class FriendsDriver extends Configured implements Tool {
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//create instance of job
		Job conf = Job.getInstance(getConf(), "Common Friends");
		conf.setJarByClass(FriendsDriver.class);
		//conf.setJobName("Common Friends");
		
		//create instance for input and output
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		//create instance for input and output format
		conf.setInputFormatClass(TextInputFormat.class);
		conf.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		//create instance for mapper and reducer
		conf.setMapperClass(FriendsMapper.class);
		conf.setReducerClass(FriendsReducer.class);
		
		//set mapper output format
		conf.setMapOutputKeyClass(FriendPair.class);
		conf.setMapOutputValueClass(FriendArray.class);
		
		//create instance for key and value class
		conf.setOutputKeyClass(FriendPair.class);
		conf.setOutputValueClass(FriendArray.class);
		
		
		
		//start job
		int code = conf.waitForCompletion(true)?1:0;
		return code;
	}
	public static void main(String[] args) {
		try {
			int code = ToolRunner.run(new FriendsDriver(), args);
			System.exit(code);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
