package com.HadoopJava;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> files, Context context) throws IOException, InterruptedException {
		//get filename and return a unique list
		Set<String> hashSet = new HashSet<String>();
		for(Text file: files) {
			hashSet.add(file.toString());			
		}
		String fList = "";
		for(String elem: hashSet) {
			fList += elem;
		}
		context.write(key, new Text(fList));
	}
}
