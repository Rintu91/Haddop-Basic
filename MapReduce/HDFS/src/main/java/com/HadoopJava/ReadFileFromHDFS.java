package com.HadoopJava;
/*
 * provide file path on args[0]
 */
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


public class ReadFileFromHDFS {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String src = args[0];
		
		Configuration conf = new Configuration();		
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(src), conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		FSDataInputStream in = null;
		try {
			in = fs.open(new Path(src));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			IOUtils.copyBytes(in, System.out, 4096, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("file: "+src+" full output shown");
		
	}

}
