package com.HadoopJava;
/*
 * copy regular files to HDFS
 */
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
public class WriteFileToHdfs {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//input path
		String src = args[0];
		String dst = args[1];
		
		InputStream in = null;
		
		try {
			in = new BufferedInputStream(new FileInputStream(src));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(dst), conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		OutputStream out = null;
		try {
			//create output stream
			out = fs.create(new Path(dst));
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			//copy the data to dst
			IOUtils.copyBytes(in, out, 4096, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(src+ " file copied to HDFS path "+dst);
	}

}
