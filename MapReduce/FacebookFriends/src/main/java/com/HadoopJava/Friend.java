package com.HadoopJava;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable; 
public class Friend implements WritableComparable{
	private IntWritable id;
	private Text name;
	private Text hometown;
	
	public Friend() {
		this.id = new IntWritable();
		this.name = new Text();
		this.hometown = new Text();
	}
	public Friend(IntWritable id, Text name, Text hometown) {
		this.id = id;
		this.name = name;
		this.hometown = hometown;
	}
	public Friend(int id, String name, String hometown) {
		this.id = new IntWritable(id);
		this.name = new Text(name);
		this.hometown = new Text(hometown);
	}
	
	public IntWritable getId() {
		return id;
	}

	public void setId(IntWritable id) {
		this.id = id;
	}

	public Text getName() {
		return name;
	}

	public void setName(Text name) {
		this.name = name;
	}

	public Text getHometown() {
		return hometown;
	}

	public void setHometown(Text hometown) {
		this.hometown = hometown;
	}	
	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		id.write(out);
		name.write(out);
		hometown.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id.readFields(in);
		name.readFields(in);
		hometown.readFields(in);
	}

	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		Friend other = (Friend)o;		
		return getId().compareTo(other.getId());
	}

	
	@Override
	public boolean equals(Object obj) {
		Friend other = (Friend)obj;
		return (getId().equals(other.getId()));
	}
	
	@Override
	public String toString() {
		return "[id:"+getId()+","+"name:"+getName()+"]";
	}
	
}
