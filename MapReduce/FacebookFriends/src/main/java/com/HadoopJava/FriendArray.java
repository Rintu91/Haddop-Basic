package com.HadoopJava;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
public class FriendArray extends ArrayWritable{
	public FriendArray() {
		super(Friend.class);
	}
	public FriendArray(Class<? extends Writable> valueClass) {
		super(valueClass);
		// TODO Auto-generated constructor stub
	}
	public FriendArray(Class<? extends Writable> valueClass, Writable[] values) {
		super(valueClass, values);
		// TODO Auto-generated constructor stub
	}
	
	public String toString() {
		Friend[] frndArr = Arrays.copyOf(get(), get().length, Friend[].class);
		String frnd = "";
		for(Friend f: frndArr) {
			frnd += f;
		}
		return frnd;		
	}
	public boolean equals(Object obj) {
		FriendArray other = (FriendArray)obj;
		Friend[] f1 = Arrays.copyOf(get(), get().length, Friend[].class);
		Friend[] f2 = Arrays.copyOf(other.get(), other.get().length, Friend[].class);
		
		boolean cmpar = false;
		for(Friend first: f1) {
			cmpar = false;
			for(Friend second: f2) {
				if(first.equals(second)) {
					cmpar = true;
					break;
				}
			}
			if(cmpar==false) {
				return false;
			}
		}
		
		return cmpar;
	}
}
