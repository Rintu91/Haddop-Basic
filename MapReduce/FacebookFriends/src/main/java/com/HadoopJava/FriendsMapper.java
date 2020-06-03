package com.HadoopJava;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.parser.ParseException;
public class FriendsMapper extends Mapper<LongWritable,Text,FriendPair,FriendArray>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer fields= new StringTokenizer(value.toString(), "\t");
		try {
			Friend user = ReadFriends.readUser(fields.nextToken());
			List<Friend> arr = ReadFriends.readFriends(fields.nextToken());
			Friend[] frndList= Arrays.copyOf(arr.toArray(), arr.toArray().length, Friend[].class);
			FriendArray outputValues = new FriendArray(Friend.class, frndList);
			
			for(Friend f: arr) {
				FriendPair fPair = new FriendPair(user,f);
				context.write(fPair, outputValues);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
}
