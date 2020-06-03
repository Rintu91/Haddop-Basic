package com.HadoopJava;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.collections.map.HashedMap;
import org.json.simple.parser.ParseException;

import junit.framework.TestResult;

public class TestMapper {

	public HashMap testMap(String fileName) throws IOException, ParseException {
		// TODO Auto-generated method stub		
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		String line;
		HashMap<FriendPair,List<FriendArray>> map = new HashMap<FriendPair,List<FriendArray>>();
		while((line=reader.readLine())!=null) {
			//System.out.println(line);
			StringTokenizer fields= new StringTokenizer(line, "\t");
			//while(fields.hasMoreTokens()) {
			//	System.out.println(fields.nextToken());
			//}
			
			Friend user = ReadFriends.readUser(fields.nextToken());
			List<Friend> arr = ReadFriends.readFriends(fields.nextToken());
			Friend[] frndList= Arrays.copyOf(arr.toArray(), arr.toArray().length, Friend[].class);
			FriendArray outputValues = new FriendArray(Friend.class, frndList);
				
			for(Friend f: arr) {
				FriendPair fPair = new FriendPair(user,f);
				//System.out.println((fPair.toString()+":"+ outputValues.toString()));
				//if(map.containsKey(fPair)) {
				int found = 0;
				FriendPair foundKey = null;
				for(FriendPair key: map.keySet()) {
					if(fPair.equals(key)) {
						found    = 1;
						foundKey = key; 
					}
				}
				if(found==1) {
					//System.out.println();
					List<FriendArray> list = map.get(foundKey);
					list.add(outputValues);
					map.put(foundKey, list);
				}
				else {
					List<FriendArray> list = new ArrayList<FriendArray>();
					list.add(outputValues);
					map.put(fPair, list);
				}
			}
		}
		return map;
	}
	
	public void testReduce(HashMap<FriendPair,List<FriendArray>> map) {
		for(FriendPair key: map.keySet()) {
			List<Friend[]> totalList = new ArrayList<Friend[]>();
			List<Friend> commonFrnds = new ArrayList<Friend>();
			
			int elemCnt = 0;
			for(FriendArray frndList: map.get(key)) {
				Friend[] f=Arrays.copyOf(frndList.get(), frndList.get().length, Friend[].class);
				totalList.add(f);
				elemCnt++;
			}
			if(elemCnt!=2)
				continue;
			Friend[] f1=totalList.get(0);
			Friend[] f2=totalList.get(1);
			
			for(Friend value1: f1) {
				for(Friend value2: f2) {
					if(value1.equals(value2))
						commonFrnds.add(value1);
				}
			}
			Friend[] commonFrnd   = Arrays.copyOf(commonFrnds.toArray(), commonFrnds.size(), Friend[].class);
			FriendArray outValues = new FriendArray(Friend.class, commonFrnd);
			System.out.println(key.toString()+":"+outValues.toString());
		}
	}
	
	public static void main(String[] args) throws IOException, ParseException {
		String fileName="C:\\Users\\Jayeeta\\Desktop\\BigData\\Hadoop\\Course\\hirw-workshop\\input\\facebook\\facebook-friends";
		TestMapper work = new TestMapper();
		HashMap<FriendPair,List<FriendArray>> map = work.testMap(fileName);
		
		System.out.println("---------------------");
		for(FriendPair key: map.keySet()) {
			System.out.println(key.toString()+" "+map.get(key).size());
		}
		System.out.println("---------------------");
		work.testReduce(map);
	}
	

}
