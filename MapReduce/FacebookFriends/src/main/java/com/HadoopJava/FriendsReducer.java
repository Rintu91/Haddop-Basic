package com.HadoopJava;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.mapreduce.Reducer;
public class FriendsReducer extends Reducer<FriendPair, FriendArray, FriendPair, FriendArray>{
	public void reduce(FriendPair key, Iterable<FriendArray> values, Context context) throws IOException, InterruptedException {
		List<Friend[]> listFrnd = new ArrayList<Friend[]>();
		List<Friend> commonList = new ArrayList<Friend>();
		int elemCnt = 0;
		
		for(FriendArray farray : values) {
			Friend[] f = Arrays.copyOf(farray.get(), farray.get().length, Friend[].class);
			listFrnd.add(f);
			elemCnt++;
		}
		
		if(elemCnt != 2)
			return;
		Friend[] f1 = listFrnd.get(0);
		Friend[] f2 = listFrnd.get(1);
		
		for(Friend value1 : f1) {
			for(Friend value2 : f2) {
				if(value1.equals(value2))
					commonList.add(value1);
			}
		}
		
	
		Friend[] commonFriends = Arrays.copyOf(commonList.toArray(), commonList.toArray().length, Friend[].class);
		FriendArray newArr = new FriendArray(Friend.class, commonFriends);
		context.write(key, newArr);
	}
}
