package com.HadoopJava;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
public class ReadFriends {
	public static Friend getValues(JSONObject data) {
		int id = ((Long)data.get("id")).intValue();
		String name = (String)data.get("name");
		String hometown = (String)data.get("hometown");
		Friend newFrnd = new Friend(id,name,hometown);
		return newFrnd;
	}
	public static Friend readUser(String input) throws ParseException {
		JSONParser parser = new JSONParser();
		JSONObject data = (JSONObject)parser.parse(input);
		return getValues(data);		
	}
	public static List<Friend> readFriends(String input) throws ParseException{
		ArrayList<Friend> arr = new ArrayList<Friend>();
		JSONParser parser = new JSONParser();
		JSONArray data = (JSONArray)parser.parse(input);
		
		for(Object obj: data) {
			JSONObject elem = (JSONObject)obj;
			arr.add(getValues(elem));
		}
		return arr;
	}
}
