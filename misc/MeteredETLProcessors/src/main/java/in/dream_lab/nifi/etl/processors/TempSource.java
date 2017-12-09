package in.dream_lab.nifi.etl.processors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.edgent.function.Supplier;
import org.bouncycastle.crypto.tls.HashAlgorithm;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class TempSource implements Supplier<Iterable<Map<String,String>>>{

	private String[] jsonArr;
	public TempSource(String[] jsonArr) {
		this.jsonArr = jsonArr;
	}

	public Iterable<Map<String,String>> get() {
		// TODO Auto-generated method stub
		List<Map<String,String>> mapList = new ArrayList<Map<String,String>>();
		JsonParser parser = new JsonParser(); 
		JsonObject json ;
		for(String jsonString:jsonArr) {
			Map<String,String> sourceMap =  new HashMap<String,String>();
			json = (JsonObject) parser.parse(jsonString);
			sourceMap.put("SENSORID", json.get("taxi_identifier").toString());
			sourceMap.put("trip_time_in_secs", json.get("trip_time_in_secs").toString());
			sourceMap.put("trip_distance", json.get("trip_distance").toString());
			sourceMap.put("fare_amount", json.get("fare_amount").toString());
			sourceMap.put("tip_amount", json.get("tip_amount").toString());
			sourceMap.put("surcharge", json.get("surcharge").toString());
			mapList.add(sourceMap);
		}
		
		
		return mapList;
	}

}
