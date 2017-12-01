import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import in.dream_lab.nifi.etl.processors.SenMLParser;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String prevResult = "META:pickup_datetime@pickup_longitude@pickup_latitude@dropoff_longitude@dropoff_latitude@payment_type@";
		//prevResult = prevResult.replace("}{","}@{");
    	//String[] jsonObj = prevResult.split("@");
    	String[] arr = prevResult.split(":");
    	
//		JsonArray jo= (JsonArray) (new JsonParser()).parse(m);
//		System.out.println(jo.get(1).getAsJsonObject().get("total_amount"));
    	System.out.println(arr[1]);
	}

}
