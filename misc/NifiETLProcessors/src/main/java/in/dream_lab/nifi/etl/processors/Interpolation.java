package in.dream_lab.nifi.etl.processors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class Interpolation {

	// static fields common to all threads
	private static final Object SETUP_LOCK = new Object(); 
	private static boolean doneSetup = false;
	private static ArrayList<String> useMsgField ; 
	private static float interpolCountWindowSize;

	private HashMap<String, ArrayList<Float>> valuesMap;

	
	public void setup() {
		
			String useMsgFieldList = "surcharge";
			String [] useMsgFieldArray = useMsgFieldList.split(",");
			useMsgField = new ArrayList<String>();
			for(int i = 0; i < useMsgFieldArray.length ; i++)
			     useMsgField.add(useMsgFieldArray[i]);
			interpolCountWindowSize = 0;
			doneSetup=true;
			valuesMap = new HashMap<String, ArrayList<Float>>();
		
	}

	protected Map doTaskLogic(Map map) 
	{	
		String sensorId = (String) map.get("SENSORID");
		Set<Map.Entry<String, String>> entrySet = map.entrySet();
		String currentVal ;
		Float sum = 0.0f; 
		
		//Window size zero indicates no interpolation needed
		if(interpolCountWindowSize == 0 || sensorId == null || entrySet.size() == 0 )
		{
			return map;
		}
		for (Entry<String, String> entry : entrySet) 
		{
			String obsType = entry.getKey();
			
			//We need to operate only on fields specified in useMsgField 
			if(useMsgField.contains(obsType))
			{
				currentVal = (String)map.get(obsType);
				String key = sensorId + obsType;
				
				//If map contains past values 
				if(valuesMap.containsKey(key))
				{
					ArrayList<Float> values = valuesMap.get(key);
					
					//Interpolation needs to be done only if current value is null
					if(currentVal.equals("null"))
					{
						for(int i =0 ; i < values.size() ; i++)
						{
							sum = sum +values.get(i);
						}
						currentVal= (sum/ values.size())+"";
						map.put(key, currentVal);
					}
					//Since the current value is not null , just add the values limiting array to the window size
					else 
					{
					  if (values.size() == interpolCountWindowSize)
						  values.remove(0);
					  
					  values.add(Float.parseFloat(currentVal));
					  valuesMap.put(key, values);
					  map.put(key, currentVal);
					}
				}
				//No past values Adding sensorId + obsType as key for first time 
				else if(!currentVal.equals("null"))
				{
					ArrayList<Float> list = new ArrayList<Float>();
					list.add(Float.parseFloat(currentVal));
					valuesMap.put(key, list);
					map.put(key, currentVal);
				}
			}
		}
		
		return map ;
	}	
}
