package in.dream_lab.echo.master;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import in.dream_lab.echo.utils.Container;
import in.dream_lab.echo.utils.Device;
import in.dream_lab.echo.utils.InputStream;
import in.dream_lab.echo.utils.Processor;
import in.dream_lab.echo.utils.Catalog;

public class ResourceDirectory {

	
	private final String USER_AGENT = "Mozilla/5.0";
	
	public ResourceDirectory() {
		// TODO Auto-generated constructor stub
	}

	public ArrayList<Device> getDevices(){
		ArrayList<Device> devices = makeDevicesArrayList();
		return devices;
	}
	public ArrayList<InputStream> getInputStreams(){
		ArrayList<InputStream> inputStreams = new ArrayList<InputStream>();
		
		/* make a call to find all /device prefix and marshal into individual objects*/
		
		String inputStreamData = searchInputStreams();
		ObjectMapper mapper = new ObjectMapper();
		Catalog catalogObject;
		try {
			catalogObject = mapper.readValue(inputStreamData, Catalog.class);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		for(Object item : catalogObject.getItems()){
			InputStream toAdd = populateInputStreams(item);
			if(toAdd != null)
			inputStreams.add(toAdd);
		}
		
		return inputStreams;
	}
	
	public void addDataFlow(Map<Processor,Device> mapping, String originalJSON, String UUID){
		Map<String, String> mappingAsString = new HashMap<String, String>();
		String jsonMapping = null;
		System.out.println("first");
		System.out.println(mapping.keySet());
		for(Map.Entry<Processor, Device> entry: mapping.entrySet()){
			mappingAsString.put(entry.getKey().getUuid(), entry.getValue().getDeviceUUID());
		}
		System.out.println(mappingAsString.toString());
		ObjectMapper mapper = new ObjectMapper();
		try {
			jsonMapping = mapper.writeValueAsString(mappingAsString);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("---------------\n" + jsonMapping);
		jsonMapping = jsonMapping.replaceAll("\"", "\\\\\"");
		String FinalJSON = "{\"item-metadata\": [{\"val\": \"DataFlow\",\"rel\": \"urn:X-hypercat:rels:hasDescription:en\"},{\"val\":\"" + 
				originalJSON + "\",\"rel\": \"originalJSON\"},{\"val\": \"" + UUID + "\",\"rel\": \"DataFlowUUID\"},{\"val\": \"" + jsonMapping + 
				"\",\"rel\": \"Mapping JSON\"}],\"href\": \"/dataflow/" + UUID +"\"}";
		try {
			sendPost(FinalJSON, UUID);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Map<String, String> getMapping(String dataFlowUUID){
		String inputStreamData = searchDataFlows(dataFlowUUID);
		ObjectMapper mapper = new ObjectMapper();
		Map<String, String> mapping = null;
		Catalog catalogObject;
		try {
			catalogObject = mapper.readValue(inputStreamData, Catalog.class);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		for(Object item : catalogObject.getItems()){
			mapping = populateDataFlows(item);
		}
		return mapping;
	}
	
	public String getOriginalJSON(String dataFlowUUID){
		String inputStreamData = searchDataFlows(dataFlowUUID);
		ObjectMapper mapper = new ObjectMapper();
		String originalJSON = null;
		Catalog catalogObject;
		try {
			catalogObject = mapper.readValue(inputStreamData, Catalog.class);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		for(Object item : catalogObject.getItems()){
			originalJSON = getJSON(item);
		}
		return originalJSON;
	}
	
	private String getJSON(Object item){
		String originalJSON = null;
		LinkedHashMap<String,Object> linkedItem = (LinkedHashMap<String,Object>) item;
		ArrayList<LinkedHashMap>linkedMap = (ArrayList<LinkedHashMap>) linkedItem.get("item-metadata");
		String mappingAsString = null;
		for(LinkedHashMap hashmap : linkedMap){
			switch((String)hashmap.get("rel")){
			case "originalJSON": 
				originalJSON = (String)hashmap.get("val");
				break;
			}
		}
		return originalJSON;
	}
	
	private Map<String, String> populateDataFlows(Object item){
		Map<String, String> mapping = new HashMap<String, String>();
		LinkedHashMap<String,Object> linkedItem = (LinkedHashMap<String,Object>) item;
		ArrayList<LinkedHashMap>linkedMap = (ArrayList<LinkedHashMap>) linkedItem.get("item-metadata");
		String mappingAsString = null;
		for(LinkedHashMap hashmap : linkedMap){
			switch((String)hashmap.get("rel")){
			case "Mapping JSON": 
				mappingAsString = (String)hashmap.get("val");
				break;
			}
		}
		mappingAsString = mappingAsString.replaceAll( "\\\\\"","\"");
		ObjectMapper mapper = new ObjectMapper();
		try {
			mapping = mapper.readValue(mappingAsString, new TypeReference<Map<String, String>>(){});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mapping;
	}
	
	private String searchDataFlows(String dataFlowUUID){
		StringBuffer response;
		try{
			/*TODO: get rid of this hardcoded URL*/
			String url = "http://localhost:8080/cat?prefix-href=/dataflow/" + dataFlowUUID;
		
			URL  obj = new URL(url);
			
			HttpURLConnection con = null;
			con = (HttpURLConnection) obj.openConnection();
		
			// optional default is GET
			con.setRequestMethod("GET");

			//add request header
			con.setRequestProperty("User-Agent", USER_AGENT);

			int responseCode = con.getResponseCode();
		
			System.out.println("\nSending 'GET' request to URL : " + url);
			System.out.println("Response Code : " + responseCode);

			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		
			String inputLine;
			response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
		
			//print result
			System.out.println(response.toString());
		}catch(Exception e){
			System.out.println(e);
			/*TODO: Add logs*/
			return null;
		}
		return response.toString();
	}

	private void sendPost(String urlParameters, String UUID) throws Exception {

		String url = "http://localhost:8080/cat?href=/dataflow/"+UUID;
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		//add reuqest header
		con.setRequestMethod("POST");
		con.setRequestProperty("User-Agent", USER_AGENT);
		con.setRequestProperty("Content-Type", "text/plain");
		con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
		//String urlParameters = "{\"intersection\": [{\"query\": \"?val=test\"},{\"query\": \"?val=87230\"}]}";
		//String urlParameters = "{\"item-metadata\": [{\"val\": \"CPUUtil\",\"rel\": \"urn:X-hypercat:rels:hasDescription:en\"},{\"val\": \"200\",\"rel\": \"CPUUtil\"}],\"href\": \"/device/cpu/1\"}";
		// Send post request
		con.setDoOutput(true);
		DataOutputStream wr = new DataOutputStream(con.getOutputStream());
		wr.writeBytes(urlParameters);
		wr.flush();
		wr.close();

		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'POST' request to URL : " + url);
		System.out.println("Post parameters : " + urlParameters);
		System.out.println("Response Code : " + responseCode);

		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		//print result
		System.out.println(response.toString());

	}	
	
	private InputStream populateInputStreams(Object item){
		InputStream toAdd = null;
		LinkedHashMap<String,Object> linkedItem = (LinkedHashMap<String,Object>) item;
		/*The switch is pretty much useless but stays as a sanity check*/
		switch(((String)linkedItem.get("href")).split("/")[2]){
		case "input_stream": toAdd = populateInputStream((ArrayList<LinkedHashMap>) linkedItem.get("item-metadata"));
		 			 break; 
		}
		return toAdd;
	}
	
	
	private InputStream populateInputStream(ArrayList<LinkedHashMap> linkedMap){
		InputStream newInputStream = new InputStream();
		for(LinkedHashMap hashmap : linkedMap){
			switch((String)hashmap.get("rel")){
			case "Type": 
				newInputStream.setType((String)hashmap.get("val"));
				break;
			case "Input_Stream_UUID": 
				newInputStream.setInputStream((String)hashmap.get("val"));
				break;
			case "DeviceUUID": 
				newInputStream.setDeviceUUID((String)hashmap.get("val"));
				break;	
			}
		}
		return newInputStream;
	}
	
	private ArrayList<Device> makeDevicesArrayList(){
		ArrayList<Device> devices = new ArrayList<Device>();
		HashMap<String, Device> deviceMap = new HashMap<String, Device>();
		
		/* make a call to find all /device prefix and marshal into individual objects*/
		String devicesData = searchDeviceMetaData();
		ObjectMapper mapper = new ObjectMapper();
		Catalog catalogObject;
		try {
			catalogObject = mapper.readValue(devicesData, Catalog.class);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		for(Object item : catalogObject.getItems()){
			populateDevice(item, deviceMap);
		}
		
		for(String key : deviceMap.keySet()){
			System.out.println(deviceMap.get(key));
			devices.add(deviceMap.get(key));
			if(deviceMap.get(key).getContainersInfo() != null){
				System.out.println(deviceMap.get(key).getContainersInfo().keySet());
			}
			if(deviceMap.get(key).getInputStreams() != null){
				System.out.println(deviceMap.get(key).getInputStreams().size());
			}
		}		
		return devices;
	}
	
	private void populateDevice(Object item, HashMap<String, Device> deviceMap){
		LinkedHashMap<String,Object> linkedItem = (LinkedHashMap<String,Object>) item;
		Device deviceToPopulate;
		/*Extract the uuid from href and check if the object is already created*/
		if(deviceMap.get(((String)linkedItem.get("href")).split("/")[3]) == null){
			deviceToPopulate = new Device();
			deviceMap.put(((String)linkedItem.get("href")).split("/")[3], deviceToPopulate);
		}
		else{
			deviceToPopulate = deviceMap.get(((String)linkedItem.get("href")).split("/")[3]);
		}
		
		switch(((String)linkedItem.get("href")).split("/")[2]){
		case "meta": populateMetaForDevice(deviceToPopulate, (ArrayList<LinkedHashMap>) linkedItem.get("item-metadata"));
					 break;
		case "cpu" : populateCPUForDevice(deviceToPopulate, (ArrayList<LinkedHashMap>) linkedItem.get("item-metadata"));
					 break;
		case "mem" : populateMemForDevice(deviceToPopulate, (ArrayList<LinkedHashMap>) linkedItem.get("item-metadata"));
					 break;
		case "ip"  : populateIPForDevice(deviceToPopulate, (ArrayList<LinkedHashMap>) linkedItem.get("item-metadata"));
					 break;
		case "containers": populateContainersForDevice(deviceToPopulate, (ArrayList<LinkedHashMap>) linkedItem.get("item-metadata"));
					 break;
		case "input_stream": populateInputStreamForDevice(deviceToPopulate, (ArrayList<LinkedHashMap>) linkedItem.get("item-metadata"));
		 			 break; 
		}
	}	

	private void populateContainersForDevice(Device deviceToPopulate, ArrayList<LinkedHashMap> linkedMap){
		Container newContainer = new Container();
		for(LinkedHashMap hashmap : linkedMap){
			switch((String)hashmap.get("rel")){
			case "Port": 
				newContainer.setPort(Integer.parseInt((String)hashmap.get("val")));
				break;
			case "DeviceUUID": 
				newContainer.setDeviceUUID((String)hashmap.get("val"));
				break;
			case "ContainerUUID": 
				newContainer.setContainerUUID((String)hashmap.get("val"));
				break;	
			}
		}
		deviceToPopulate.addContainer(newContainer.getContainerUUID(), newContainer);
	}
	
	
	private void populateInputStreamForDevice(Device deviceToPopulate, ArrayList<LinkedHashMap> linkedMap){
		InputStream newInputStream = new InputStream();
		for(LinkedHashMap hashmap : linkedMap){
			switch((String)hashmap.get("rel")){
			case "Type": 
				newInputStream.setType((String)hashmap.get("val"));
				break;
			case "Input_Stream_UUID": 
				newInputStream.setInputStream((String)hashmap.get("val"));
				break;
			case "DeviceUUID": 
				newInputStream.setDeviceUUID((String)hashmap.get("val"));
				break;	
			}
		}
		deviceToPopulate.addInputStreams(newInputStream);
	}
	
	
	private void populateIPForDevice(Device deviceToPopulate, ArrayList<LinkedHashMap> linkedMap){
		for(LinkedHashMap hashmap : linkedMap){
			switch((String)hashmap.get("rel")){
			case "IP": 
				deviceToPopulate.setDeviceIP((String)hashmap.get("val"));
				break;
			}
		}
	}
	private void populateMemForDevice(Device deviceToPopulate, ArrayList<LinkedHashMap> linkedMap){
		for(LinkedHashMap hashmap : linkedMap){
			switch((String)hashmap.get("rel")){
			case "MemUtil": 
				deviceToPopulate.setMemUtil(Float.parseFloat((String)hashmap.get("val")));
				break;
			}
		}
	}
	
	private void populateMetaForDevice(Device deviceToPopulate, ArrayList<LinkedHashMap> linkedMap){
		for(LinkedHashMap hashmap : linkedMap){
			switch((String)hashmap.get("rel")){
			case "DeviceUUID": 
				deviceToPopulate.setDeviceUUID((String)hashmap.get("val"));
				break;
			case "Total CPU Available":
				deviceToPopulate.setCpuAvail(Float.parseFloat((String)hashmap.get("val")));		
				break;
			case "Total Memory Available":
				deviceToPopulate.setMemAvail(Float.parseFloat((String)hashmap.get("val")));
				break;
			case "isAccelerated":
				deviceToPopulate.setAccelerated(Boolean.parseBoolean((String)hashmap.get("val")));
				break;
			}
		}
	}
	
	private void populateCPUForDevice(Device deviceToPopulate, ArrayList<LinkedHashMap> linkedMap){
		for(LinkedHashMap hashmap : linkedMap){
			switch((String)hashmap.get("rel")){
			case "CPUUtil": 
				deviceToPopulate.setCpuUtil(Float.parseFloat((String)hashmap.get("val")));
				break;
			}
		}
	}
	
	// HTTP GET request
	private String searchDeviceMetaData(){
		StringBuffer response;
		try{
			/*TODO: get rid of this hardcoded URL*/
			String url = "http://localhost:8080/cat?prefix-href=/device";
		
			URL  obj = new URL(url);
			
			HttpURLConnection con = null;
			con = (HttpURLConnection) obj.openConnection();
		
			// optional default is GET
			con.setRequestMethod("GET");

			//add request header
			con.setRequestProperty("User-Agent", USER_AGENT);

			int responseCode = con.getResponseCode();
		
			System.out.println("\nSending 'GET' request to URL : " + url);
			System.out.println("Response Code : " + responseCode);

			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		
			String inputLine;
			response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
		
			//print result
			System.out.println(response.toString());
		}catch(Exception e){
			System.out.println(e);
			/*TODO: Add logs*/
			return null;
		}
		return response.toString();
	}
	
	
	private String searchInputStreams(){
		StringBuffer response;
		try{
			/*TODO: get rid of this hardcoded URL*/
			String url = "http://localhost:8080/cat?prefix-href=/device/input_stream";
		
			URL  obj = new URL(url);
			
			HttpURLConnection con = null;
			con = (HttpURLConnection) obj.openConnection();
		
			// optional default is GET
			con.setRequestMethod("GET");

			//add request header
			con.setRequestProperty("User-Agent", USER_AGENT);

			int responseCode = con.getResponseCode();
		
			System.out.println("\nSending 'GET' request to URL : " + url);
			System.out.println("Response Code : " + responseCode);

			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		
			String inputLine;
			response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
		
			//print result
			System.out.println(response.toString());
		}catch(Exception e){
			System.out.println(e);
			/*TODO: Add logs*/
			return null;
		}
		return response.toString();
	}	
}