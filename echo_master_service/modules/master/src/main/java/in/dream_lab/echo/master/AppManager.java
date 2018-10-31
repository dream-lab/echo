package in.dream_lab.echo.master;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import in.dream_lab.echo.nifi.NifiDeployer;
import in.dream_lab.echo.utils.DataflowInput;
import in.dream_lab.echo.utils.Device;
import in.dream_lab.echo.utils.Processor;

/**
 * Created by pushkar on 5/16/17.
 * 
 * Modified by Siva on 5/26/17
 */
public class AppManager{
	private DataflowInput inputDag;
	private List<Device> devices;
	private Map<Processor, Device> deviceMapping;
	private String uuid;
	private ResourceDirectory resourceDirectory;
	private ObjectMapper mapper;
	private String mqttBroker;
	private MqttClient mqttClient;
	private AppDeployer deployer;

	public AppManager() {
		super();
		resourceDirectory = new ResourceDirectory("13.71.125.147", 8080);
		mqttBroker = "tcp://10.24.24.222:1883";
		mapper = new ObjectMapper();
		try {
			MemoryPersistence persistence = new MemoryPersistence();
			mqttClient = new MqttClient(mqttBroker, "master", persistence);
		} catch (MqttException e) {
			e.printStackTrace();
		}
	}

	public AppManager(String uuid, String json) {
		this();
		this.uuid = uuid;
		updateVariables(json);
	}

	private void updateVariables(String json) {
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        try {
			inputDag = mapper.readValue(json, DataflowInput.class);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
	

	public void startDAG() throws Exception {
		Scheduler sc = new Scheduler();
		this.devices = resourceDirectory.getDevices();
		/* To run the scalability test, we want to deploy
		 * in an incremental order so better keep devices
		 * in sorted order of deviceUUID's
		 */
		Collections.sort(this.devices, new Comparator<Device>() {

			@Override
			public int compare(Device d1, Device d2) {
				Integer id1 = Integer.parseInt(d1.getDeviceUUID());
				Integer id2 = Integer.parseInt(d2.getDeviceUUID());
				return id1.compareTo(id2);
			}
		});
		
		System.out.println("Got devices");
		System.out.println(devices.size());
		this.deviceMapping = sc.schedule(devices, inputDag);
		System.out.println(deviceMapping);

		System.out.println("inputDag");

		NetworkVisibilityMatrix matrix =
				new NetworkVisibilityMatrix("./networkvisibility.csv");

		deployer = new NifiDeployer(mqttClient, matrix);
		inputDag = deployer.deployDag(deviceMapping, inputDag);
		System.out.println("deploy done");
		String inputJSONString;
		try {
			inputJSONString = mapper.writeValueAsString(inputDag);
			inputJSONString = inputJSONString.replaceAll("\"", "\\\\\"");
			System.out.println(inputJSONString);
			System.out.println("------------------------------");
			resourceDirectory.addDataFlow(deviceMapping, inputJSONString, uuid);
			System.out.println("Should be added?");
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static Device makeDevice(String IP) {
		Device dev = new Device();
		dev.setDeviceIP(IP);
		return dev;
	}

	public boolean rebalanceDAG() throws Exception {
	    Scheduler sc = new Scheduler();
		this.devices = resourceDirectory.getDevices();
		System.out.println("Got devices");
		System.out.println(devices.size());
		this.deviceMapping = sc.schedule(devices, inputDag);
		System.out.println(deviceMapping);

		NetworkVisibilityMatrix matrix =
				new NetworkVisibilityMatrix("./networkvisibility.csv");


		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
		deployer.rebalanceDag(deviceMapping, inputDag);
		System.out.println("redeploy done");
		return true;
	}

	public boolean stopDAG() throws Exception {
		
		ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        deployer.stopDag();
        // TODO remove the entry from the RD
        return true;
	}

	private Map<Processor, Device> getPlacementMap(DataflowInput inputDag2, String dataFlowUUID) {
		Set<Processor> processors = inputDag2.getProcessors();
		
		List<Device> devices = resourceDirectory.getDevices();
		Map<String,String> mapping = resourceDirectory.getMapping(dataFlowUUID);
		
		Map<String,Processor> processorMap = new HashMap<String,Processor>();
		Map<String,Device> deviceMap = new HashMap<String,Device>();
		
		Map<Processor, Device> placementMap = new HashMap<Processor, Device>();
		
		for(Processor proc:processors) 
			processorMap.put(proc.getUuid(),proc);
		
		for(Device device: devices)
			deviceMap.put(device.getDeviceUUID(), device);
		
		for(Entry<String, String> entry:mapping.entrySet())
				placementMap.put(processorMap.get(entry.getKey()), deviceMap.get(entry.getValue()));
		
		return placementMap;
	}

	private Map<String, Device> getOldPlacementMap(DataflowInput inputDag2, String dataFlowUUID) {
		Set<Processor> processors = inputDag2.getProcessors();
		
		List<Device> devices = resourceDirectory.getDevices();
		Map<String,String> mapping = resourceDirectory.getMapping(dataFlowUUID);
		
		Map<String,Device> deviceMap = new HashMap<String,Device>();
		
		Map<String, Device> placementMap = new HashMap<String, Device>();
		
		for(Device device: devices)
			deviceMap.put(device.getDeviceUUID(), device);
		
		for(Entry<String, String> entry:mapping.entrySet())
				placementMap.put(entry.getKey(), deviceMap.get(entry.getValue()));
		
		return placementMap;
	}

}
