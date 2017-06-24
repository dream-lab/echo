package in.dream_lab.echo.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
//import nifi.NifiClient;
//import nifi.NifiClientFactory;
//import nifi.utils.Device;
/*This Bean stores all the device related information extracted from the Resource Directory*/
public class Device implements Serializable{

	private String deviceUUID;
	
	private int port = 8080;
	
	/*TODO: Convert to long*/
	private String deviceIP;
	
	/* cpuUtil is the current CPU utilization of the device */
	private float cpuUtil;
	
	/* cpuAvail is calculated as # of CPU cores * 100 */
	/* Note that this value will not change over time for a particular device */
	private float cpuAvail;
	
	/* memUtil is the current memory utilization of the device in MB */
	private float memUtil;
	
	/* memAvail is the total amount of memory available on the device in MB*/
	/* Note that this value will not change over time for a particular device */
	private float memAvail;
	
	private boolean isAccelerated;
	
	private HashMap<String,Container> containersInfo;
	
	private ArrayList<InputStream> inputStreams;
	
	public Device() {
		// TODO Auto-generated constructor stub
	}
	
	
	/**
	 * Returns the unique identifier called deviceUUId for the device
	 * @return deviceUUID for the device
	 */
	public String getDeviceUUID(){
		return this.deviceUUID;
	}

	/**
	 * Sets the unique identifier called deviceUUId for the device 
	 * @param deviceUUID for the device
	 */
	public void setDeviceUUID(String deviceUUID){
		this.deviceUUID = deviceUUID;
	}
	
	/**
	 * Returns the IP address of the device as a String
	 * @return IP address of the device
	 */
	public String getDeviceIP(){
		return this.deviceIP;
	}
	
	public void setDeviceIP(String deviceIP){
		this.deviceIP = deviceIP;
	}
	
	public float getCpuUtil(){
		return this.cpuUtil;
	}

	public void setCpuUtil(float cpuUtil){
		this.cpuUtil = cpuUtil;
	}
	
	public float getCpuAvail(){
		return this.cpuAvail;
	}
	
	public void setCpuAvail(float cpuAvail){
		this.cpuAvail = cpuAvail;
	}
	
	public float getMemUtil(){
		return this.memUtil;
	}

	public void setMemUtil(float memUtil){
		this.memUtil = memUtil;
	}
	
	public float getMemAvail(){
		return this.memAvail;
	}
	
	public void setMemAvail(float memAvail){
		this.memAvail = memAvail;
	}
	
	public HashMap<String,Container> getContainersInfo(){
		return this.containersInfo;
	}
	
	public void setContainersInfo(HashMap<String,Container> containersInfo){
		this.containersInfo = containersInfo;
	}
	
	public ArrayList<InputStream> getInputStreams(){
		return this.inputStreams;
	}
	
	public void setInputStreams(ArrayList<InputStream> inputStreams){
		this.inputStreams = inputStreams;
	}
	
	public void addInputStreams(InputStream inputStream){
		if(this.inputStreams == null){
			this.inputStreams = new ArrayList<InputStream>();
		}
		this.inputStreams.add(inputStream);
	}
	
	public void addContainer(String containerUUID, Container container){
		if(containersInfo == null)
			containersInfo = new HashMap<String,Container>();
		containersInfo.put(containerUUID, container);
	}
	
	public void removeContainer(String containerUUID){
		containersInfo.remove(containerUUID);
	}


	public boolean isAccelerated() {
		return isAccelerated;
	}


	public void setAccelerated(boolean isAccelerated) {
		this.isAccelerated = isAccelerated;
	}
	public String toString(){
		return this.deviceUUID.concat(" ").concat(Float.toString(cpuAvail));
	}
	
    public NifiClient getClient(){
        NifiClientFactory factory = new NifiClientFactory();
        return factory.create(this.deviceIP, this.port);
    }

    public boolean equals(Device device) {
        if (this.deviceIP == device.deviceIP &&
                this.port == this.port)
            return true;
        else
            return false;
    }	
}
