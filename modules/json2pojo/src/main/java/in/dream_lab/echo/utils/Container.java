package in.dream_lab.echo.utils;

import java.io.Serializable;

public class Container implements Serializable{
	
	/* The unique identifier assigned to the container */
	private String containerUUID;
	
	/* current CPU Utilization of the container */
	private float cpuUtil;
	
	/* Total CPU shares allocated to the container */
	private float cpuAllocated;
	
	/* current memory Utilization of the container */
	private float memUtil;
	
	/* Total memory allocated to the container*/
	private float memAllocated;
	
	private int port;
	
	private String deviceUUID;
	
	public Container() {
		// TODO Auto-generated constructor stub
	}

	public String getContainerUUID(){
		return this.containerUUID;
	}
	
	public void setContainerUUID(String containerUUID){
		this.containerUUID = containerUUID;
	}
	
	public float getCpuUtil(){
		return this.cpuUtil;
	}
	
	public void setCpuUtil(float cpuUtil){
		this.cpuUtil = cpuUtil;
	}
	
	public float getCpuAllocatedl(){
		return this.cpuAllocated;
	}
	
	public void setCpuAllocated(float cpuAllocated){
		this.cpuAllocated = cpuAllocated;
	}
	
	public float getMemUtil(){
		return this.memUtil;
	}
	
	public void setMemUtil(float MemUtil){
		this.memUtil = MemUtil;
	}
	
	public float getMemAllocatedl(){
		return this.memAllocated;
	}
	
	public void setMemAllocated(float memAllocated){
		this.memAllocated = memAllocated;
	}
	
	public int getPort(){
		return this.port;
	}
	
	public void setPort(int port){
		this. port = port;
	}

	public String getDeviceUUID() {
		return deviceUUID;
	}

	public void setDeviceUUID(String deviceUUID) {
		this.deviceUUID = deviceUUID;
	}
	
}
