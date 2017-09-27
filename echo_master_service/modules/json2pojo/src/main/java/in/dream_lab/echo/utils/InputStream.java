package in.dream_lab.echo.utils;

import java.io.Serializable;

public class InputStream implements Serializable{

	private String inputStreamUUID;
	/* TODO: refactor to Enum*/
	private String type;
	private String deviceUUID;
	public InputStream() {
		// TODO Auto-generated constructor stub
	}

	public String getInputStream(){
		return this.inputStreamUUID;
	}
	
	public void setInputStream(String inputStreamUUID){
		this.inputStreamUUID = inputStreamUUID;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getDeviceUUID() {
		return deviceUUID;
	}

	public void setDeviceUUID(String deviceUUID) {
		this.deviceUUID = deviceUUID;
	}
	
	public String toString(){
		return inputStreamUUID.concat(" ").concat(type).concat(" ").concat(deviceUUID);
	}
}
