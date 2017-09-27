package in.dream_lab.echo.master;

/**
 * Created by Aakash on 5/26/17.
 */
import java.util.*;

import in.dream_lab.echo.utils.DataflowInput;
import in.dream_lab.echo.utils.Processor;
import in.dream_lab.echo.utils.Device;
import in.dream_lab.echo.utils.InputStream;
//import in.dream_lab.interfaces.IScheduler;

public class Scheduler {

	public Scheduler() {
		// TODO Auto-generated constructor stub
	}
	
	
	public Map<Processor,Device> schedule(List<Device> devices, DataflowInput inputDag){
		Map<Processor,Device> mapping = new HashMap<Processor,Device>();
		Set<Processor> processors = inputDag.getProcessors();
		for(Processor processor : processors){
			if(processor.getIsInput()){
				for(Device device : devices){
					if (device.getInputStreams() == null)
						continue;
					for(InputStream inputStream: device.getInputStreams()){
						 if(inputStream.getInputStream().equals(processor.getSourceUuid())){
							 mapping.put(processor, device);
							 //processors.remove(processor);
							 break;
						 }
					}
				}
			}
		}
		/*TODO: Test*/
		int current = (int) ((Math.random()*1000)%devices.size());
		for(Processor processor : processors){
		    if (processor.getIsInput())
		    	continue;
			mapping.put(processor, devices.get(current));
			current = (current + 1)%devices.size();
		}
		return mapping;
	}
}
