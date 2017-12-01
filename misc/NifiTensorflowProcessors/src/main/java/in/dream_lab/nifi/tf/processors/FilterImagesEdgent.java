package in.dream_lab.nifi.tf.processors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.edgent.function.Predicate;
import org.apache.edgent.providers.direct.DirectProvider;
import org.apache.edgent.topology.TStream;
import org.apache.edgent.topology.Topology;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import in.dream_lab.nifi.tf.processors.TempSource;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;


@SideEffectFree
@Tags({"Filter","Images","Tensorflow"})
@CapabilityDescription("Filters Image that matches the object class")
public class FilterImagesEdgent extends AbstractProcessor implements Processor{
	
	
	final ComponentLog logger = getLogger();
	 private Set<Relationship> relationships;
	 private List<PropertyDescriptor> properties;
	 public static final PropertyDescriptor OBJECT_CLASS = new PropertyDescriptor.Builder()
			    .name("Object Class")
			    .description("Specifies the class of object that has to be filtered")
			    .required(true)
			    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			    .build();
	private Map<String,Integer> batchMap;
	private Map<String,String> resultMap;

	@Override
	public void init(final ProcessorInitializationContext context){
		final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
        
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(OBJECT_CLASS);
        this.properties = Collections.unmodifiableList(properties);
        batchMap = new HashMap<String,Integer>();
	}

	@Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

	@Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
             .name("success")
             .description("Filter successful")
             .build();
	
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		// TODO Auto-generated method stub
		resultMap = new HashMap<String,String>();
		System.out.println("Started Nifi");
		FlowFile flowfile = session.get();
		String jsonDir = flowfile.getAttribute("sourceDir");
		String batchID = flowfile.getAttribute("batchID");

		// why are they being removed?
		//if(batchMap.containsKey(batchID)) {
		//	session.remove(flowfile);
		//	return;
		//}
		batchMap.put(batchID, 1);
		DirectProvider dp = new DirectProvider();
		
		Topology topology = dp.newTopology("Edgent Application");
		String objectClass = context.getProperty(OBJECT_CLASS).getValue();
        
		getLogger().info("Created Edgent Topology: "+topology.getName());
		TempSource source = new TempSource(jsonDir +"/out");
		
		TStream<Pair<String,String>> sourceStream = topology.source(source);
		TStream<Pair<String,String>> filteredStream = sourceStream.filter(new Predicate<Pair<String,String>>() {

			@Override
			public boolean test(Pair<String,String> map) {
				boolean isValid = false;
				String jsonString = map.getRight();
				JsonArray jsonArray = (new Gson()).fromJson(jsonString, JsonArray.class);
				for(int i=0;i<jsonArray.size();i++)
				{
					JsonObject jsonObject = jsonArray.get(i).getAsJsonObject();
					if(jsonObject.get("label").getAsString().equals(objectClass)) {
						isValid = true;
						break;
					}
				}
				return isValid;
				
			}
		});
		filteredStream.sink(tuple -> update(tuple));
		dp.submit(topology);
		System.out.println("Submitted Topology");

		// Why are we sleeping here?
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    System.out.println(resultMap.size());
	    // To write the results back out ot flow file
	    session.remove(flowfile);
	    
	    //for(Map.Entry<String, String> entry:resultMap.entrySet())
	    
	    if(resultMap.size()>0){
	    	FlowFile flowFile = session.create();
	    	flowFile = session.putAttribute(flowFile, "batchID", batchID);
	    	// should imageID be a NULL string?
	    	flowFile = session.putAttribute(flowFile, "imageID", "");
            flowFile = session.putAttribute(flowFile, "fromEdgent", "1");
	    	session.transfer(flowFile,REL_SUCCESS);
	    }
	}
	private void update(Pair<String, String> tuple) {
		resultMap.put(tuple.getLeft(),tuple.getRight());
	}

    
    
}
