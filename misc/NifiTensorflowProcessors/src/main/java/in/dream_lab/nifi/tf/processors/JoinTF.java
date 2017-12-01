package in.dream_lab.nifi.tf.processors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
@SideEffectFree
@Tags({"Join","Edgent","Tensorflow"})
@CapabilityDescription("Filters Image that matches the object class")
public class JoinTF extends AbstractProcessor implements Processor{
	
	
	final ComponentLog logger = getLogger();
	 private Set<Relationship> relationships;
	 private List<PropertyDescriptor> properties;
	 public static final PropertyDescriptor OBJECT_CLASS = new PropertyDescriptor.Builder()
			    .name("Object Class")
			    .description("Specifies the class of object that has to be filtered")
			    .required(true)
			    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			    .build();
//	private Map<String,String> imageMap;
//	private Map<String, String> urlMap;
	private Map<String, String> batchMap;
	@Override
	public void init(final ProcessorInitializationContext context){
		final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
        
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(OBJECT_CLASS);
        this.properties = Collections.unmodifiableList(properties);
//        imageMap = new HashMap<String,String>();
//        urlMap = new HashMap<String,String>();
        batchMap = new HashMap<String,String>();
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
		FlowFile flowfile = session.get();
		Map<String,String> attr =flowfile.getAttributes();
		String batchID = flowfile.getAttribute("batchID");
		if(attr.containsKey("fromEdgent")) {
			//String imageId = flowfile.getAttribute("imageID");
			if(batchMap.containsKey(batchID)) {
				session.transfer(flowfile, REL_SUCCESS);
				batchMap.put(batchID, "");
			} else {
				session.remove(flowfile);
            }
		}
		else {
            batchMap.put(batchID, "");
            session.remove(flowfile);
        }
        session.commit();
//		else
//		{
//			String url = flowfile.getAttribute("URL");
//			urlMap.put(batchID,url);
//			if(imageMap.containsKey(batchID)) {
//				flowfile = session.putAttribute(flowfile, "imageID",imageMap.get(batchID));
//				session.transfer(flowfile, REL_SUCCESS);
//				imageMap.remove(batchID);
//				batchMap.put(batchID, "");
//			}
//			else
//				session.remove(flowfile);
//		}
	}
}
