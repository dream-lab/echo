package in.dream_lab.nifi.tf.processors;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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


@SideEffectFree
@Tags({"update"})
@CapabilityDescription("update")
public class Update extends AbstractProcessor implements Processor{
	final ComponentLog logger = getLogger();
	
	

	
	@Override
	public void init(final ProcessorInitializationContext context){
		final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
     this.relationships = Collections.unmodifiableSet(relationships);

     
    
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
	 private Set<Relationship> relationships;
	 private List<PropertyDescriptor> properties;
	
	 @Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		// TODO Auto-generated method stub
		FlowFile flowfile = session.get();
        
	    flowfile = session.putAttribute(flowfile, "Directory", "/home/sivaprakash/Siva/Edgent/NifiTestout");  
	    session.transfer(flowfile,REL_SUCCESS);
	}
}
