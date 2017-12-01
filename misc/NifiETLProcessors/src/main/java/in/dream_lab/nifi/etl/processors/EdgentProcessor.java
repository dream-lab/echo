package in.dream_lab.nifi.etl.processors;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import in.dream_lab.nifi.etl.processors.TempSource;
@SideEffectFree
@Tags({"Edgent"})
@CapabilityDescription("Detect end of Stream")
public class EdgentProcessor extends AbstractProcessor implements Processor{
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

	 public static final Relationship REL_SUCCESS = new Relationship.Builder()
             .name("success")
             .description("Successful parse result")
             .build();
	 private Set<Relationship> relationships;
	
	 @Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		// TODO Auto-generated method stub
		System.out.println("Started Nifi");
		TStream<Double> tempStream;
		DirectProvider dp = new DirectProvider();
		
		Topology topology = dp.newTopology("Simple Edgent Application");
		final AtomicReference<String> value = new AtomicReference<String>();
        FlowFile flowfile = session.get();
//        flowfile.
        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in)  {
                try{
                	String json = IOUtils.toString(in, "UTF-8");
                	//getLogger().debug(json);
                    value.set(json + "\n Processed");
                    
                    getLogger().debug(value.get());
                }catch(Exception ex){
                    ex.printStackTrace();
                    getLogger().error("Failed to read json string.");
                    getLogger().error(ex.toString());
                }
            }
        });
		/*//System.out.println("Created Topology: "+topology.getName());
		TempSource source = new TempSource();
		//TStream<Double> tempStream = topology.source(source, 3, TimeUnit.MILLISECONDS);
		//dp.submit(topology);
		try{
		    PrintWriter writer = new PrintWriter("/home/sivaprakash/output.txt", "UTF-8");
		   // writer.println(tempStream.toString());
		    writer.close();
		} catch (IOException e) {
		   // do something
		}
	}*/
	

	  
	    // Write the results to an attribute
	    
	   

	    // To write the results back out ot flow file
	    flowfile = session.write(flowfile, new OutputStreamCallback() {

	       
	        public void process(OutputStream out) throws IOException {
	        	out.write(value.get().getBytes());
	        }

			
	    });

	    session.transfer(flowfile,REL_SUCCESS);
	}

}
