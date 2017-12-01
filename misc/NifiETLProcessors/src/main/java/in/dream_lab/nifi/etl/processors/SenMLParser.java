package in.dream_lab.nifi.etl.processors;

import java.io.*;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import in.dream_lab.nifi.etl.processors.attributes.ExperimentAttributes;
import jdk.nashorn.internal.runtime.Property;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
@SideEffectFree
@Tags({"ETL","SenMLParse"})
@CapabilityDescription("SenML Parser")
public class SenMLParser extends AbstractProcessor implements Processor{


	public static final PropertyDescriptor EXPERIMENT_LOG_FILE = new PropertyDescriptor.Builder()
			.name("Experiment Log File")
			.description("File path to wherever the experiment logs are written")
			.required(true)
			.addValidator(Validator.VALID)
			.defaultValue("~/experiment.log")
			.build();

    PrintWriter expLogStream;
    private List<PropertyDescriptor> properties;

    private void openFile(String filename) {
        if (expLogStream == null)
            try {
                expLogStream = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

	@Override
	public void init(final ProcessorInitializationContext context){
		final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(EXPERIMENT_LOG_FILE);
        this.properties = Collections.unmodifiableList(properties);
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
             .description("Successful parse result")
             .build();
	 private Set<Relationship> relationships;

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

		final AtomicReference<String[]> value = new AtomicReference<String[]>();
		final AtomicReference<String> result =new AtomicReference<String>();
		HashMap<String, String> map = new HashMap<String, String>(); 
        FlowFile flowfile = session.get();
        openFile(context.getProperty(EXPERIMENT_LOG_FILE).getValue());
        expLogStream.println(String.format("%s,%s,%s,%s", flowfile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
                SenMLParser.class, (new Timestamp(System.currentTimeMillis())).getTime(), "ENTRY"));

        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in)  {
                try{
                	String json = IOUtils.toString(in, "UTF-8");
                	String[] obsType = json.split(System.getProperty("line.separator"));
                	value.set(obsType);
                                        
                }catch(Exception ex){
                    ex.printStackTrace();
                    getLogger().error("Failed to read json string.");
                    getLogger().error(ex.toString());
                }
            }
        });
        String partResult = "";
		for(String msg:value.get()) {
           Map<String, String> parsedMap =  getResult(msg.split(",",2)[1]);
           partResult += parsedMap.toString()+"\n";           
		}
		partResult.trim();
        result.set(partResult);
        // To write the results back out to flow file
	    flowfile = session.write(flowfile, new OutputStreamCallback() {
	    
	    	public void process(OutputStream out) throws IOException {
	        	out.write(result.toString().getBytes());
	    		//out.write(value.get().getBytes());
	        }
			
	    });
        expLogStream.println(String.format("%s,%s,%s,%s", flowfile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
                ETLGetFile.class, (new Timestamp(System.currentTimeMillis())).getTime(), "EXIT"));
        expLogStream.flush();

	    session.transfer(flowfile,REL_SUCCESS);
	}
	public Map<String, String> getResult(String m)
	{
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObject ; 
		Map<String, String> mapkeyValues = new HashMap<String, String>();
		try 
		{
			jsonObject =  (JsonObject) jsonParser.parse(m);
			long baseTime =   Long.parseLong((jsonObject.get("bt") == null  ? 0L : jsonObject.get("bt")).toString()) ; // for sys and taxi
			//long baseTime =   Long.parseLong(((String)jsonObject.get("bt"))) ;     // for fit dataset
			//String baseUnit = (( jsonObject.get("bu") == null ) ? null : jsonObject.get("bu") ).toString();
			//String baseName = (( jsonObject.get("bn") == null ) ? null : jsonObject.get("bn") ).toString();
			JsonArray jsonArr = (JsonArray) jsonObject.get("e");
			Object v;
			String n, u;
			long t;
			
			mapkeyValues.put("timestamp", String.valueOf(baseTime));
			//getLogger().debug("JSON Array: "+jsonArr.toString());
			for(int j=0; j<jsonArr.size(); j++)
			{
				jsonObject = (JsonObject) jsonArr.get(j);
				
				v = (jsonObject.get("v") == null ) ?  jsonObject.get("sv").toString() : jsonObject.get("v").toString();				

				t = (jsonObject.get("t") == null) ? 0: Long.parseLong(jsonObject.get("t").toString());
				
				t = t+ baseTime;
				
				/* if name does not exist, consider base name */
				n = (jsonObject.get("n") == null) ? "" :(String) jsonObject.get("n").toString();					

				u = (jsonObject.get("u") == null) ? "" :(String) jsonObject.get("u").toString();

				/* Add to  Hashmap  each key value pair */
				mapkeyValues.put(n, v.toString());			
			}
			
		}
		catch (Exception e)
		 {
			getLogger().error("Parse JSON String: "+e);
			e.printStackTrace();
		 }
		return mapkeyValues; 
	}
}
