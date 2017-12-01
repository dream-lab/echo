package in.dream_lab.nifi.etl.processors;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import in.dream_lab.nifi.etl.processors.attributes.ExperimentAttributes;
import org.apache.commons.io.IOUtils;
import org.apache.edgent.providers.direct.DirectProvider;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
@SideEffectFree
@Tags({"JoinETL"})
@CapabilityDescription("Join task of ETL")
public class JoinETL extends AbstractProcessor implements Processor{
	final ComponentLog logger = getLogger();
	public static final PropertyDescriptor SCHEMA_FILEPATH = new PropertyDescriptor.Builder()
            .name("Join Schema File Path")
            .description("Full Path to Join Schema file")
            .required(true)
            .addValidator(StandardValidators.createURLorFileValidator())
            .expressionLanguageSupported(true)
            .build();
	
	private int maxCountPossible = 17;
    private HashMap<Long,HashMap<String, String>> msgIdCountMap ;
    private ArrayList<String> schemaFieldOrderList ;
    private String schemaFieldOrderFilePath;// = "/home/sivaprakash/Workspace/Edgent/taxi-schema-without-annotation.csv";
    private String  [] metaFields;

	public static final PropertyDescriptor EXPERIMENT_LOG_FILE = new PropertyDescriptor.Builder()
			.name("Experiment Log File")
			.description("File path to wherever the experiment logs are written")
			.required(true)
			.addValidator(Validator.VALID)
			.defaultValue("~/experiment.log")
			.build();

	PrintWriter expLogStream;

	private void openFile(String filename) {
		if (expLogStream == null)
			try {
				expLogStream = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

    public void prepare() {
    	
    	String metaField = "pickup_datetime@pickup_longitude@pickup_latitude@dropoff_longitude@dropoff_latitude@payment_type";
        metaFields = metaField.split("@");


        msgIdCountMap = new  HashMap<Long,HashMap<String, String>>();
        schemaFieldOrderList = new ArrayList<String>();
        
        /* reading the schema field order into a list to maintain values ordering after join */
    	try 
    	{
    		FileReader reader = new FileReader(schemaFieldOrderFilePath);
    		BufferedReader br = new BufferedReader(reader);
    		String [] values;
    		String line = br.readLine();
    		if(line != null)
    		{
    			values = line.split(",");
    			for(String s : values)
    				schemaFieldOrderList.add(s);
    			line = br.readLine();
    		}
		} 
    	catch (Exception e)
    	{
			e.printStackTrace();
		}
		
    }
	@Override
	public void init(final ProcessorInitializationContext context){
		final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
         this.relationships = Collections.unmodifiableSet(relationships);

         final List<PropertyDescriptor> properties = new ArrayList<>();
         properties.add(SCHEMA_FILEPATH);
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
             .description("Filter successful")
             .build();
	 private Set<Relationship> relationships;
	 private List<PropertyDescriptor> properties;
	
	 @Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		 schemaFieldOrderFilePath=context.getProperty(SCHEMA_FILEPATH).evaluateAttributeExpressions().getValue();
		 prepare();
		// TODO Auto-generated method stub
		System.out.println("Started Nifi");
		
		final AtomicReference<String[]> value = new AtomicReference<String[]>();
		final AtomicReference<String> result =new AtomicReference<String>();
		FlowFile flowfile = session.get();
		 openFile(context.getProperty(EXPERIMENT_LOG_FILE).getValue());
		 expLogStream.println(String.format("%s,%s,%s,%s", flowfile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
				 JoinETL.class, (new Timestamp(System.currentTimeMillis())).getTime(), "ENTRY"));
        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in)  {
                try{
                	
                	String prevResult = IOUtils.toString(in, "UTF-8");
                  	String[] jsonArr = prevResult.split("\n");
                    value.set(jsonArr);
                }catch(Exception ex){
                	ex.printStackTrace();
                    getLogger().error("Failed to read json string.");
                    getLogger().error(ex.toString());
                }
            }
        });
		
        List<Map<String,String>> mapList = new ArrayList<Map<String,String>>();
		JsonParser parser = new JsonParser(); 
		JsonObject json ;
		for(String jsonString:value.get()){
			Map<String,String> sourceMap =  new HashMap<String,String>();
			json = (JsonObject)  parser.parse(jsonString);
			sourceMap.put("SENSORID", json.get("SENSORID").toString());
			sourceMap.put("taxi_identifier", json.get("SENSORID").toString());
			sourceMap.put("trip_time_in_secs", json.get("trip_time_in_secs").toString());
			sourceMap.put("trip_distance", json.get("trip_distance").toString());
			sourceMap.put("fare_amount", json.get("fare_amount").toString());
			sourceMap.put("tip_amount", json.get("tip_amount").toString());
			sourceMap.put("surcharge", json.get("surcharge").toString());
			int randomNum = ThreadLocalRandom.current().nextInt(1, 10000 + 1);
			sourceMap.put("MSGID", randomNum+"");
			mapList.add(sourceMap);
		}
		List<Map<String,String>> resultmapList = new ArrayList<Map<String,String>>();
		for(Map map:mapList)
		{
			
			resultmapList.add(execute(map));
		}
		String resultString="";
		for(Map map:resultmapList)
		{
			resultString += map.toString()+"\n";
		}
		resultString.trim();
	    result.set(resultString);
	    
	    // To write the results back out ot flow file
	    flowfile = session.write(flowfile, new OutputStreamCallback() {
	       public void process(OutputStream out) throws IOException {
	        	out.write(result.get().getBytes());
	        }
	    });

		 expLogStream.println(String.format("%s,%s,%s,%s", flowfile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
				 ETLGetFile.class, (new Timestamp(System.currentTimeMillis())).getTime(), "EXIT"));
		 expLogStream.flush();

	    session.transfer(flowfile,REL_SUCCESS);
	}
	 
	    public HashMap execute(Map inputMap) 
	    {   prepare();
	    	String msgId = (String) inputMap.get("MSGID");
	    	String meta = "pickup_datetime@pickup_longitude@pickup_latitude@dropoff_longitude@dropoff_latitude@payment_type";
	    	Set<Map.Entry<String, String>> entrySet = inputMap.entrySet();
	    	HashMap<String, String> outmap = new HashMap<String, String>() ;
	    	outmap.put("MSGID",msgId);
			outmap.put("META",meta);
	    	for (Entry<String, String> entry : entrySet) 
	    	{
	    		String obsType = entry.getKey();
		    	String obsVal =entry.getValue();
		    	HashMap map ; 
		    	 	/* if message id is present in hashmap, update */
		    	Long msgIdLong = Long.parseLong(msgId);
	   	       
	    	
	    	if(msgIdCountMap.containsKey(msgIdLong) == true)
	    	{   
	    		map = (HashMap)msgIdCountMap.get(msgIdLong);
	    		map.put(obsType, obsVal);
	       		msgIdCountMap.put(msgIdLong, map);
	    		if(map.size() <= maxCountPossible)
	    		{
	    			/* emit the msg as it has received all its field values also maintaining schema order*/
	    			StringBuilder joinedValues = new StringBuilder();
	    			for(String s : schemaFieldOrderList)
	    			{
	    				joinedValues.append((String)inputMap.get(s)).append("@"); 
	    			}
	    			joinedValues = joinedValues.deleteCharAt(joinedValues.length()-1);
	    			msgIdCountMap.remove(msgIdLong);
	    			
	    			outmap.put("joinedValue", joinedValues.toString());
	    		}
	    	}
	    	/*else add the msgId and create an hashmap for the incoming msg id */
	    	else
	    	{   
	    		map = new HashMap<String, String>();
	    		map.put(obsType, obsVal);
	    		
	    		/* split the meta fields and add it to hash map 
	    		 * to merge back into csv. This is done once only for a msg Id */
	    		String [] metaVal = meta.split("@");
	    		for(int i = 0; i< metaVal.length ; i++ )
	    		{
	    			 
	    			map.put(metaFields[i], metaVal[i]);
	    			
	    		}
	    		
	    		msgIdCountMap.put(msgIdLong, map);
	    		
	    		if(map.size() <= maxCountPossible)
	    		{
	    			/* emit the msg as it has received all its field values also maintaining schema order*/
	    			StringBuilder joinedValues = new StringBuilder();
	    			for(String s : schemaFieldOrderList)
	    			{
	    				joinedValues.append((String)inputMap.get(s)).append("@"); 
	    			}
	    			joinedValues = joinedValues.deleteCharAt(joinedValues.length()-1);
	    			msgIdCountMap.remove(msgIdLong);

	    			outmap.put("joinedValue", joinedValues.toString());
	    		}
	    		
	    	}
	    	
	    	}
			return outmap;
	    }

	   
    
}
