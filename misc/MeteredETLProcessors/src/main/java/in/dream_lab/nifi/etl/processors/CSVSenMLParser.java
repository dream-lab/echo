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
import java.util.concurrent.atomic.AtomicReference;

import in.dream_lab.nifi.etl.processors.attributes.ExperimentAttributes;
import org.apache.commons.io.IOUtils;
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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;



@SideEffectFree
@Tags({"CSV2SenML"})
@CapabilityDescription("CSV to SenML Parser")
public class CSVSenMLParser extends AbstractProcessor implements Processor{
	public static final PropertyDescriptor SCHEMA_FILEPATH = new PropertyDescriptor.Builder()
            .name("Annotation Schema File Path")
            .description("Full Path to Annotation Schema file")
            .required(true)
            .addValidator(StandardValidators.createURLorFileValidator())
            .expressionLanguageSupported(true)
            .build();
	
	private static int timestampField;
	/* for task benchmarking */
	private String sampledata = "";
	private static int useMsgField;
	/* hashmap of schema in the form fieldnum as key and  
	 * coulumn name,unit and type of data separated by comma as value*/
	private static HashMap<Integer, String> schemaMap = new HashMap<Integer, String>();

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

	public void prepare()
	{
		FileReader reader;
		BufferedReader br;
		String column, unit, type;
		String [] columns, units, types;
		//String schemaFilePath = "/home/sivaprakash/Workspace/Edgent/taxi-schema-without-annotation.csv";
		useMsgField =0;
		try 
		{
			reader = new FileReader(schemaFilePath);
			br = new BufferedReader(reader);
			column = br.readLine();
			unit = br.readLine();
			type = br.readLine();
			columns = column.split(",");
			units = unit.split(",");
			types = type.split(",");
			br.close();
			reader.close();
			long bt;
			for(int i = 0; i< columns.length ;i++)
			{
				if(columns[i].equals("timestamp"))	
					timestampField = i;
				schemaMap.put(i,columns[i]+","+units[i]+","+types[i]);
			}
		
		} catch (Exception e) 
		{
			e.printStackTrace();
		}
	sampledata = "024BE2DFD1B98AF1EA941DEDA63A15CB,9F5FE566E3EE57B85B723B71E370154C,2013-01-14 03:57:00,2013-01-14 04:23:00,200,10,-73.953178,40.776016,-73.779190,40.645145,CRD,52.00,0.00,0.50,13.00,4.80,70.30,uber,sam,Houston";
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

	private String schemaFilePath;
	
	 @Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		schemaFilePath=context.getProperty(SCHEMA_FILEPATH).evaluateAttributeExpressions().getValue();
		prepare();
		System.out.println("Started Nifi");
		
		final AtomicReference<String[]> value = new AtomicReference<String[]>();
		final AtomicReference<String> result =new AtomicReference<String>();
		FlowFile flowfile = session.get();
		 openFile(context.getProperty(EXPERIMENT_LOG_FILE).getValue());
		 expLogStream.println(String.format("%s,%s,%s,%s", flowfile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
				 CSVSenMLParser.class, (new Timestamp(System.currentTimeMillis())).getTime(), "ENTRY"));
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
		String[] arr;
		for(String jsonString:value.get()) {
			Map<String,String> sourceMap =  new HashMap<String,String>();
			jsonString = jsonString.replace("{", "");
			jsonString = jsonString.replace("}", "");
			String s1[] = jsonString.split("=");
			sourceMap.put(s1[0].trim(), s1[1].trim());
			mapList.add(sourceMap);
		}
		List<String> resultList = new ArrayList<String>();
		for(Map<String,String> map:mapList)
		{
			String res = doTaskLogic(map.get("D"));
			resultList.add(res);
		}
		String resultString="";
		for(String res:resultList)
		{
			resultString += res+"\n";
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
	
	   
	
	protected String doTaskLogic(String in) 
	{
		// reading the meta and obsvalue from string
		JsonObject obj;
		JsonArray jsonArr = new JsonArray();
		JsonObject finalSenML;
		try
		{
			String m ;
			if(useMsgField == -1)
				m= sampledata;
			else
				m = in;
			String [] val = m.split(",");
			String[] sch;
			jsonArr = new JsonArray();
			val = m.split(",");
			
			finalSenML = new JsonObject();
			finalSenML.addProperty("bt",val[timestampField] );
			for(int i = 0; i< schemaMap.size(); i++)
			{  	
				sch = ((String)schemaMap.get(i)).split(",");
				if(i != timestampField)
				{
					obj = new JsonObject();
					obj.addProperty("n", sch[0]);
					obj.addProperty(sch[2], val[i]);
					obj.addProperty("u", sch[1]);
					jsonArr.add(obj);
				}
			}
			finalSenML.addProperty("e", jsonArr.toString());
			return finalSenML.toString();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return null;
	
	}
}
