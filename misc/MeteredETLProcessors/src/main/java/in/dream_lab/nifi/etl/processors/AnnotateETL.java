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


@SideEffectFree
@Tags({"Annotate"})
@CapabilityDescription("Annotate task of ETL")
public class AnnotateETL extends AbstractProcessor implements Processor{
	final ComponentLog logger = getLogger();
	public static final PropertyDescriptor META_FILEPATH = new PropertyDescriptor.Builder()
            .name("Metadata File Path")
            .description("Full Path Metadata file")
            .required(true)
            .addValidator(StandardValidators.createURLorFileValidator())
            .expressionLanguageSupported(true)
            .build();
	private static Map<String, String> annotationMap;
	private static String filePath ; 
	private static int useMsgField;

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
		annotationMap = new HashMap<String, String>();
			// Check usefield 
			useMsgField = 0;
			// read file path for anotations
			// read file content 
			FileReader reader;
			try {
				reader = new FileReader(filePath);
				BufferedReader br = new BufferedReader(reader);
			    String s ;
			    String [] annotation ;
			    // populate hashmap with anotations
			    while((s = br.readLine()) != null)
				{
					annotation = s.split(":");
					assert annotation.length == 2;
					annotationMap.put(annotation[0], annotation[1]);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				getLogger().error("Error in Annotate Setup");
				e.printStackTrace();
			} 
			
		    		
	}

	
	@Override
	public void init(final ProcessorInitializationContext context){
		final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
         this.relationships = Collections.unmodifiableSet(relationships);

         final List<PropertyDescriptor> properties = new ArrayList<>();
         properties.add(META_FILEPATH);
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
		// TODO Auto-generated method stub
		System.out.println("Started Nifi");
		 filePath=context.getProperty(META_FILEPATH).evaluateAttributeExpressions().getValue();
		 prepare();
		
		final AtomicReference<String[]> value = new AtomicReference<String[]>();
		final AtomicReference<String> result =new AtomicReference<String>();
		FlowFile flowfile = session.get();
		openFile(context.getProperty(EXPERIMENT_LOG_FILE).getValue());
		expLogStream.println(String.format("%s,%s,%s,%s", flowfile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
                AnnotateETL.class, (new Timestamp(System.currentTimeMillis())).getTime(), "ENTRY"));
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
			arr = jsonString.split(", ");
			for(String s:arr)
			{
				String s1[] = s.split("=");
				sourceMap.put(s1[0].trim(), s1[1].trim());
			}
			mapList.add(sourceMap);
		}
		List<Map<String,String>> resultmapList = new ArrayList<Map<String,String>>();
		for(Map<String,String> map:mapList)
		{
			Map<String,String> outmap = new HashMap<String, String>();
			outmap.put("D", doTaskLogic(map.get("joinedValue")));
			resultmapList.add(outmap);
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
	
	   
	
	protected String doTaskLogic(String in) 
	{
		// reading the meta and obsvalue from string
		
		String annotateKey;
		String annotatedValue = in;
		
		annotateKey = in.split("@")[useMsgField];
		
		// Fetch annotated values from hashmap corresponding to this key 
		String annotation = annotationMap.get(annotateKey);
		if(annotation != null)
		{
		   annotatedValue = annotatedValue+","+annotation;
		}
			return annotatedValue.replace("@", ",");
	
	}
}
