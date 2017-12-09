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
import org.apache.edgent.function.Predicate;
import org.apache.edgent.providers.direct.DirectProvider;
import org.apache.edgent.topology.TStream;
import org.apache.edgent.topology.Topology;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
@SideEffectFree
@Tags({"Edgent","Filter","Range","Bloom"})
@CapabilityDescription("Edgent version of filters: Range -> Bloom -> Interpolation")
public class EdgentFilter_RBI extends AbstractProcessor implements Processor{
	
	public static final PropertyDescriptor BLOOMFILTER_FILEPATH = new PropertyDescriptor.Builder()
            .name("Bloom Filter File Path")
            .description("Full Path to Bloom Filter file")
            .required(true)
            .addValidator(StandardValidators.createURLorFileValidator())
            .expressionLanguageSupported(true)
            .build();
	final ComponentLog logger = getLogger();
	private String[] useMsgFieldList = {"trip_time_in_secs","trip_distance","fare_amount","tip_amount","surcharge"};
	
	private Set<Relationship> relationships;
	private List<PropertyDescriptor> properties;

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

	@Override
	public void init(final ProcessorInitializationContext context){
		final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
        
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(BLOOMFILTER_FILEPATH);
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
	
	 @Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		// TODO Auto-generated method stub
		System.out.println("Started Nifi");
		DirectProvider dp = new DirectProvider();
		
		Topology topology = dp.newTopology("Simple Edgent Application");
		final AtomicReference<String[]> value = new AtomicReference<String[]>();
		final AtomicReference<String> result =new AtomicReference<String>();
		FlowFile flowfile = session.get();

		openFile(context.getProperty(EXPERIMENT_LOG_FILE).getValue());
		expLogStream.println(String.format("%s,%s,%s,%s", flowfile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
				 EdgentFilter_RBI.class, (new Timestamp(System.currentTimeMillis())).getTime(), "ENTRY"));
        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in)  {
                try{
                	String prevResult = IOUtils.toString(in, "UTF-8");
                	
                	String[] jsonArr = prevResult.replaceAll("\"\"", "\"").split("\n");
                    value.set(jsonArr);
                }catch(Exception ex){
                    ex.printStackTrace();
                    getLogger().error("Failed to read json string.");
                    getLogger().error(ex.toString());
                }
            }
        });
		getLogger().info("Created Edgent Topology: "+topology.getName());
		TempSource source = new TempSource(value.get());
		
		TStream<Map<String,String>> sourceStream = topology.source(source);
		
		TStream<Map<String,String>> rangeStream = sourceStream.filter(new Predicate<Map<String,String>>() {
			@Override
			public boolean test(Map<String,String> map) {
				boolean isValid = true;
				isValid = Float.parseFloat(map.get("trip_time_in_secs").replaceAll("\"", "")) < 3155 && Float.parseFloat(map.get("trip_time_in_secs").replaceAll("\"", "")) > 140;
				isValid = Float.parseFloat(map.get("trip_distance").replaceAll("\"", "")) < 29.86 && Float.parseFloat(map.get("trip_distance").replaceAll("\"", "")) > 1.37;
				isValid = Float.parseFloat(map.get("fare_amount").replaceAll("\"", "")) < 201.00 && Float.parseFloat(map.get("fare_amount").replaceAll("\"", "")) > 6.00;
				isValid = Float.parseFloat(map.get("tip_amount").replaceAll("\"", "")) < 38.55 && Float.parseFloat(map.get("tip_amount").replaceAll("\"", "")) > 0.65;
				isValid = Float.parseFloat(map.get("surcharge").replaceAll("\"", "")) < 1.0 && Float.parseFloat(map.get("surcharge").replaceAll("\"", "")) > 0.1;
				return isValid;
				
			}
		});
//		System.out.println("Range Stream: ");
//		rangeStream.print();
		setup(context);
		TStream<Map<String,String>> bloomStream = rangeStream.filter(new Predicate<Map<String,String>>() {
			@Override
			public boolean test(Map<String,String> map) {
				boolean isValid = true;
				for(int i = 0; i< useMsgFieldList .length ; i++)
				{
					String obsType,obsVal;
					if( map.containsKey(useMsgFieldList[i]) )
					{
						obsType = useMsgFieldList[i];
						obsVal = map.get(useMsgFieldList[i]);
						bloomFilter = bloomFilterMap.get(obsType);
						isValid = bloomFilter.mightContain(obsVal);
					}
				}
				return isValid;
				
			}
		});
		//System.out.println("Bloom Stream: ");
		//bloomStream.print();
		Interpolation interpolation = new Interpolation();
		interpolation.setup();
		
		TStream<Map<String,String>> interpolStream = bloomStream.map(interpolation::doTaskLogic);
		
		interpolStream.sink(tuple -> update(tuple));
		
		dp.submit(topology);
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    result.set(resultString);
	    
	    // To write the results back out ot flow file
	    flowfile = session.write(flowfile, new OutputStreamCallback() {
	       public void process(OutputStream out) throws IOException {
	        	out.write(resultString.getBytes());
	        }
	    });
		 expLogStream.println(String.format("%s,%s,%s,%s", flowfile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
				 ETLGetFile.class, (new Timestamp(System.currentTimeMillis())).getTime(), "EXIT"));
		 expLogStream.flush();

	    session.transfer(flowfile,REL_SUCCESS);
	}
	private void update(Map<String, String> tuple) {
		// TODO Auto-generated method stub
		//System.out.println("String" + tuple);
		resultString += tuple.toString()+ "\n";
	}

	private static String bloomFilterFilePaths;
	private static int testingRange;
	private static HashMap <String,BloomFilter<String>> bloomFilterMap;
	BloomFilter<String> bloomFilter;
	private Logger l=LoggerFactory.getLogger("Edgent Filter");
	private static String resultString = "";
    public void setup(ProcessContext context) {
		try 
		{
			// If positive, use that particular field number in the input CSV message as input for count
			String useMsgList ="taxi_identifier";
			bloomFilterFilePaths=context.getProperty(BLOOMFILTER_FILEPATH).evaluateAttributeExpressions().getValue();//"/home/sivaprakash/Workspace/Edgent/bloomfilter_taxi_id.model";
			int expectedInsertions = 20000000;
			useMsgFieldList = useMsgList.split(",");
			String bloomFilterPathList[] = bloomFilterFilePaths.split(",");
		
			// Check to enable matching of bloom filter model files and fields provided in property file
			if(useMsgFieldList.length != bloomFilterPathList.length) {
				return;
			}
			bloomFilterMap = new HashMap<String,BloomFilter<String>>();
			testingRange = expectedInsertions;
			
			/// Populating bloom filter for each model
			for(int i = 0; i < useMsgFieldList.length ;i++) {
			//Load BloomFilter from serialized file
			bloomFilter  = readBloomFilterFromfile(bloomFilterPathList[i]);
			if(bloomFilter == null) {
			
				return;
			}
			bloomFilterMap.put(useMsgFieldList[i], bloomFilter);
			}	
		} catch (Exception e) {
		
		}
			
	}
   
    static BloomFilter<String> readBloomFilterFromfile(String bloomFilterFilePath) throws IOException {
 		Funnel<String> memberFunnel = new Funnel<String>() {
 			public void funnel(String memberId, PrimitiveSink sink) {
 				sink.putString(memberId, Charsets.UTF_8);
 			}
 		};
 		try
 		{
 			FileInputStream fis = new FileInputStream(new File(bloomFilterFilePath));
 			return BloomFilter.readFrom(fis, memberFunnel);
 		}
 		catch(Exception e)
 		{
 			e.printStackTrace();
 		}
 		return null;
 	}
    
}
