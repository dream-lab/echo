package in.dream_lab.nifi.etl.processors;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import in.dream_lab.nifi.etl.processors.attributes.ExperimentAttributes;
import org.apache.commons.io.IOUtils;
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

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableServiceEntity;


public class InsertAzure extends AbstractProcessor implements Processor
{	
	private static String storageConnStr ;
	private static String tableName ;
	private static boolean doneSetup = false;
	private static int useMsgField;
	private  ComponentLog logger = getLogger();
	private CloudTable table ;
	private String sampleData;  
	public static final PropertyDescriptor AZURE_STORAGE_CONN_STR = new PropertyDescriptor.Builder()
            .name("Azure storage Connection String")
            .description("Azure storage Connection String")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
	public static final PropertyDescriptor AZURE_TABLE_TABLE_NAME= new PropertyDescriptor.Builder()
            .name("Azure Table Name")
            .description("Table Name")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

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
     properties.add(AZURE_STORAGE_CONN_STR);
     properties.add(AZURE_TABLE_TABLE_NAME);
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
		setup(context);
		System.out.println("Started Nifi");
		
		final AtomicReference<String[]> value = new AtomicReference<String[]>();
		final AtomicReference<String> result =new AtomicReference<String>();
		FlowFile flowfile = session.get();
		 openFile(context.getProperty(EXPERIMENT_LOG_FILE).getValue());
		 expLogStream.println(String.format("%s,%s,%s,%s", flowfile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
				 InsertAzure.class, (new Timestamp(System.currentTimeMillis())).getTime(), "ENTRY"));
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
			Boolean res = doTaskLogic(map);
			resultList.add(res.toString());
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
	public void setup(ProcessContext context) 
	{
		try {
			if(!doneSetup)
			{ 
				storageConnStr = context.getProperty(AZURE_STORAGE_CONN_STR).evaluateAttributeExpressions().getValue(); 
				tableName = context.getProperty(AZURE_TABLE_TABLE_NAME).evaluateAttributeExpressions().getValue();
				useMsgField = 0; 
				doneSetup=true;
			}
			table = connectToAzTable(storageConnStr,tableName );
			/* added for use in benchmarking task with sample data */
			sampleData = "024BE2DFD1B98AF1EA941DEDA63A15CB,9F5FE566E3EE57B85B723B71E370154C,2013-01-14 03:57:00,1358117580000,-1,0,-73.953178,40.776016,-73.779190,40.645145,CRD,52.00,11.00,0.50,13.00,4.80,70.30,uber,sam,Houston";
		
		}
		catch(Exception e) {
			getLogger().error("Error in Connecting to Azure: "+e.getStackTrace().toString());
		}
	}

	
	protected boolean doTaskLogic(Map map) 
	{
		try 
		{
			String m = (String)map.get("D");
			TaxiTrip taxiEntity = new TaxiTrip();
			if (useMsgField == -1)
				taxiEntity.parseString(sampleData);
			else
				taxiEntity.parseString(m);
			
			// Create an operation to add the new entity to the taxi data table.
//			TableOperation operation = TableOperation.insertOrReplace(taxiEntity);
			
			TableBatchOperation batchOperation = new TableBatchOperation();
			batchOperation.insertOrReplace(taxiEntity);
			// Submit the operation to the table service.
			table.execute(batchOperation);
			System.out.println("Execute succ");
		}
			catch (StorageException e) 
			{
				e.printStackTrace();
			}
			return true;
	}
	
	/***
	 *
	 * @param azStorageConnStr
	 * @param tableName
	 * @param l
    * @return
    */
	public CloudTable connectToAzTable(String azStorageConnStr, String tableName) {
		CloudTable cloudTable = null;
		try {
			// Retrieve storage account from connection-string.
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(azStorageConnStr);

			// Create the table client
			CloudTableClient tableClient = storageAccount.createCloudTableClient();

			// Create a cloud table object for the table.
			cloudTable = tableClient.getTableReference(tableName);
			
		} catch (Exception e) 
		{
			logger.warn("Exception in connectToAzTable: "+tableName, e);
		}
		return cloudTable;
	}
	
	
	public static  final class TaxiTrip extends TableServiceEntity
	{
		private String taxi_identifier,hack_license, pickup_datetime,drop_datetime; 
		public String getDrop_datetime() {
			return drop_datetime;
		}

		public void setDrop_datetime(String drop_datetime) {
			this.drop_datetime = drop_datetime;
		}
		private double trip_time_in_secs,trip_distance;
		private String pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type;
		private double fare_amount,surcharge,mta_tax,tip_amount,tolls_amount, total_amount;
		private String company, driver, city;
		public String getCompany() {
			return company;
		}

		public void setCompany(String company) {
			this.company = company;
		}

		public String getDriver() {
			return driver;
		}

		public void setDriver(String driver) {
			this.driver = driver;
		}

		public String getCity() {
			return city;
		}

		public void setCity(String city) {
			this.city = city;
		}

		public String getTaxi_identifier() {
			return taxi_identifier;
		}

		public void setTaxi_identifier(String taxi_identifier) {
			this.taxi_identifier = taxi_identifier;
		}

		public String getHack_license() {
			return hack_license;
		}

		public void setHack_license(String hack_license) {
			this.hack_license = hack_license;
		}

		public String getPickup_datetime() {
			return pickup_datetime;
		}

		public void setPickup_datetime(String l) {
			this.pickup_datetime = l;
		}
		
		public double getTrip_time_in_secs() {
			return trip_time_in_secs;
		}

		public void setTrip_time_in_secs(double trip_time_in_secs) {
			this.trip_time_in_secs = trip_time_in_secs;
		}

		public double getTrip_distance() {
			return trip_distance;
		}

		public void setTrip_distance(double trip_distance) {
			this.trip_distance = trip_distance;
		}

		public String getPickup_longitude() {
			return pickup_longitude;
		}
		public void setPickup_longitude(String pickup_longitude) {
			this.pickup_longitude = pickup_longitude;
		}

		public String getPickup_latitude() {
			return pickup_latitude;
		}

		public void setPickup_latitude(String pickup_latitude) {
			this.pickup_latitude = pickup_latitude;
		}

		public String getDropoff_longitude() {
			return dropoff_longitude;
		}

		public void setDropoff_longitude(String dropoff_longitude) {
			this.dropoff_longitude = dropoff_longitude;
		}

		public String getDropoff_latitude() {
			return dropoff_latitude;
		}

		public void setDropoff_latitude(String dropoff_latitude) {
			this.dropoff_latitude = dropoff_latitude;
		}

		public String getPayment_type() {
			return payment_type;
		}
		public void setPayment_type(String payment_type) {
			this.payment_type = payment_type;
		}
		
		public double getFare_amount() {
			return fare_amount;
		}
		public void setFare_amount(double fare_amount) {
			this.fare_amount = fare_amount;
		}

		public double getSurcharge() {
			return surcharge;
		}

		public void setSurcharge(double surcharge) {
			this.surcharge = surcharge;
		}

		public double getMta_tax() {
			return mta_tax;
		}

		public void setMta_tax(double mta_tax) {
			this.mta_tax = mta_tax;
		}

		public double getTip_amount() {
			return tip_amount;
		}

		public void setTip_amount(double tip_amount) {
			this.tip_amount = tip_amount;
		}

		public double getTolls_amount() {
			return tolls_amount;
		}

		public void setTolls_amount(double tolls_amount) {
			this.tolls_amount = tolls_amount;
		}

		public double getTotal_amount() {
			return total_amount;
		}

		public void setTotal_amount(double total_amount) {
			this.total_amount = total_amount;
		}
		
		public void parseString(String s)
		{
			String fields[] = s.split(",");
			this.rowKey = fields[0];
			this.partitionKey = fields[1];
			this.setTaxi_identifier(fields[0]);
			this.setHack_license(fields[1]);
			this.setPickup_datetime(fields[2]);
			this.setDrop_datetime(fields[3]);
			if(fields[4].replaceAll("\"", "")!=null)this.setTrip_time_in_secs(Double.parseDouble(fields[4].replaceAll("\"", "").replaceAll("null", "0")));
			if(fields[5].replaceAll("\"", "")!=null)this.setTrip_distance(Double.parseDouble(fields[5].replaceAll("\"", "").replaceAll("null", "0")));
			this.setPickup_longitude(fields[6]);
			this.setPickup_latitude(fields[7]);
			this.setDropoff_longitude(fields[8]);
			this.setDropoff_latitude(fields[9]);
			this.setPayment_type(fields[10]);
			if(fields[11].replaceAll("\"", "")!=null)this.setFare_amount(Double.parseDouble(fields[11].replaceAll("\"", "").replaceAll("null", "0")));
			if(fields[12].replaceAll("\"", "")!=null)this.setSurcharge(Double.parseDouble(fields[12].replaceAll("\"", "").replaceAll("null", "0")));
			if(fields[13].replaceAll("\"", "")!=null)this.setMta_tax(Double.parseDouble(fields[13].replaceAll("\"", "").replaceAll("null", "0")));
			if(fields[14].replaceAll("\"", "")!=null)this.setTip_amount(Double.parseDouble(fields[14].replaceAll("\"", "").replaceAll("null", "0")));
			if(fields[15].replaceAll("\"", "")!=null)this.setTolls_amount(Double.parseDouble(fields[15].replaceAll("\"", "").replaceAll("null", "0")));
			if(fields[16].replaceAll("\"", "")!=null)this.setTotal_amount(Double.parseDouble(fields[16].replaceAll("\"", "").replaceAll("null", "0")));
			if(fields.length == 20) {
				this.setCompany(fields[17]);
				this.setDriver(fields[18]);
				this.setCity(fields[19]);
			}
			
		}
	}


	
}
