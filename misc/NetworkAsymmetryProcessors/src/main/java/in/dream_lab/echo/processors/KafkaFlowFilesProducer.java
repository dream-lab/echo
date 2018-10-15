package in.dream_lab.echo.processors;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "NetworkAsymmetryKafkaProducer" })
@CapabilityDescription("This will act as the publisher when doing broker indirection experiment")
public class KafkaFlowFilesProducer extends AbstractProcessor {

	public static final PropertyDescriptor KAFKA_TOPIC = new PropertyDescriptor.Builder().name("topic")
			.displayName("Kafka topic").description("The kafka topic to produce to").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor KAFKA_BROKER_IP = new PropertyDescriptor.Builder().name("brokerip")
			.displayName("Kafka Broker IP").description("The IP Address of kafka broker").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor EXPERIMENT_LOG_FILE = new PropertyDescriptor.Builder()
			.name("Experiment Log File").displayName("Experiment Log File")
			.description("File path to wherever the experiment logs are written").required(false)
			.addValidator(Validator.VALID).defaultValue("/exp_logs/exp_producer.log").build();

	public static final Relationship SUCCESS = new Relationship.Builder().name("success").description("On success")
			.autoTerminateDefault(true).build();
	
	// ALPR specific
	public static final String TOPIC_TYPE = "entry_exit";

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;
	private String topic;
	private String brokerIp;
	private Properties props;
	private KafkaProducer<String, byte[]> producer;
	private PrintWriter expLogStream;

	private void openFile(String filename) {
		if (expLogStream == null)
			try {
				expLogStream = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(KAFKA_TOPIC);
		descriptors.add(KAFKA_BROKER_IP);
		descriptors.add(EXPERIMENT_LOG_FILE);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(SUCCESS);
		this.relationships = Collections.unmodifiableSet(relationships);

	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		topic = context.getProperty(KAFKA_TOPIC).getValue();
		brokerIp = context.getProperty(KAFKA_BROKER_IP).getValue();
		props = new Properties();
		String conn = brokerIp; //do not attach port, its attached from the platform service
		props.put("bootstrap.servers", conn);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put("retries", 0);
		// props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		// props.put("buffer.memory", 158589830);
		// props.put("max.request.size", 158589830);

		producer = new KafkaProducer<String, byte[]>(props);
	}

	/* (non-Javadoc)
	 * The producer will publish the message in the following format
	 * long (flowFileId) followed by integer (length of topic type) 
	 * followed by actual string for topic type followed by the
	 * actual byte array for the image
	 */
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if(flowFile == null)
			return;
		openFile(context.getProperty(EXPERIMENT_LOG_FILE).getValue());
		final long flowFileId = Long.parseLong(flowFile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()));
		//ALPR specific
		String topicType = flowFile.getAttribute(TOPIC_TYPE);
		int topicTypeLength = topicType.length();
		expLogStream.println(String.format("%s,%s,%s,%s", flowFile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
				KafkaFlowFilesProducer.class, (new Timestamp(System.currentTimeMillis())).getTime(), "ENTRY"));
		session.read(flowFile, new InputStreamCallback() {

			@Override
			public void process(InputStream in) throws IOException {
				byte[] imageContent = IOUtils.toByteArray(in);
				ByteBuffer idBuf = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + topicTypeLength);
				idBuf.putLong(flowFileId);
				idBuf.putInt(topicTypeLength);
				idBuf.put(topicType.getBytes());
				ByteBuffer buffer = ByteBuffer.allocate(idBuf.capacity() + imageContent.length);
				buffer.put(idBuf.array(), 0, idBuf.capacity());
				buffer.put(imageContent, 0, buffer.capacity() - idBuf.capacity());
				ProducerRecord<String, byte[]> record = 
						new ProducerRecord<String, byte[]>(topic, "test", buffer.array());
				producer.send(record, new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if (exception != null) {
							exception.printStackTrace();
						} else {
							System.out.println(
									"The record is produced with " + metadata.topic() + " offset " + metadata.offset());
						}
					}
				});

			}
		});
		expLogStream.println(String.format("%s,%s,%s,%s", flowFile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
				KafkaFlowFilesProducer.class, (new Timestamp(System.currentTimeMillis())).getTime(), "EXIT"));
		expLogStream.flush();
		// session.transfer(flowFile, SUCCESS);
		session.remove(flowFile);

	}

}
