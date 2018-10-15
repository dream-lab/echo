package in.dream_lab.echo.processors;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "NetworkAsymmetryKafkaConsumer" })
@CapabilityDescription("This will act as the consumer when doing broker indirection experiment")
public class KafkaFlowFilesConsumer extends AbstractProcessor {

	public static final PropertyDescriptor KAFKA_TOPIC = new PropertyDescriptor.Builder().name("topic")
			.displayName("Kafka topic").description("The kafka topic to consume from").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor CONSUMER_GROUP = new PropertyDescriptor.Builder().name("group_name")
			.displayName("Consumer group").description("The consumer group").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor KAFKA_BROKER_IP = new PropertyDescriptor.Builder().name("brokerip")
			.displayName("Kafka Broker IP").description("The IP Address of kafka broker").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	public static final PropertyDescriptor EXPERIMENT_LOG_FILE = new PropertyDescriptor.Builder()
			.name("Experiment Log File")
			.displayName("Experiment Log File")
			.description("File path to wherever the experiment logs are written")
			.required(false)
			.addValidator(Validator.VALID)
			.defaultValue("/exp_logs/exp_consumer.log")
			.build();

	public static final Relationship SUCCESS = new Relationship.Builder().name("success").description("On success")
			.autoTerminateDefault(false).build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;
	private String topic;
	private String consumerGroup;
	private String brokerIp;
	private Properties props;
	private KafkaConsumer<String, byte[]> consumer;
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
		descriptors.add(CONSUMER_GROUP);
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
		this.topic = context.getProperty(KAFKA_TOPIC).getValue();
		this.consumerGroup = context.getProperty(CONSUMER_GROUP).getValue();
		this.brokerIp = context.getProperty(KAFKA_BROKER_IP).getValue();
		this.props = new Properties();
		String conn = this.brokerIp;  //do not attach port, its attached from the platform service
		props.put("bootstrap.servers", conn);
		props.put("group.id", this.consumerGroup);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put("auto.offset.reset", "earliest");
		consumer = new KafkaConsumer<String, byte[]>(props);
		consumer.subscribe(Arrays.asList(topic));
	}
	
	@OnStopped
	public void onStopped() {
		try {
			if (consumer != null)
				consumer.close();
		} catch (Exception e) {
			System.out.println("Error in closing connection during onStopped method");
		}
	}

	/* (non-Javadoc)
	 * This consumer will have 8 bytes plus the content , 8 bytes are used to store the
	 * flowfileId which the producer should add to the byte content
	 */
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		ConsumerRecords<String, byte[]> records = consumer.poll(100);
		PrintWriter writer;

		if (!records.isEmpty()) {
			for (ConsumerRecord<String, byte[]> record : records) {
				final byte[] idAndContent = record.value();
				ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
				buffer.put(Arrays.copyOfRange(idAndContent, 0, Long.BYTES));
				buffer.flip();
				Long flowFileId = buffer.getLong();
				FlowFile flowFile = session.create();
				flowFile = session.putAttribute(flowFile, ExperimentAttributes.FLOWFILE_ID.key(),
						flowFileId.toString());
				openFile(context.getProperty(EXPERIMENT_LOG_FILE).getValue());
				expLogStream.println(String.format("%s,%s,%s,%s", flowFile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
						KafkaFlowFilesConsumer.class, (new Timestamp(System.currentTimeMillis())).getTime(), "ENTRY"));
				//this is application specific and done for ALPR for now
				if(topic.contains("entry")) {
					flowFile = session.putAttribute(flowFile, "entry_exit", "entry");
				} else {
					flowFile = session.putAttribute(flowFile, "entry_exit", "exit");
				}
				flowFile = session.putAttribute(flowFile, "msgId", Long.toString(record.offset()));
				flowFile = session.write(flowFile, new OutputStreamCallback() {

					public void process(OutputStream out) throws IOException {
						out.write(Arrays.copyOfRange(idAndContent, Long.BYTES, idAndContent.length));
					}
				});
				session.transfer(flowFile, SUCCESS);
				expLogStream.println(String.format("%s,%s,%s,%s", flowFile.getAttribute(ExperimentAttributes.FLOWFILE_ID.key()),
						KafkaFlowFilesConsumer.class, (new Timestamp(System.currentTimeMillis())).getTime(), "EXIT"));
			    expLogStream.flush();
			}
		}

	}

}
