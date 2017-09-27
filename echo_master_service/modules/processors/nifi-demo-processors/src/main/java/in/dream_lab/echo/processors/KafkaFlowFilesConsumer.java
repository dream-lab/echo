package in.dream_lab.echo.processors;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.slf4j.Logger;


public class KafkaFlowFilesConsumer extends AbstractProcessor{
    public static final PropertyDescriptor CONSUMER_GROUP_NAME = new PropertyDescriptor.Builder().name("group_name")
            .displayName("Consumer group name").description("Consumer group name of the subscriber").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder().name("topic")
            .displayName("Topic").description("topic name").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor BROKERIP = new PropertyDescriptor.Builder().name("brokerip")
            .displayName("BrokerIP").description("Public IP of the broker").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final Relationship success = new Relationship.Builder().name("success").description("On success")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private Properties p;
    private static Logger l;
    KafkaConsumer<String, String> consumer;
    Properties props;

    String key = "";
    String content = "";
    String topic;
    String groupName;
    JSONObject jobj;
    JSONObject attrval;
    String brokerIP="";

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(TOPIC);
        descriptors.add(CONSUMER_GROUP_NAME);
        descriptors.add(BROKERIP);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(success);
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
        try {
            topic = context.getProperty(TOPIC).getValue();
            groupName = context.getProperty(CONSUMER_GROUP_NAME).getValue();
            brokerIP = context.getProperty(BROKERIP).getValue();
            props = new Properties();
            props.put("bootstrap.servers", brokerIP);
            props.put("group.id", groupName);
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("auto.offset.reset", "earliest");
            consumer = new KafkaConsumer<String, String>(props);
            consumer.subscribe(Arrays.asList(topic));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        key = "";
        content = "";
        jobj = new JSONObject();
        ConsumerRecords<String, String> records = consumer.poll(100);

        if (!records.isEmpty()) {
            for (ConsumerRecord<String, String> record : records) {
                FlowFile flowFile = session.create();
                String attrName = "", attrValue = "";
                key = record.key();
                String v = record.value();
                jobj = new JSONObject(v);
                attrval = (JSONObject) jobj.get("attributes");
                content = jobj.getString("content");

                Iterator<String> keys = attrval.keys();
                while(keys.hasNext()){
                    attrName = keys.next();
                    attrValue = (String) attrval.get(attrName);
                    flowFile = session.putAttribute(flowFile, attrName, attrValue);
                }


                //String[] m = meta.split(",");
				/*for(String t : m) {
					String[] attributes = t.split("\\+");
					attrName = attributes[0];
					attrValue = attributes[1];
					flowFile = session.putAttribute(flowFile, attrName, attrValue);
				}*/
                flowFile = session.write(flowFile, new OutputStreamCallback() {

                    public void process(OutputStream out) throws IOException {
                        out.write(content.getBytes());
                    }
                });
                session.transfer(flowFile, success);
            }
        }

    }
}
