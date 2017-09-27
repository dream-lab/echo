package in.dream_lab.echo.processors;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.json.JSONObject;

import com.jayway.jsonpath.JsonPath;


public class KafkaFlowFilesProducer extends AbstractProcessor{
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
    KafkaProducer<String, String> producer;
    Properties props;
    JSONObject jobj;
    String key = "";
    String content = "";
    String topic="";
    String brokerIP="";

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(TOPIC);
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
            brokerIP = context.getProperty(BROKERIP).getValue();
            props = new Properties();
            props.put("bootstrap.servers", brokerIP);
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<>(props);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        key = flowFile.getAttribute("uuid");
        content = "";

        jobj = new JSONObject();
        JSONObject attrval = new JSONObject();

        Map<String, String> m = flowFile.getAttributes();
        for(Map.Entry<String, String> e : m.entrySet()) {
            String k = e.getKey();
            String v = e.getValue();
            attrval.put(k, v);
        }

        jobj.put("attributes", attrval);

        session.read(flowFile, new InputStreamCallback() {

            public void process(InputStream arg0) throws IOException {
                content = IOUtils.toString(arg0, "UTF-8");
                jobj.put("content", content);
            }
        });

        String sending = jobj.toString();

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, sending);
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null)
                    e.printStackTrace();
                System.out.println("The offset of the record we just sent is: " + metadata.offset());
            }
        });

        session.transfer(flowFile, success);
    }
}