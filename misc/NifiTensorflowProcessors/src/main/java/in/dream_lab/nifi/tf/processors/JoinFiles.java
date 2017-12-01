package in.dream_lab.nifi.tf.processors;

import org.apache.nifi.processor.*;
import org.apache.nifi.flowfile.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.*;

import java.lang.Thread;
import java.util.*;
import java.nio.file.*;
import java.io.*;



public class JoinFiles extends AbstractProcessor {

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of files to pull in each iteration")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();
    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("The directory to which files should be written. You may use expression language such as /aa/bb/${path}")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor WAIT_TIME = new PropertyDescriptor.Builder()
            .name("Wait Time")
            .description("The number of seconds that the processor should wait for if content not in file system. seconds per image.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to the output directory are transferred to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        // relationships
        final Set<Relationship> procRels = new HashSet<Relationship>();
        procRels.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(procRels);

        // descriptors
        final List<PropertyDescriptor> supDescriptors = new ArrayList<PropertyDescriptor>();
        supDescriptors.add(DIRECTORY);
        supDescriptors.add(BATCH_SIZE);
        supDescriptors.add(WAIT_TIME);
        properties = Collections.unmodifiableList(supDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        
        FlowFile flowfile = session.get();
        String batchId = flowfile.getAttribute("batchID");
        int batchSize = Integer.parseInt(context.getProperty(BATCH_SIZE).getValue());
        int sleepTime = Integer.parseInt(context.getProperty(WAIT_TIME).getValue());
        // Assume that this is the full path to the root directory
        String rootDirectory = context.getProperty(DIRECTORY).getValue();
        try {
            Path batchDirectory = Paths.get(rootDirectory, batchId);
	    while (true) {
		    if (Files.exists(batchDirectory)) {
			File directory = new File(batchDirectory.toString());
			while (true){
			    File[] imageList = directory.listFiles();
			    if (imageList.length >= batchSize) {
				// send each of these images out to the next guy
				for (File image : imageList) {
				    FlowFile newImage = session.create();
				    Path imagePath = image.toPath();
				    session.importFrom(imagePath, true, newImage);
				    session.putAttribute(newImage, "batchID", batchId);
				    session.transfer(newImage, REL_SUCCESS);
				}
				break;
			    } else {
				Thread.sleep(sleepTime);
			    }
			}
			break;
		    } else {
			Thread.sleep(sleepTime);
		    }
	    }
        } catch (Exception e) {
            e.printStackTrace();
        }
 	session.commit();
    }
}
