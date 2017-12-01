package in.dream_lab.nifi.tf.processors;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

public class PutToSQL extends AbstractProcessor {

    public static final PropertyDescriptor DB_NAME = new PropertyDescriptor.Builder()
            .name("Database Name")
            .description("The database's that is running in the fog server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of files to pull in each iteration")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Once all the files for a batch are received")
            .build();


    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    private HashMap<String, Integer> batchCounterMap;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        batchCounterMap = new HashMap<>();
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DB_NAME);
        properties.add(BATCH_SIZE);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() { return properties; }

    @Override
    public Set<Relationship> getRelationships() { return relationships; }

    private byte[] readFile(Path path) {
        ByteArrayOutputStream bos = null;
        try {
            File f = new File(path.toString());
            FileInputStream fis = new FileInputStream(f);
            byte[] buffer = new byte[1024];
            bos = new ByteArrayOutputStream();
            for (int len; (len = fis.read(buffer)) != -1;) {
                bos.write(buffer, 0, len);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bos != null ? bos.toByteArray() : null;
    }

    private void writeImageToDatabase(String batchID, Path path, String dbName) {

        String insertSQL = "INSERT INTO IMAGES"
        + "(ID, BATCH_ID, IMAGE) VALUES" + "(?, ?, ?)";
        try (Connection conn = connect(dbName);
             PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            pstmt.setInt(1, (new Random(System.currentTimeMillis())).nextInt());
            pstmt.setInt(2, Integer.parseInt(batchID));
            pstmt.setBytes(3, readFile(path));

            pstmt.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private Connection connect(String dbName) {
        String url = "jdbc:sqlite:" + dbName;
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        FlowFile flowfile = session.get();
        OutputStream outputStream = new ByteArrayOutputStream();
        Path path = FileSystems.getDefault().getPath("/tmp","nifi.file");
        session.exportTo(flowfile, path, false);

        String batchID = flowfile.getAttribute("batchID");
        String dbName = context.getProperty(DB_NAME).getValue();
        writeImageToDatabase(batchID, path, dbName);
        // session.
        // put the flowfile into the db
        int count = 0;
        if (batchCounterMap.containsKey(batchID))
            count = batchCounterMap.get(batchID);
        batchCounterMap.put(batchID, ++count);

        if (count >= Integer.parseInt(context.getProperty(BATCH_SIZE).getValue())) {
            session.transfer(flowfile, REL_SUCCESS);
            batchCounterMap.remove(batchID);
        } else
            session.remove(flowfile);

        session.commit();
    }

}
