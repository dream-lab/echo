package in.dream_lab.nifi.tf.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;
import org.sqlite.core.DB;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

public class JoinFromSQL extends AbstractProcessor {

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
    //private static int

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

    private Connection connect(String dbName) {
        String url = "jdbc:sqlite:" + dbName;
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    private ArrayList<Path> getFilePaths(String batchID, String dbName) {

        ArrayList<Path> paths = new ArrayList<>();
        String getSQL = "SELECT IMAGE FROM IMAGES WHERE BATCH_ID = " + Integer.parseInt(batchID);
        try (Connection conn = connect(dbName);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(getSQL)){

            int i = 0;
            while (rs.next()) {
                File file = new File("/tmp/image" + i);
                FileOutputStream fos = new FileOutputStream(file);

                InputStream input = rs.getBinaryStream("image");
                byte[] buffer = new byte[1024];
                while (input.read(buffer) > 0 ) {
                    fos.write(buffer);
                }
                ++i;
                fos.close();
                paths.add(FileSystems.getDefault().getPath(file.getAbsolutePath()));

            }

        } catch(SQLException | IOException e) {
            e.printStackTrace();
        }
        return paths;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        FlowFile flowfile = session.get();
        String batchID = flowfile.getAttribute("batchID");
        String dbName = context.getProperty(DB_NAME).getValue();
        ArrayList<Path> paths = getFilePaths(batchID, dbName);
        // loop through paths and send flowfiles
        for (Path path : paths) {
            FlowFile newFlowfile = session.create();
            session.importFrom(path, true, newFlowfile);
            session.putAttribute(newFlowfile, "batchID", batchID);
            session.transfer(flowfile, REL_SUCCESS);
        }
        session.commit();

    }

}
