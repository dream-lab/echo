package test.in.dream_lab.nifi.tf.processors.test;

import in.dream_lab.nifi.tf.processors.PutToSQL;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class PutToSQLTest {

    private byte[] readImage() {
        ByteArrayOutputStream bos = null;
        try {
            File f = new File("/home/pushkar/image_00000207_0.png");
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

    @Test
    public void testOnTrigger() throws IOException {

        InputStream content = new ByteArrayInputStream(readImage());

        TestRunner runner = TestRunners.newTestRunner(new PutToSQL());

        runner.setProperty(PutToSQL.BATCH_SIZE, "10");
        runner.setProperty(PutToSQL.DB_NAME, "/home/pushkar/workplace/testdb.db");

        Map<String, String> attributes = new HashMap<>();
        attributes.put("batchID", "1");

        runner.enqueue(content, attributes);

        runner.run(1);

    }

}
