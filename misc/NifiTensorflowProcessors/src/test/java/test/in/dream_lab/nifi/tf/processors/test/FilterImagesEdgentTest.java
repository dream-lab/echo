package test.in.dream_lab.nifi.tf.processors.test;

import in.dream_lab.nifi.tf.processors.FilterImagesEdgent;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class FilterImagesEdgentTest {

    @Test
    public void testOnTrigger() throws IOException {

        TestRunner runner = TestRunners.newTestRunner(new FilterImagesEdgent());

        //set Properties
        // runner.setProperty()

        // add content
        //runner.enqueue(InputStream type)

        //Run
        runner.run(1);

        // check
        //runner.assert...

        // check flowfile
        //List<MockFlowFile> results = runner.getFlowFilesForRelationship(relationshipname)
        //assertwhatever
        //MockFlowFile result = results.get(0);
        // result.assertwhatever

    }

}
