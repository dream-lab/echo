package in.dream_lab.echo.test;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.dream_lab.echo.utils.DataflowInput;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * Created by pushkar on 5/26/17.
 */
public class DataflowInputTest {

    @Test
    public void testDeserialize() {
        try {
            BufferedReader reader = new BufferedReader(
                    new FileReader("../../input-dags/trivial-dag-1.json"));
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
            }
            String json = builder.toString();
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
            DataflowInput dataflowInput = mapper.readValue(json, DataflowInput.class);
            assert(dataflowInput.getProcessors()
                    .iterator().next()
                    .getProperties().get(0)
                    .getAdditionalProperties().get("Batch Size").equals("10"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
