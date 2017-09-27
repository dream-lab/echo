package in.dream_lab.echo.master;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by pushkar on 17/9/17.
 */
public class NetworkVisibilityMatrix {

    public static String PUSH_DIRECTION = "PUSH";
    public static String PULL_DIRECTION = "PULL";
    public static String NOT_VISIBLE = "NNOT_VISIBLE";

    private List<List<Boolean>> matrix;

    public NetworkVisibilityMatrix(String fileName) {
        matrix = new ArrayList<>();
        try {
            InputStream fis = new FileInputStream(fileName);
            InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
            BufferedReader br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null) {
                List<Boolean> newRow = new ArrayList<>();
                StringTokenizer tokenizer = new StringTokenizer(line, ",");
                while (tokenizer.hasMoreTokens()) {
                    if (Integer.parseInt(tokenizer.nextToken()) == 1)
                        newRow.add(true);
                    else
                        newRow.add(false);
                }
                matrix.add(newRow);
            }
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        }
        System.out.println("MATRIX SIZE " + matrix.size());
    }

    public String getDirection(int sourceId, int destinationId) {
        sourceId--; destinationId--;
        if (this.matrix.get(sourceId).get(destinationId).booleanValue())
            return PUSH_DIRECTION;
        else if (this.matrix.get(destinationId).get(sourceId).booleanValue())
            return PULL_DIRECTION;
        else
            return NOT_VISIBLE;
    }

}
