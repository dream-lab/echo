package in.dream_lab.echo.utils;

import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.util.PythonInterpreter;

/**
 * Created by pushkar on 5/22/17.
 */
public class NifiClientFactory {

    private PyObject nifiClientClass;

    public NifiClientFactory() {
        /*StringBuilder sb = new StringBuilder();
        sb.append(System.getProperty("java.class.path"));
        sb.append(":").append("~/workplace/echo/src/main/nifi");
        Properties props = new Properties();
        props.setProperty("python.path", sb.toString());
        props.setProperty("python.verbose", "error");

        PythonInterpreter.initialize(System.getProperties(), props, new String[0]);*/

        PythonInterpreter interpreter = new PythonInterpreter();
        //interpreter.exec("import os\nprint os.getcwd()");
        interpreter.exec("import sys\nif '' not in sys.path:\n    sys.path.append(\"\")");
        interpreter.exec("from modules.json2pojo.src.main.python.DefaultNifiClient import DefaultNifiClient");
        nifiClientClass = interpreter.get("DefaultNifiClient");
    }

    public static NifiClient create(String address, int port) {
        NifiClientFactory factory = new NifiClientFactory();

        PyObject clientObject = factory.nifiClientClass.__call__(new PyString(address),
                                                        new PyString(Integer.toString(port)));
        return (NifiClient)clientObject.__tojava__(NifiClient.class);
    }

}
