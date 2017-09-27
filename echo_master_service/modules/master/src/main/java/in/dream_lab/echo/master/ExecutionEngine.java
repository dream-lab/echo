package in.dream_lab.echo.master;

import in.dream_lab.echo.master.resources.EchoApplicationResource;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

/**
 * This is the class that contains the main method for the platform.
 * The ExecutionEngine is holds all the other classes 
 * Created by pushkar on 5/16/17.
 * 
 * Modified by Siva on 5/26/17
 */


/*public class ExecutionEngine {
	
	// For future use- to stop executing App/ reschedule etc.
	private HashMap<String, AppManager> appManagerMap = new HashMap<String, AppManager>();
	
    public static void main(String args[]) {
    	HttpServer server;
		try {
			server = HttpServer.create(new InetSocketAddress(8000), 0);
			// Mapping URL to corresponding Handler - Siva
            server.createContext("/", new indexHandler());
			server.createContext("/submitDAG", new submitDAGHandler());
			server.createContext("/stopDAG", new stopDAGHandler());
			server.createContext("/rebalanceDAG", new rebalanceDAGHandler());
			server.createContext("/applications", new applicationsHanlder());
			server.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    }
    /*
     * This class handles the submitDAG Http POST request
     * 
     * Created by Siva on 5/16/17.
     */
/*
    static class indexHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String response = "Hello";
            StringBuffer responseBuffer = new StringBuffer();
            BufferedReader reader = new BufferedReader(new FileReader("./static/index.html"));
            String temp;
            while( (temp = reader.readLine()) != null)
                responseBuffer.append(temp);
            response = responseBuffer.toString();
            exchange.sendResponseHeaders(200, response.length());
            OutputStream outstream = exchange.getResponseBody();
            outstream.write(response.toString().getBytes());
            outstream.close();
        }
    }

    static class applicationsHanlder implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            ResourceDirectory rd = new ResourceDirectory();

        }
    }

    static class submitDAGHandler implements HttpHandler {
		@Override
		public void handle(HttpExchange exchange) throws IOException {
			
            InputStreamReader inputStreamReader = new InputStreamReader(exchange.getRequestBody(), "utf-8");
            BufferedReader reader = new BufferedReader(inputStreamReader);
            String query ="";
            StringBuilder json = new StringBuilder();
            while( (query = reader.readLine()) != null) {
                json.append(query);
             }
            UUID uuid = UUID.randomUUID();
            // This has to be updated in the map
            System.out.println("Creating App Manager");
            AppManager appManager = new AppManager(uuid.toString(),json.toString());
            System.out.println("Deserialized");
            appManager.run();
            
            
            String response = "Submitted DAG with UUID: "+uuid;
            exchange.sendResponseHeaders(200, response.length());
            OutputStream outstream = exchange.getResponseBody();
            outstream.write(response.toString().getBytes());
            outstream.close();
		}
    }
    static class stopDAGHandler implements HttpHandler {
		@Override
		public void handle(HttpExchange exchange) throws IOException {
			
            String dataFlowUUID = exchange.getRequestHeaders().get("UUID").toString();
            
            AppManager appManager = new AppManager();
          
            boolean flag = appManager.stopDAG(dataFlowUUID);
            
            
            System.out.println("Stopped Dag: "+dataFlowUUID);
            
            String response = "Stopped Dag: "+dataFlowUUID;
            exchange.sendResponseHeaders(200, response.length());
            OutputStream outstream = exchange.getResponseBody();
            outstream.write(response.toString().getBytes());
            outstream.close();
		}
    }
    static class rebalanceDAGHandler implements HttpHandler {
		@Override
		public void handle(HttpExchange exchange) throws IOException {
			
            String dataFlowUUID = exchange.getRequestHeaders().get("UUID").toString();
            dataFlowUUID = dataFlowUUID.substring(1);
            dataFlowUUID = dataFlowUUID.replaceAll("]", "");
            System.out.println(dataFlowUUID);

            AppManager appManager = new AppManager();
          
            boolean flag = appManager.rebalanceDAG(dataFlowUUID);
            
            
            System.out.println("rebalanced Dag: "+dataFlowUUID);
            
            String response = "rebalanced Dag: "+dataFlowUUID;
            exchange.sendResponseHeaders(200, response.length());
            OutputStream outstream = exchange.getResponseBody();
            outstream.write(response.toString().getBytes());
            outstream.close();
		}
    }
}*/

public class ExecutionEngine extends Application<ExecutionEngineConfiguration>{

    public static void main(String [] args) throws Exception {
        new ExecutionEngine().run(args);
    }

    @Override
    public void initialize(Bootstrap<ExecutionEngineConfiguration> bootstrap) {

    }

    @Override
    public void run(ExecutionEngineConfiguration config, Environment env) {
        ResourceDirectoryClientFactory factory = new ResourceDirectoryClientFactory(
                config.getRdClientconfig().getRdAddress(),
                config.getRdClientconfig().getPortNumber()
        );
        env.jersey().register(new EchoApplicationResource(factory));
    }

}
