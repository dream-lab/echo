package in.dream_lab.echo.master.resources;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;

import in.dream_lab.echo.master.AppManager;
import in.dream_lab.echo.master.ResourceDirectoryClientFactory;
import in.dream_lab.echo.master.db.EchoApplication;

/**
 * TODO SKM ::This is a just a thought but whenever we do a copy paste, then we need to
 * change were we paste. Can we build a semantic checker to suggest and maybe do
 * those changes. One place I felt the need is in scalability experiment for
 * Echo when I was adding startTime and endTime logging for an operation. If I
 * copy directly the log line for start at the end, I have to change the time to
 * endTime there. Can this be suggested. Something like this would definitely be
 * existing but lets start this development this december with a lookout first.
 *
 */
@Path("/DAG")
public class EchoApplicationResource {

	// private String name;
	private ResourceDirectoryClientFactory factory;

	// this is for scalability experiments
	private PrintWriter expLog;

	private static final String EXP_LOG_FILE = "/scale_experiment.log";

	private void openLogFile() {
		if (expLog == null)
			try {
				expLog = new PrintWriter(new BufferedWriter(new FileWriter(EXP_LOG_FILE, true)));
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	public EchoApplicationResource(ResourceDirectoryClientFactory factory) {
		this.factory = factory;
	}

	private static Map<String, AppManager> applicationMap = new HashMap<>();

	@GET
	@Timed
	@Produces(MediaType.APPLICATION_JSON)
	public EchoApplication get(@QueryParam("uuid") Optional<String> uuid) {
		if (uuid.isPresent())
			return new EchoApplication(uuid.get(), 0);
		else
			return new EchoApplication();
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public EchoApplication submit(@Valid String input) {
		long startTime = System.currentTimeMillis();
		String applicationId = UUID.randomUUID().toString();

		// adding the start time
		openLogFile();
		expLog.println(
				String.format("%s,%s,%s,%s", "DEPLOY", applicationId, 
						(new Timestamp(startTime)).getTime(), "START"));

		EchoApplication app = new EchoApplication(applicationId, 1);

		AppManager manager = new AppManager(applicationId, input);
		applicationMap.put(applicationId, manager);

		try {
			manager.startDAG();
		} catch (Exception e) {
			e.printStackTrace();
			throw new WebApplicationException(304);
		}
		long endTime = System.currentTimeMillis();

		// adding the end time
		expLog.println(
				String.format("%s,%s,%s,%s", "DEPLOY", applicationId,
						(new Timestamp(endTime)).getTime(), "END"));
		expLog.flush();

		System.out.println("************* deploy took " + (endTime - startTime) + "ms.");
		return app;
	}

	@POST
	@Path("/stop/")
	@Produces(MediaType.APPLICATION_JSON)
	public EchoApplication stop(@QueryParam("uuid") String uuid) {
		long startTime = System.currentTimeMillis();

		// adding the start time
		openLogFile();
		expLog.println(String.format("%s,%s,%s,%s", "STOP", uuid,
				(new Timestamp(startTime)).getTime(), "START"));

		AppManager manager = applicationMap.get(uuid);
		boolean flag = false;
		try {
			flag = manager.stopDAG();
		} catch (Exception e) {
			e.printStackTrace();
			throw new WebApplicationException(304);
		}
		long endTime = System.currentTimeMillis();

		// adding the end time
		expLog.println(String.format("%s,%s,%s,%s", "STOP", uuid,
				(new Timestamp(endTime)).getTime(), "END"));
		expLog.flush();

		System.out.println("************* stopping took " + (endTime - startTime) + "ms.");

		if (flag) {
			applicationMap.remove(uuid);
			return new EchoApplication();
		} else
			return new EchoApplication();
	}

	@POST
	@Path("/rebalance/")
	@Produces(MediaType.APPLICATION_JSON)
	public EchoApplication rebalance(@QueryParam("uuid") String uuid) {
		long startTime = System.currentTimeMillis();

		// adding the start time
		openLogFile();
		expLog.println(String.format("%s,%s,%s,%s", "REBALANCE", uuid,
				(new Timestamp(startTime)).getTime(), "START"));

		AppManager manager = applicationMap.get(uuid);
		try {
			manager.rebalanceDAG();
		} catch (Exception e) {
			e.printStackTrace();
			throw new WebApplicationException(304);
		}

		long endTime = System.currentTimeMillis();

		// adding the end time
		expLog.println(String.format("%s,%s,%s,%s", "REBALANCE", uuid,
				(new Timestamp(endTime)).getTime(), "END"));
		expLog.flush();
		// boolean flag = manager.stopDAG();
		// if (flag) {
		// manager.rebalanceDAG();
		// } else {

		// }
		return new EchoApplication(uuid, 1);
	}

}
