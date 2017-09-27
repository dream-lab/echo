package in.dream_lab.echo.HyperCatServer;

import java.util.ArrayList;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.QueryParam;

/**
 * Root resource (exposed at "myresource" path)
 * @param <V>
 */
@Path("/cat")
public class Search{
    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "text/plain" media type.
     *
     * @return String that will be returned as a text/plain response.
     */
	@DELETE
	public Response deleteHandler(@Context UriInfo uriInfo) {
		String href = uriInfo.getQueryParameters().getFirst("href");
		if(href != null) {
			Catalogue.deleteCatalogueEntry(href);
			return Response.status(Status.OK).build();
		}
		else {
			return Response.status(Status.BAD_REQUEST).build();
		}
	}
	
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response getHandler(@Context UriInfo uriInfo) {
    	String href = uriInfo.getQueryParameters().getFirst("href");
    	String prefixhref = uriInfo.getQueryParameters().getFirst("prefix-href");
    	
    	
    	if(href != null && prefixhref == null){
    		String jsonResponse = simpleSearch(href);
    		return Response.ok(jsonResponse, MediaType.APPLICATION_JSON).build();
    	}
    	else if(prefixhref != null && href ==null){
    		String jsonResponse;
    		jsonResponse = prefixSearch(prefixhref);
    		return Response.ok(jsonResponse, MediaType.APPLICATION_JSON).build();
    	}
    	else if(prefixhref == null && href ==null) {
    		/*Write code to return the entire Catalogue*/
    		String jsonResponse = "null";
    		jsonResponse = returnEntireCatalogue();
    		return Response.ok(jsonResponse, MediaType.APPLICATION_JSON).build();
    	}
    	else {
    		return Response.status(Status.BAD_REQUEST).build();
    	}
    }
    
    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    public Response postHandler(@Context UriInfo uriInfo, String body) {
    	String href = uriInfo.getQueryParameters().getFirst("href");
    	if(href !=null && testCorrectness(body)) {
    		Catalogue.addCatalogueEntry(href, body);
    		return Response.status(Status.OK).build();
    	}else {
    		return Response.status(Status.BAD_REQUEST).build();
    	}
    }
    
    private String returnEntireCatalogue() {
    	ArrayList<String> items = Catalogue.returnAllItems();
    	String jsonResponse = null;
    	if(items.size() != 0)
			jsonResponse = Catalogue.baseString+",\"items\":"+items.toString()+"}";
		else
			jsonResponse = Catalogue.baseString+"}";
    	return jsonResponse;
    }
    
    private boolean testCorrectness(String body) {
    	boolean correct = true;
    	/*TODO: write code to test the correctness*/
    	return correct;
    }
    
    private String simpleSearch(String href) {
    	String item = Catalogue.searchByHref(href);
		String jsonResponse = null;
		if(item != null) 
			jsonResponse = Catalogue.baseString+",\"items\":["+item+"]}";
		else
			jsonResponse = Catalogue.baseString+"}";
		return jsonResponse;
    }
    
    private String prefixSearch(String prefixhref) {
    	ArrayList<String> items = Catalogue.searchByHrefPrefix(prefixhref);
    	String jsonResponse = null;
    	if(items.size() != 0)
			jsonResponse = Catalogue.baseString+",\"items\":"+items.toString()+"}";
		else
			jsonResponse = Catalogue.baseString+"}";
    	return jsonResponse;
    }
    
}
