package in.dream_lab.echo.HyperCatServer;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class Catalogue {
	private static ConcurrentHashMap<String, String> catalogue = new ConcurrentHashMap<String, String>();
	static String baseString = "{\n" + 
			"    \"catalogue-metadata\": [\n" + 
			"        {\n" + 
			"            \"val\": \"application/vnd.hypercat.catalogue+json\",\n" + 
			"            \"rel\": \"urn:X-hypercat:rels:isContentType\"\n" + 
			"        },\n" + 
			"        {\n" + 
			"            \"val\": \"HyperCat for ECHO\",\n" + 
			"            \"rel\": \"urn:X-hypercat:rels:hasDescription:en\"\n" + 
			"        },\n" + 
			"        {\n" + 
			"            \"rel\": \"urn:X-hypercat:rels:supportsSearch\",\n" + 
			"            \"val\": \"urn:X-hypercat:search:multi\"\n" + 
			"        },\n" + 
			"        {\n" + 
			"            \"rel\": \"urn:X-hypercat:rels:supportsSearch\",\n" + 
			"            \"val\": \"urn:X-hypercat:search:prefix\"\n" + 
			"        }\n" + 
			"    ]";
	Catalogue(){
		//catalogue.put();
	}
	
	public static String searchByHref(String href){
		String item = null;
		item = catalogue.get(href);
		return item;
	}
	
	public static ArrayList<String> searchByHrefPrefix(String href) {
		ArrayList<String> items = new ArrayList<String>();
		for(String key : catalogue.keySet()) {
			if(key.contains(href))
				items.add(catalogue.get(key));
		}
		return items;
	}
	
	public static ArrayList<String> returnAllItems() {
		ArrayList<String> items = new ArrayList<String>();
		for(String key : catalogue.keySet()) {
			items.add(catalogue.get(key));
		}
		return items;		
	}
	
	public static void deleteCatalogueEntry(String href) {
		catalogue.remove(href);
	}
	
	public static void addCatalogueEntry(String href, String json){
		catalogue.put(href, json);
	}
}
