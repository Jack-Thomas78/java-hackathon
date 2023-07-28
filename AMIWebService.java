package uk.ac.mmu.advprog.hackathon;
import static spark.Spark.get;
import static spark.Spark.port;

import spark.Request;
import spark.Response;
import spark.Route;

/**
 * Handles the setting up and starting of the web service
 * You will be adding additional routes to this class, and it might get quite large
 * Feel free to distribute some of the work to additional child classes, like I did with DB
 * @author You, Mainly!
 */
public class AMIWebService 
{

	/**
	 * Main program entry point, starts the web service
	 * @param args not used
	 */
	public static void main(String[] args) 
	{		
		port(8088);
		
		//Simple route so you can check things are working...
		//Accessible via http://localhost:8088/test in your browser
		get("/test", new Route() 
		{
			@Override
			public Object handle(Request request, Response response) throws Exception 
			{
				try (DB db = new DB()) 
				{
					return "Number of Entries: " + db.getNumberOfEntries();
				}
			}
		});
		
		//route for a user to find the last signal of a requested motorway signal
		//Accessible via http://localhost:8088/lastsignal in your browser
		get("/lastsignal", new Route() 
		{ 
			@Override
			public Object handle(Request request, Response response) throws Exception 
			{
				try (DB db = new DB()) 
				{
					String signalID = request.queryParams("signal_id");
					return "Last signal: " + db.lastSignal(signalID); 
				}
			}		
		});
		
		//route for the user to find frequency of signals for a specific motorway in JSON format
		//Accessible via http://localhost:8088/frequency in your browser
		get("/frequency", new Route() 
		{ 
			@Override
			public Object handle(Request request, Response response) throws Exception 
			{
				try (DB db = new DB()) 
				{
					String signalID = request.queryParams("motorway");
					response.type("application/json");
					return db.getFrequency(signalID);
				}
			}		
		});
		
		//route that returns all signal groups in XML format
		//Accessible via http://localhost:8088/groups
		get("/groups", new Route() // returns all the groups contained in the database
		{ 
			@Override
			public Object handle(Request request, Response response) throws Exception 
			{
				try (DB db = new DB()) 
				{
					response.type("application/xml");
					return db.getGroups();
				}
			}		
		});
		
		
		
		System.out.println("Server up! Don't forget to kill the program when done!");
	}
}




