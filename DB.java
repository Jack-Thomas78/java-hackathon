package uk.ac.mmu.advprog.hackathon;

import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Handles database access from within your web service
 * @author You, Mainly!
 */
public class DB implements AutoCloseable {
	
	//allows us to easily change the database used
	private static final String JDBC_CONNECTION_STRING = "jdbc:sqlite:./data/AMI.db";
	
	//allows us to re-use the connection between queries if desired
	private Connection connection = null;
	
	/**
	 * Creates an instance of the DB object and connects to the database
	 */
	public DB() {
		try {
			connection = DriverManager.getConnection(JDBC_CONNECTION_STRING);
		}
		catch (SQLException sqle) {
			error(sqle);
		}
	}
	
	/**
	 * Returns the number of entries in the database, by counting rows
	 * @return The number of entries in the database, or -1 if empty
	 */
	public int getNumberOfEntries() {
		int result = -1;
		try {
			Statement s = connection.createStatement();
			ResultSet results = s.executeQuery("SELECT COUNT(*) AS count FROM ami_data");
			while(results.next()) { //will only execute once, because SELECT COUNT(*) returns just 1 number
				result = results.getInt(results.findColumn("count"));
			}
		}
		catch (SQLException sqle) {
			error(sqle);
			
		}
		return result;
	}
	/**
	 * returns the last signal of the specified id that is a relevant signal i.e not "OFF","NR",and"BLNK"
	 * @param n is the string value specified in the url given under the value of signal_id
	 * @return the last signal of the signal sign selected, or no result if invalid input is given
	 */
	public String lastSignal(String n)
	{
		if(n.length() <= 12 && n.length() > 0)
		{
			String result = "no results";
			String query ="SELECT signal_value FROM ami_data "
				+ "WHERE signal_id = ?" 
				+ " AND NOT signal_value = 'OFF' "
				+ "AND NOT signal_value = 'NR' "
				+ "AND NOT signal_value = 'BLNK' "
				+ "ORDER BY datetime DESC "
				+ "LIMIT 1;";
			try 
			{
				PreparedStatement s = connection.prepareStatement(query);
				s.setString(1, n);
				ResultSet lastSignalResults = s.executeQuery();
				while(lastSignalResults.next())
				{
					result = lastSignalResults.getString(lastSignalResults.findColumn("signal_value"));
				}
			}
			catch (SQLException sqle) 
			{
				error(sqle);
			}
			return result; 
		}
		else
		{
			return "no results";
		}
	}
	/**
	 * returns the number of times a signal value occurs per specific motorways
	 * @param m is the motorway which the user wishes to check the frequency of signal occurrences
	 * @return the frequency of every signal value for a specified motorway in JSON format
	 */
	public String getFrequency(String m)
	{
		
		JSONArray array = new JSONArray();
		String query = "SELECT"
				+ " COUNT(signal_value) AS frequency,"
				+ " signal_value"
				+ " FROM ami_data"
				+ " WHERE signal_id LIKE ?"
				+ " GROUP BY signal_value"
				+ " ORDER BY frequency DESC;";
		
		try 
		{
			PreparedStatement s = connection.prepareStatement(query);
			s.setString( 1 , m +"%" );
			ResultSet frequencies = s.executeQuery();	
			
			while (frequencies.next())
			{
				int freq = frequencies.getInt(frequencies.findColumn("frequency"));
				String a = frequencies.getString(frequencies.findColumn("signal_value"));
				JSONObject j = new JSONObject();
				j.put( "Value: " + a, "Frequency: " + freq);
				array.put(j);
			}
		}
		catch(SQLException sqle)
		{
			error(sqle);
		}
		return array.toString();
	}
	/**
	 * returns every possible group from the database
	 * @return every possible group from the database in xml format
	 */
	public String getGroups()
	{
		
		String xml ="";
		
		try 
		{
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			Document doc = dbf.newDocumentBuilder().newDocument();
			
			Element groups = doc.createElement("GRoups");
			doc.appendChild(groups);
			
			Statement s = connection.createStatement();
			ResultSet group = s.executeQuery("SELECT DISTINCT signal_group FROM ami_data;");
			while(group.next())
			{
				Element group1 = doc.createElement("Group");
				group1.setTextContent(group.getString(group.findColumn("signal_group")));
				groups.appendChild(group1);
			}
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			Writer output = new StringWriter();
			transformer.setOutputProperty(OutputKeys.INDENT,"yes");
			transformer.transform(new DOMSource(doc), new StreamResult(output));
			xml = output.toString();
		}
		catch(ParserConfigurationException |TransformerException ioe) {
			System.err.println("Error creating XML: " + ioe);
		}
		
		
		catch(SQLException sqle)
		{
			error(sqle);
		}
		
		return xml;
	}
	
	/**
	 * Closes the connection to the database, required by AutoCloseable interface.
	 */
	@Override
	public void close() {
		try {
			if ( !connection.isClosed() ) {
				connection.close();
			}
		}
		catch(SQLException sqle) {
			error(sqle);
		}
	}

	/**
	 * Prints out the details of the SQL error that has occurred, and exits the programme
	 * @param sqle Exception representing the error that occurred
	 */
	private void error(SQLException sqle) {
		System.err.println("Problem Opening Database! " + sqle.getClass().getName());
		sqle.printStackTrace();
		System.exit(1);
	} 
}
