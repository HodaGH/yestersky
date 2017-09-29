package queryAirData;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;

import javax.servlet.*;
import javax.servlet.http.*;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * This is a servlet used by Tomcat7 to communicate with Cassandra and build webpages.
 * Rather than executing a main method, the doGet method is run whenever a GET request
 * is made to the webpage associated with this servlet.
 * 
 * Any CLASSPATH dependencies for servlets should be included in
 * "/var/lib/tomcat7/conf/catalina.properties" (or wherever "catalina.properties" is stored)
 * 
 * The GET methods should have four variables:
 *   timestamp - an integer corresponding to a UNIX timestamp
 *   lattitude and longitude - doubles corresponding to GPS coordinates
 *   radius - an integer corresponding to a distance in km
 * 
 * @author Kevin Moss
 *
 */
public class GetQuery extends HttpServlet {
	
	//IP address used to query Cassandra
	static final String ipAddress = "ec2-52-40-31-95.us-west-2.compute.amazonaws.com";
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) 
			throws IOException, ServletException {
		
		// Set the response MIME type of the response message
		response.setContentType("text/html");
		
		String timestamp = request.getParameter("timestamp");
		String lattitude = request.getParameter("lattitude");
		String longitude = request.getParameter("longitude");
		String radius = request.getParameter("radius");
		
		int time = Integer.parseInt(timestamp);
		double lat = Double.parseDouble(lattitude);
		double lon = Double.parseDouble(longitude);
		int rad = Integer.parseInt(radius);
		assert time > 0;
		assert -90 < lat & lat < 90;
		assert -180 < lon & lon < 180;
		
		
		ArrayList<String> results = toCassandra(time, lat, lon, rad);
		
		// Allocate a output writer to write the response message into the network socket
		PrintWriter out = response.getWriter();
		try {	
			writeHTML(out, results, lat, lon, rad);
		} finally {
			out.close();
		}
	}
	
	/**
	 * Sends the query to Cassandra and gets a list of GPS coordinates in response.
	 * @param time The UNIX timestamp
	 * @param lat Lattitude of the center point
	 * @param lon Longitude of the center point
	 * @param rad Desired radius of the map
	 * @return An ArrayList of Strings representing GPS coordinates
	 */
	protected static ArrayList<String> toCassandra(int time, double lat, double lon, int rad) {
		
		/*
		 * Resolution determines the size of the geohash boxes that will be queried, and
		 * radius (map size) is the factor in determining resolution.
		 */
		int res = getResolution(rad);
		
		
		String[] geoHashes = geosNeeded(lat, lon, rad, res);
		
		
	    CassandraConnector client = new CassandraConnector();
		final int port = 9042;
		client.connect(ipAddress, port);
		
		Session session = client.getSession();
		String query = "SELECT lat, lon FROM openskyproject.openskydata WHERE"
				+ " timeinterval=? AND geohash=?;";
		
		ArrayList<String> points = new ArrayList<String>();
		for(String geo : geoHashes) {
			ResultSet rs = session.execute(query, time/3600, geo);
			Iterator<Row> rsIt = rs.iterator();
			while(rsIt.hasNext()) {
				Row row = rsIt.next();
				double thisLat = row.getDouble(0);
				double thisLon = row.getDouble(1);
				points.add("["+thisLat+","+thisLon+"]");
			}
		}
		
		client.close();
		return points;
	}
	
	
	/**
	 * Constructs a geohash of GPS coordinates. This method is a duplicate of the
	 * geohash method "processAirData.FlightState.encodeGeohash", and any changes
	 * made to one should be made to the other as well.
	 * 
	 * This method is a duplicate because dependencies are undesirable for servlets.
	 * 
	 * @param lat Lattitude
	 * @param lon Longitude
	 * @param bits Number of bits to return for the geohash
	 * @return A long representing a geohash. Best viewed as a binary string.
	 */
	public static long geohash(double lat, double lon, int bits) {
		double minLat = -90,  maxLat = 90;
		double minLng = -180, maxLng = 180;
		long result = 0;
		for (int i = 0; i < bits; i++) {
			if (i % 2 == 0) {                 // even bit: bisect longitude
				double midpoint = (minLng + maxLng) / 2;
				if (lon < midpoint) {
					result <<= 1;                 // push a zero bit
					maxLng = midpoint;            // shrink range downwards
				} else {
					result = result << 1 | 1;     // push a one bit
					minLng = midpoint;            // shrink range upwards
				}
			} else {                          // odd bit: bisect latitude
				double midpoint = (minLat + maxLat) / 2;
				if (lat < midpoint) {
					result <<= 1;                 // push a zero bit
					maxLat = midpoint;            // shrink range downwards
				} else {
					result = result << 1 | 1;     // push a one bit
					minLat = midpoint;            // shrink range upwards
				}
			}
		}
		return result;
	}
	
	/**
	 * Gets a geohash as a hexadecimal string. This method guarantees an exact number of digits,
	 * i.e. it doesn't drop leading 0s. This is an important property for geohashes, as leading
	 * digits are significant.
	 * @param lat Lattitude
	 * @param lon Longitude
	 * @param hexDigits Number of hexadecimal digits desired.
	 * @return A hexadecimal number in the form of a String
	 */
	public static String getHexGeohash(double lat, double lon, int hexDigits) {
		String s = Long.toHexString(geohash(lat, lon, 4*hexDigits));
		while(s.length() < hexDigits) {
			s = "0"+s;
		}
		return s;
	}
	
	
	private static final int maxSteps = 20;/* Limits the number of geohash boxes to be queried.
	At maxSteps==20, (20*2+1)^2 = 2601 boxes are queried.
	This variable will be removed when more resolutions are supported in future implementations.*/
	
	/**
	 * Calculates and constructs the set of geohash strings needed for the query.
	 * The geohash strings are the boxes within a square centered at the desired point.
	 * The box's size is 2*rad by 2*rad
	 * 
	 * @param lat Lattitude of center point
	 * @param lon Longitude of center point
	 * @param rad Distance from the center to the middle of one side of the box.
	 * @param res "Resolution" i.e. number of hexadecimal digits for the geohash boxes
	 * @return
	 */
	public static String[] geosNeeded(double lat, double lon, int rad, int res) {
		/* First we would determine the dimensions of a geohash box and use them to determine the
		 * number of boxes to we should query. Currently the number of geohash boxes we query is
		 * fixed for the purpose of consistent latency. */
		
		//Number of geohash boxes queried is typically equal to (2*latSteps+1)(2*lonSteps+1)
		int latSteps = maxSteps;
		int lonSteps = maxSteps;
		
		/* For simplicity, there is currently wraparound at the north and south poles.
		 * The OpenSky Network doesn't cover the poles, so the issue is currently unimportant.
		 * Overlap caused by wraparound is not an issue with the currently supported resolution
		 * levels, but it should be addressed in future development.*/
		
		//In this step, we split the center geohash into its horizontal vertical parts
		long geoCenter = geohash(lat,lon,4*res);
		String fullHash = Long.toBinaryString(geoCenter);
		while(fullHash.length() < 4*res) {
			fullHash = "0"+fullHash;
		}
		
		String latHash = "";
		String lonHash = "";
		for(int i = 0; i < fullHash.length(); i+=2) {
			lonHash += fullHash.charAt(i);
		}
		for(int i = 1; i < fullHash.length(); i+=2) {
			latHash += fullHash.charAt(i);
		}
		long binaryLat = Long.parseLong(latHash,2);
		long binaryLon = Long.parseLong(lonHash,2);
		
		
		//Finally, construct the geohash list 
		int mod = (int)Math.pow(2, 2*res);
		
		ArrayList<String> geos = new ArrayList<String>();
		
		for(int i = -latSteps; i <= latSteps; i++) {
			for(int j = -lonSteps; j <= lonSteps; j++) {
				long thisLat = (binaryLat + mod + i)%mod;
				long thisLon = (binaryLon + mod + j)%mod;
				String thisLatString = Long.toBinaryString(thisLat);
				while(thisLatString.length() < 2*res) {
					thisLatString = "0"+thisLatString;
				}
				String thisLonString = Long.toBinaryString(thisLon);
				while(thisLonString.length() < 2*res) {
					thisLonString = "0"+thisLonString;
				}
				String together = "";
				for(int k = 0; k < res*2; k++) {
					together += ""+thisLonString.charAt(k)
							+ thisLatString.charAt(k);//
				}
				String thisHex = Long.toHexString(Long.parseLong(together,2));
				geos.add(thisHex);
			}
		}
		return geos.toArray(new String[geos.size()]);
	}
	
	
	
	/**
	 * Writes an HTML page. In particular, the page is a Leaflet heatmap that plots the given points,
	 * is centered at the specified lattitude and longitude, and has size determined by the radius.
	 * @param out A PrintWriter that is meant to write the HTML page.
	 * @param points An ArrayList of Strings, each of which should be a gps location in the form "[lat, lon]"
	 * @param lat Latitude of map center
	 * @param lon Longitude of map center
	 * @param rad "Radius" of the desired map, i.e. distance from center to midpoint of each side of square.
	 */
	private static void writeHTML(PrintWriter out, ArrayList<String> points,
			double lat, double lon, int rad) {
		
		int res = getResolution(rad);
		/* Currently, the magnification (zoomLevel) of the map is that which best
		 * fits the resolution. In future implementations, it will support a more
		 * dynamic radius */
		int zoomLevel = 2*res - 3;
		zoomLevel = (zoomLevel >= 0) ? zoomLevel : 0;//TODO use this
		
		
		out.println("<html>");
		out.println("<head>");
		out.println("    <title>Air-Traffic Heat Map</title>");
		out.println("    <link rel=\"stylesheet\" href=\"http://cdn.leafletjs.com/"
				+ "leaflet/v0.7.7/leaflet.css\" />");
		out.println("    <script src=\"http://cdn.leafletjs.com/leaflet/v0.7.7/leaflet.js\"></script>");
		out.println("<style>");
		out.println("        #map { width: 800px; height: 600px; }");
		out.println("        body { font: 16px/1.4 \"Helvetica Neue\", Arial, sans-serif; }");
		out.println("        .ghbtns { position: relative; top: 4px; margin-left: 5px; }");
		out.println("        a { color: #0077ff; }");
		out.println("    </style>");
		out.println("</head>");
		
		out.println("<body>");
		out.println("<!-- <p> </p> -->");//If a description is desired above the map, put it here.

		out.println("<div id=\"map\"></div>");
		out.println("<script src=\"Leaflet.heat/dist/leaflet-heat.js\"></script>");
		
		out.println("<script>");
		out.println("var addressPoints = [");
		for(int i = 0; i < points.size()-1; i++) {
			out.print(points.get(i)+",");
			if(i%10==9) {
				out.println();
			}
		}
		out.println(points.get(points.size()-1)+"];");
		out.println();
		
		out.println("var map = L.map('map').setView(["+lat+", "+lon+"], "+zoomLevel+");");
		out.println("var tiles = L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {");
		out.println("    attribution: '&copy; <a href=\"http://osm.org/copyright\">"
				+ "OpenStreetMap</a> contributors',");
		out.println("}).addTo(map);");
		out.println("addressPoints = addressPoints.map(function (p) { return [p[0], p[1]]; });");
		out.println("var heat = L.heatLayer(addressPoints, {blur:8, radius:5, maxZoom:5}).addTo(map);");

		out.println("</script>");
		out.println("</body>");
		out.println("</html>");
	}
	
	/**
	 * Gets the most appropriate resolution of the map for the given radius.
	 * Resolution is the number of hexadecimal digits used in geohashes.
	 * Currently only resolutions 4, 5, 6, and 7 are supported.
	 * 
	 * @param rad an integer
	 * @return an integer
	 */
	private static int getResolution(int rad) {
		int res;
		if(rad >= 625) { 
			res = 4;
		} else if (rad >= 156) { 
			res = 5;
		} else if (rad >= 39) { 
			res = 6;
		} else { 
			res = 7;
		}
		return res;
	}
	
	
}

