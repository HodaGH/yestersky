package processAirData;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * An object containing data about a flight's state.
 * See https://opensky-network.org/datasets/states/README.txt for details about each variable.
 * @author kevin
 *
 */
public class FlightState {
	/*
	 * Fields:
	 *        time,icao24,lat,lon,velocity,heading,vertrate,callsign,onground,
	 *        alert,spi,squawk,baroaltitude,geoaltitude,lastposupdate,lastcontact
	 *        
	 * By default, fields are in this order in CSV files. Some fields are not relevant to the schema.
	 * They are kept in this object for use in future development.
	 * 
	 */
	
	private Integer time, squawk;
	private String icao24, callsign;
	private Double lat, lon, velocity, heading, vertrate, baroaltitude, geoaltitude, lastposupdate, lastcontact;
	private boolean onground, alert, spi;
	
	// The schema will be used for AVRO serialization
	private static String userSchema = "{\"type\":\"record\",\"name\":\"flightstate\",\"fields\":"
			+ "[{\"name\":\"timeinterval\",\"type\":\"int\"},"
			+ "{\"name\":\"geohash\",\"type\":\"string\"},"
			+ "{\"name\":\"flight\",\"type\":\"string\"},"
			+ "{\"name\":\"lat\",\"type\":\"double\"},"
			+ "{\"name\":\"lon\",\"type\":\"double\"},"
			+ "{\"name\":\"time\", \"type\":\"int\"}]}";
	private static Schema.Parser parser = new Schema.Parser();
	protected final static Schema schema = parser.parse(userSchema);
	
	/**
	 * Creates a FlightState from an array of strings corresponding to
	 * the FlightState variables. This constructor is intended to be used
	 * for CSV files.
	 * 
	 * The String[] should have exactly 16 entries. The corresponding fields,
	 * in order, are:
	 *   time,icao24,lat,lon,velocity,heading,vertrate,callsign,onground,
	 *   alert,spi,squawk,baroaltitude,geoaltitude,lastposupdate,lastcontact
	 * @param parts An array of strings
	 */
	public FlightState(String[] parts) { //For CSV files
		if(parts.length != 16) {
			throw new IllegalArgumentException("String array should have exactly 16 entries.");
		}
		for(int i = 0; i < parts.length; i++) {
			parts[i].trim();
		}
		time = parseInt(parts[0]);
		icao24 = parts[1].trim();
		lat = parseDouble(parts[2]);
		lon = parseDouble(parts[3]);
		velocity = parseDouble(parts[4]);
		heading = parseDouble(parts[5]);
		vertrate = parseDouble(parts[6]);
		callsign = parts[7].trim();
		onground = Boolean.parseBoolean(parts[8].trim());
		alert = Boolean.parseBoolean(parts[9].trim());
		spi = Boolean.parseBoolean(parts[10].trim());
		squawk = parseInt(parts[11]);
		baroaltitude = parseDouble(parts[12]);
		geoaltitude = parseDouble(parts[13]);
		lastposupdate = parseDouble(parts[14]);
		lastcontact = parseDouble(parts[15]);
	}
	
	/**
	 * Constructs a FlightState from a String corresponding to flight state variables.
	 * This constructor is intended to be used for JSON files from live-scrape data.
	 * The string should be a list of values separated by commas. Corresponding variables
	 * are as follows:
	 * 
	 * 0. ica024 (String)
	 * 1. callsign (String)
	 * 2. origin_country (String) - ignored
	 * 3. time_position (Integer) - 
	 * 4. time_velocity (Integer) - ignored
	 * 5. longitude (Double)
	 * 6. latitude (Double)
	 * 7  altitude (Double) - could be baroaltitude or geoaltitude
	 * 8. on_ground (boolean)
	 * 9. velocity (Double)
	 * 10. heading (Double)
	 * 11. vertical_rate (Double)
	 * 12. sensors (int[]) - should be null since no sensor filtering was used
	 * 13. ???  (Double)
	 * 14. squawk (String)
	 * 15. ???   (boolean) 
	 * 16. ??? (Integer) - usually 0
	 * 
	 * alert, spi, squak
	 * lastposupdate,lastcontact
	 * See "https://opensky-network.org/apidoc/rest.html" for further information.
	 * 
	 * 
	 * @param state
	 */
	public FlightState(String state) { //For JSON files extracted from live data
		String[] parts = state.split(",");
		
		icao24 = parts[0].replaceAll("\"","").trim();
		callsign = parts[1].replaceAll("\"","").trim();
		time = parseInt(parts[3]);
		lon = parseDouble(parts[5]);
		lat = parseDouble(parts[6]);
		baroaltitude = parseDouble(parts[7]);
		geoaltitude = baroaltitude;
		onground = Boolean.parseBoolean(parts[8].trim());
		velocity = parseDouble(parts[9]);
		heading = parseDouble(parts[10]);
		vertrate = parseDouble(parts[11]);
		squawk = parseInt(parts[14].replaceAll("\"",""));
		
		lastposupdate = parseDouble(""+time); //it'll be within 15 seconds for live data
		//alert = null;
		//spi = null;
		//lastcontact = null;
		
		
	}
	
	
	
	/**
	 * Parses a string representing an integer. Whitespace or an empty string becomes null.
	 * @param s A string
	 * @return An integer
	 */
	private Integer parseInt(String s) {
		String s2 = s.trim();
		if(s2.equals("") || s2.equals("null")) {
			return null;
		} else {
			return Integer.parseInt(s2);
		}
	}
	
	
	/**
	 * Parses a string representing a double. Whitespace or an empty string becomes null;
	 * @param s A string
	 * @return A double
	 */
	private Double parseDouble(String s) {
		String s2 = s.trim();
		if(s2.equals("") || s2.equals("null")) {
			return null;
		} else {
			return Double.parseDouble(s2);
		}
	}
	
	public String getPlane() {
		return icao24;
	}
	public Integer getTime() {
		return time;
	}
	public Integer getHour() {
		return time/3600;
	}
	public Double getLat() {
		return lat;
	}
	public Double getLon() {
		return lon;
	}
	public String getGeohash(int hexDigits) {
		return getHexGeohash(lat,lon,hexDigits);
	}
	
	/**
	 * Checks that important variables are defined and that the age of the state is
	 * at most 15 seconds.
	 * @return
	 */
	public boolean isValid() {
		if(lat == null || lon == null || time == null || icao24.equals("") || lastposupdate == null) {
			return false;
		}
		if(time - lastposupdate >= 15) {
			return false;
		}
		return true;
	}
	
	/**
	 * A simple geohashing algorithm. Geohashing is an efective for describing
	 * GPS coordinates in one variable, as well as for  partitioning a map.
	 * 
	 * Geohash values should be viewed as a BitString. If the number of bits
	 * is divisible by 4, viewing the geohash as a HexString is also valuable.
	 * 
	 * To minimize servlet dependencies, a duplicate method is defined in 
	 * "queryAirData.GetQuery". Any changes made here should be reflected there
	 * as well.
	 * 
	 * @param lat a double signifying latitude
	 * @param lng a double signifying longitude
	 * @param bits An integer assigning the accuracy of the geohash
	 * @return A long. Best viewed as a bitstring with leading 0s re-added
	 */
	public static long encodeGeohash(double lat, double lon, int bits) {
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
	 * Returns a hexadecimal string representing a geohash value.
	 * Each additional hex digit increases accuracy.
	 * 
	 * Intentional use is between 4 and 7 digits inclusive.
	 * 
	 * @param lat A double representing latitude
	 * @param lon A double representing longitude
	 * @return A string denoting a hexadecimal number.
	 */
	public static String getHexGeohash(double lat, double lon, int hexDigits) {
		String s = Long.toHexString(encodeGeohash(lat, lon, 4*hexDigits));
		while(s.length() < hexDigits) {
			s = "0"+s;
		}
		return s;
	}
	
	
	/**
	 * Produces a record based on the default schema.
	 * The record also contains the topic, which is necessary for Kafka's PubSub system.
	 * The message is in JSON format. The KafkaProducer will apply Avro Serialization.
	 * @param fs A FlightState
	 * @param geohashDigits The number of hexadecimal geohash digits to be used in the message
	 * @return A record
	 */
	public static ProducerRecord<Object, Object> kafkaAvroRecord(FlightState fs, int geohashDigits) {
		if(!fs.isValid()) {
			throw new IllegalArgumentException("Flight state has expired or is incomplete.");
		}
		
		GenericRecord record = new GenericData.Record(schema);
		
		record.put("timeinterval", fs.getHour());
		record.put("geohash", fs.getGeohash(geohashDigits));
		record.put("flight", fs.getPlane());
		record.put("lat", fs.getLat());
		record.put("lon", fs.getLon());
		record.put("time", fs.getTime());
		
		return new ProducerRecord<Object, Object>("opensky-topic",record);
	}
	
	/**
	 * Gives a short description of the state
	 * @return A string
	 */
	public String describe() {
		String out = "";
	    //  time,icao24,lat,lon,velocity,heading,vertrate,callsign,onground,
	 	//  alert,spi,squawk,baroaltitude,geoaltitude,lastposupdate,lastcontact
		out += "time:"+time+", ";
		out += "icao24:"+icao24+", ";
		out += "lat:"+lat+", ";
		out += "lon:"+lon;
		return out;
	}
	
	
	
	
}
