package processAirData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.SerializationException;


/**
 * Reads a CSV or JSON file for airline data, processes it, and sends it to Kafka.
 * @author Kevin Moss
 *
 */
public class ReadOpenSkyFile {
	
	private static File f;
	
	private static int[] csvOrder = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
	private static boolean csvOrderDefault = true; //Code is optimized for default order.
	
	private static KafkaProducer<Object, Object> producer;
	
	/**
	 * The main method finds and reads the file, creates FlightState objects, and passes
	 * them as messages to Kafka.
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		//Get the file
		if(args.length < 1) {
			throw new IllegalArgumentException("A file must be specified.");
		} else if (args.length > 1) {
			throw new IllegalArgumentException("Too many ("+args.length+") arguments received.");
		}
		f = new File(args[0]);
		if(!f.isFile()) {
			throw new FileNotFoundException("No file found at path \""+args[0]+"\" or path is a directory.");
		}
		
		//Make the Kafka producer object
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put("schema.registry.url", "http://localhost:8081");
		producer = new KafkaProducer<>(props);
		
		//Read the file, and send messages to Kafka
		if(args[0].endsWith(".csv")) {
			readCSV();
		} else if(args[0].endsWith(".json")) {
			readJSON();
		} else {
			throw new IllegalArgumentException("Only .csv and .json files are accepted");
		}
		
		producer.close();
	}
	
	/**
	 * Reads a Comma-Separated-Values (CSV) file and sends the information to Kafka
	 * @throws IOException
	 */
	public static void readCSV() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(f));
		String line;
		if((line = br.readLine()) != null) {
			setCSVorder(line);
		}
		
		while((line = br.readLine()) != null) {
			FlightState fs = getStateFromCSV(line);
			sendToKafka(fs);
		}
		
		br.close();
	}
	
	/**
	 * Sets the order of the variables read in the CSV file.
	 * The first line in a CSV file should be a list of variable names.
	 * @param firstLine A line of variable names, separated by commas.
	 */
	public static void setCSVorder(String firstLine) {
		String[] vars = firstLine.split(",");
		csvOrder = new int[vars.length];
		csvOrderDefault = false;
		for(int i = 0; i < vars.length; i++) {
			vars[i] = vars[i].trim();
			switch(vars[i]) {
			case "time":csvOrder[i]=0; break;
			case "icao24":csvOrder[i]=1; break;
			case "lat":csvOrder[i]=2; break;
			case "lon":csvOrder[i]=3; break;
			case "velocity":csvOrder[i]=4; break;
			case "heading":csvOrder[i]=5; break;
			case "vertrate":csvOrder[i]=6; break;
			case "callsign":csvOrder[i]=7; break;
			case "onground":csvOrder[i]=8; break;
			case "alert":csvOrder[i]=9; break;
			case "spi":csvOrder[i]=10; break;
			case "squawk":csvOrder[i]=11; break;
			case "baroaltitude":csvOrder[i]=12; break;
			case "geoaltitude":csvOrder[i]=13; break;
			case "lastposupdate":csvOrder[i]=14; break;
			case "lastcontact":csvOrder[i]=15; break;
			default: break; //Other variables are ignored
			}
		}
		//if order is 0,1,2,...,15; then use optimized settings
		if(vars.length == 16) {
			csvOrderDefault = true;
			for(int i = 0; i < 16; i++) {
				if(csvOrder[i] != i) {
					csvOrderDefault = false;
					break;
				}
			}
		}
		
	}
	
	/**
	 * Reads a line in a CSV file and returns the flight state corresponding to that file
	 * @param line
	 * @return
	 */
	public static FlightState getStateFromCSV(String line) {
		String[] lineParts = line.split(",");
		
		if(!csvOrderDefault) {
			String[] newParts = new String[16];
			for(int i = 0; i < lineParts.length; i++) {
				newParts[csvOrder[i]] = lineParts[i];
			}
			lineParts = newParts;
		}
		return new FlightState(lineParts);
	}
	
	
	/**
	 * Reads a Javascript Object Notation (JSON) file and sends the data to Kafka.
	 * This currently only supports records of live OpenSky data found at
	 * "https://opensky-network.org/api/states/all". It does not support
	 * JSON files found at "https://opensky-network.org/datasets/states/".
	 * @throws IOException 
	 */
	public static void readJSON() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(f));
		String line;
		if((line = br.readLine()) != null) {
			String [] flightStates = parseJSON(line);
			for(String state : flightStates) {
				FlightState fs = new FlightState(state);
				sendToKafka(fs);
			}
		}
		br.close();
	}
	
	/**
	 * Parses JSON data corresponding to a live state
	 * @param line A (long) String corresponding to a JSON file
	 * @return An array of strings corresponding to flight states
	 */
	public static String[] parseJSON(String line) {
		String temp = line.replaceFirst("\\{\"time\":[0-9]+,\"states\":\\[\\[", "");
		String[] states = temp.split("\\],\\[");
		states[states.length-1].replaceAll("\\]\\]\\}", "");
		return states;
	}
	
	
	
	/**
	 * Sends messages to Kafka for a given flight state.
	 * For faster user-side queries, messages are sent for multiple Geohash resolutions.
	 * @param fs A FlightState
	 */
	public static void sendToKafka(FlightState fs) {
		try {
			producer.send(FlightState.kafkaAvroRecord(fs, 4));
			producer.send(FlightState.kafkaAvroRecord(fs, 5));
			producer.send(FlightState.kafkaAvroRecord(fs, 6));
			producer.send(FlightState.kafkaAvroRecord(fs, 7));
		} catch(SerializationException e) {
			System.out.println("Serialization Exception: "+e);
		} catch(IllegalArgumentException e) {
			// Incomplete flight state is skipped without notification
		}
	}
	
	

}
