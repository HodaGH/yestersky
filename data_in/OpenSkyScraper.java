
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Scrapes live data from The OpenSky Network. This class is meant for demonstrative 
 * purposes only. Please scrape responsibly.
 * 
 *  Live data from the network is found at "https://opensky-network.org/api/states/all",
 *  and includes data retrieved by the network with a time resolution of 10 seconds.
 *  This class scrapes and stores the data in a JSON file periodically.
 *  By default, the period is 120 seconds, but this can be changed.
 *  
 *  As is, the scraper scrapes indefinitely. The method needs to be killed manually.
 *  
 * @author Kevin Moss
 *
 */
public class OpenSkyScraper {
	
	/**
	 * Executes the data scraper. Optionally, you 
	 * 
	 * @param args
	 * @throws UnknownHostException
	 * @throws InterruptedException
	 * @throws MalformedURLException
	 */
	public static void main(String[] args) throws UnknownHostException,
	InterruptedException, MalformedURLException {
		final String outDir = args.length > 1 ? args[1] : "."+File.separator+"OpenSkyData";
		int timeBetweenScrapes = args.length > 0 ? Integer.parseInt(args[0]) : 120;
		assert timeBetweenScrapes > 0;
		
		new File(outDir).mkdirs();
		
		ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
		Runnable downloader = new Runnable () {
			public void run() { try {
				getOpenSkyData(outDir);
			} catch (IOException e) {
				System.out.println(System.currentTimeMillis()/1000+" "+e);
			} }
		};
		
		ses.scheduleAtFixedRate(downloader,	0, timeBetweenScrapes, TimeUnit.SECONDS);
	}
	
	/**
	 * This method is the scraping task that executes periodically.
	 * @param outDir The directory that the data is sent to.
	 * @throws IOException
	 */
	private static void getOpenSkyData(String outDir) throws IOException {
		long currentTime = System.currentTimeMillis()/1000;//current UNIX timestamp
		String fileName = "OpenSkyState"+Long.toString(currentTime);
		
		URL u = new URL("https://opensky-network.org/api/states/all");
		ReadableByteChannel rbc = Channels.newChannel(u.openStream());
		FileOutputStream fos = new FileOutputStream(outDir+File.separator+fileName+".json");
		fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
		fos.close();
	}
}
