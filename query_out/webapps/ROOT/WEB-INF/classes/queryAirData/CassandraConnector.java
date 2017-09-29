package queryAirData;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

/**
 * A cluster object used to connect to Cassandra.
 * @author Kevin Moss
 *
 */
public class CassandraConnector {
	// Cassandra Cluster
	private Cluster cluster;
	// Cassandra Session
	private Session session;
	
	public void connect(final String node, final int port) {
		cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
		final Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster %s\n", metadata.getClusterName());
		for (final Host host : metadata.getAllHosts()) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		session = cluster.connect();
	}
	
	public Session getSession() {
		return session;
	}
	public void close() {
		cluster.close();
	}

}
