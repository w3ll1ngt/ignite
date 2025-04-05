import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.ClientCache;

public class SimpleIgniteClient {
	public static void main(String[] args) {
		IgniteClientConfiguration cfg = new IgniteClientConfiguration()
				.setAddresses("127.0.0.1:10800");
		try (IgniteClient client = Ignition.startClient(cfg)) {
			System.out.println("✅ Connected to Apache Ignite!");

			ClientCache<Integer, String> cache = client.getOrCreateCache("myCache");

			cache.put(1, "Hello");
			cache.put(2, "Ignite");

		}
	}
}
