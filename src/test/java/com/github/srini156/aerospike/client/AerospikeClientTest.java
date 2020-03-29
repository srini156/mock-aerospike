package comy.github.srini156.aerospike.client;

import com.aerospike.client.AerospikeClient;
import com.github.srini156.aerospike.client.BaseAerospikeClientTest;

/**
 * Running the BaseTests on actual aerospike box to verify the implementation of MockAerospike.
 *
 * @author srinivas.iyengar
 */
public class AerospikeClientTest extends BaseAerospikeClientTest {

    /**
     * Default Constructor.
     */
    public AerospikeClientTest() {

        // aerospike-box => Hostname of the aerospike box.
        // Port number => 3000
        super(new AerospikeClient("localhost", 3000));
    }
}
