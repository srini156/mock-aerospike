package com.srini156.aerospike.client;

import com.aerospike.client.AerospikeClient;

public class AerospikeClientTest extends BaseAerospikeTest {

	/**
	 * Running the BaseTests on actual aerospike box to verify the implementation of MockAerospike.
	 */
	public AerospikeClientTest() {

		// aerospike-box => Hostname of the aerospike box.
		// Port number => 3000
		super(new AerospikeClient("aerospike-box", 3000));
	}
}
