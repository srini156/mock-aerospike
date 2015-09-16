package com.srini156.aerospike.client;

import org.srini156.aerospike.client.MockAerospikeClient;

/**
 * Tests to be run against MockAerospikeClient to ensure functionality of IAerospikeClient is
 * implemented.
 * 
 * @author srinivas.iyengar
 *
 */
public class MockAerospikeClientTest extends BaseAerospikeClientTest {

	/**
	 * Default Constructor.
	 */
	public MockAerospikeClientTest() {
		super(new MockAerospikeClient());
	}

}
