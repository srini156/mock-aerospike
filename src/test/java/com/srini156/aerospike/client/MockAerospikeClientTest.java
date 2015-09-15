package com.srini156.aerospike.client;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.srini156.aerospike.client.MockAerospikeClient;
import org.testng.annotations.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * @author srinivas.iyengar
 *
 */
public class MockAerospikeClientTest {

	private IAerospikeClient mockAerospikeClient = new MockAerospikeClient();
	private String namespace = "test";
	private String set = "test_set";
	private Key key = new Key(namespace, set, "Key");
	private Bin[] bins = new Bin[] { new Bin("first", "first"), new Bin("second", 123L) };

	@Test
	public void shouldPutRecord() {
		mockAerospikeClient.put(null, key, bins);
	}

	@Test(dependsOnMethods = "shouldPutRecord")
	public void shouldGetRecord() {
		Record record = mockAerospikeClient.get(null, key);
		assertNotNull(record);
	}

	@Test(dependsOnMethods = "shouldGetRecord")
	public void shouldDeleteRecord() {
		assertTrue(mockAerospikeClient.delete(null, key));
	}

	@Test(dependsOnMethods = "shouldDeleteRecord")
	public void shouldNotGetRecord() {
		Record record = mockAerospikeClient.get(null, key);
		assertNull(record);
	}

	@Test
	public void shouldNotDeleteNonExistingRecord() {
		Key nonExistingKey = new Key(namespace, set, "Non-key");
		assertFalse(mockAerospikeClient.delete(null, nonExistingKey));

	}
}
