package com.srini156.aerospike.client;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

public abstract class BaseAerospikeClientTest {

	protected IAerospikeClient aerospikeClient;
	protected String namespace = "test";
	protected String set = "test_set";
	protected Key key = new Key(namespace, set, "Key");
	protected Bin[] bins = new Bin[] { new Bin("first", "first"), new Bin("second", 123L) };

	public BaseAerospikeClientTest(IAerospikeClient client) {
		this.aerospikeClient = client;
	}

	@Test
	public void shouldPutRecord() {
		aerospikeClient.put(null, key, bins);
	}

	@Test(dependsOnMethods = "shouldPutRecord")
	public void shouldGetRecord() {
		Record record = aerospikeClient.get(null, key);
		assertNotNull(record);
		assertEquals(record.bins.size(), 2);
		assertTrue(record.bins.containsKey("first"));
		assertEquals(record.bins.get("first"), "first");
		assertTrue(record.bins.containsKey("second"));
		assertEquals(record.bins.get("second"), 123L);
	}

	@Test(dependsOnMethods = "shouldGetRecord")
	public void shouldGetHeader() {
		Record record = aerospikeClient.getHeader(null, key);
		assertNotNull(record);
		assertNull(record.bins);
	}

	@Test(dependsOnMethods = "shouldGetHeader")
	public void shouldDeleteRecord() {
		assertTrue(aerospikeClient.delete(null, key));
	}

	@Test(dependsOnMethods = "shouldDeleteRecord")
	public void shouldNotGetRecord() {
		Record record = aerospikeClient.get(null, key);
		assertNull(record);
	}

	@Test
	public void shouldNotDeleteNonExistingRecord() {
		Key nonExistingKey = new Key(namespace, set, "Non-key");
		assertFalse(aerospikeClient.delete(null, nonExistingKey));

	}
}
