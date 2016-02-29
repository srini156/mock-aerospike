package com.github.srini156.aerospike.client;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public abstract class BaseAerospikeClientTest {

	protected IAerospikeClient aerospikeClient;
	protected String namespace = "test";
	protected String set = "test_set";
	protected Key key1 = new Key(namespace, set, "Key1");
	protected Bin[] bins1 = new Bin[] { new Bin("first", "first-1"),
			new Bin("second", 123L), new Bin("third", "third-1") };
	protected Key key2 = new Key(namespace, set, "Key2");
	protected Bin[] bins2 = new Bin[] { new Bin("first", "first-2"),
			new Bin("second", 124L), new Bin("third", "third-2") };
	protected Key nonExistingKey = new Key(namespace, set, "Non-key");
	protected Key key3 = new Key(namespace, set, "Key3");

	public BaseAerospikeClientTest(IAerospikeClient client) {
		this.aerospikeClient = client;
	}

	@Test
	public void shouldPutRecord() {
		aerospikeClient.put(null, key1, bins1);
	}

	@Test(dependsOnMethods = "shouldPutRecord")
	public void shouldGetRecord() {
		Record record = aerospikeClient.get(null, key1);
		assertNotNull(record);
		assertEquals(record.bins.size(), 3);
		assertTrue(record.bins.containsKey("first"));
		assertEquals(record.bins.get("first"), "first-1");
		assertTrue(record.bins.containsKey("second"));
		assertEquals(record.bins.get("second"), 123L);
		assertTrue(record.bins.containsKey("third"));
		assertEquals(record.bins.get("third"), "third-1");
	}

	@Test(dependsOnMethods = "shouldGetRecord")
	public void shouldGetHeader() {
		Record record = aerospikeClient.getHeader(null, key1);
		assertNotNull(record);
		assertNotNull(record.generation);
		assertNotNull(record.expiration);
		assertNull(record.bins);
	}

	@Test(dependsOnMethods = "shouldGetHeader")
	public void shouldDeleteRecord() {
		assertTrue(aerospikeClient.delete(null, key1));
	}

	@Test(dependsOnMethods = "shouldDeleteRecord")
	public void shouldNotGetRecord() {
		Record record = aerospikeClient.get(null, key1);
		assertNull(record);
	}

	@Test(dependsOnMethods = "shouldNotGetRecord")
	public void shouldPutMultipleRecords() {
		aerospikeClient.put(null, key1, bins1);
		aerospikeClient.put(null, key2, bins2);
	}

	@Test(dependsOnMethods = "shouldPutMultipleRecords")
	public void shouldExistRecord() {
		assertTrue(aerospikeClient.exists(null, key1));
		assertTrue(aerospikeClient.exists(null, key2));
	}

	@Test(dependsOnMethods = "shouldExistRecord")
	public void shouldExistRecords() {
		boolean[] result = aerospikeClient.exists(null, new Key[] { key1, key2,
				nonExistingKey });
		assertEquals(result.length, 3);
		assertTrue(result[0]);
		assertTrue(result[1]);
		assertFalse(result[2]);
	}

	@Test(dependsOnMethods = "shouldExistRecords")
	public void shouldGetHeaders() {
		Record[] records = aerospikeClient.getHeader(null, new Key[] { key1,
				key2, nonExistingKey });
		assertEquals(records.length, 3);
		assertNotNull(records[0]);
		assertNotNull(records[1]);
		assertNull(records[2]);
	}

	@Test(dependsOnMethods = "shouldGetHeaders")
	public void shouldGetMultipleRecords() {
		Record[] records = aerospikeClient.get(null, new Key[] { key1, key2,
				nonExistingKey });
		assertEquals(records.length, 3);
		// Assert first record
		Record firstRecord = records[0];
		assertEquals(firstRecord.bins.size(), 3);
		assertTrue(firstRecord.bins.containsKey("first"));
		assertEquals(firstRecord.bins.get("first"), "first-1");
		assertTrue(firstRecord.bins.containsKey("second"));
		assertEquals(firstRecord.bins.get("second"), 123L);
		assertTrue(firstRecord.bins.containsKey("third"));
		assertEquals(firstRecord.bins.get("third"), "third-1");
		// Assert second record
		Record secondRecord = records[1];
		assertEquals(secondRecord.bins.size(), 3);
		assertTrue(secondRecord.bins.containsKey("first"));
		assertEquals(secondRecord.bins.get("first"), "first-2");
		assertTrue(secondRecord.bins.containsKey("second"));
		assertEquals(secondRecord.bins.get("second"), 124L);
		assertTrue(secondRecord.bins.containsKey("third"));
		assertEquals(secondRecord.bins.get("third"), "third-2");
		// Assert Third record which is null.
		assertNull(records[2]);
	}

	@Test
	public void shouldGetBooleanBins() {
		Bin[] boolBins = new Bin[] { new Bin("boolean_true", true),
				new Bin("boolean_false", false) };
		aerospikeClient.put(null, key3, boolBins);
		Record record = aerospikeClient.get(null, key3);
		assertEquals(record.bins.size(), 2);
		assertEquals(record.bins.get("boolean_true"), 1L);
		assertEquals(record.bins.get("boolean_false"), 0L);
	}

	@Test(dependsOnMethods = "shouldGetMultipleRecords")
	public void shouldGetMultipleRecordsFilteredBins() {
		Record[] records = aerospikeClient.get(null, new Key[] { key1, key2,
				nonExistingKey }, "first", "second");
		assertEquals(records.length, 3);
		// Assert first record
		Record firstRecord = records[0];
		assertEquals(firstRecord.bins.size(), 2);
		assertTrue(firstRecord.bins.containsKey("first"));
		assertEquals(firstRecord.bins.get("first"), "first-1");
		assertTrue(firstRecord.bins.containsKey("second"));
		assertEquals(firstRecord.bins.get("second"), 123L);
		// Assert second record
		Record secondRecord = records[1];
		assertEquals(secondRecord.bins.size(), 2);
		assertTrue(secondRecord.bins.containsKey("first"));
		assertEquals(secondRecord.bins.get("first"), "first-2");
		assertTrue(secondRecord.bins.containsKey("second"));
		assertEquals(secondRecord.bins.get("second"), 124L);
		// Assert Third record which is null.
		assertNull(records[2]);
	}

	@Test
	public void shouldNotDeleteNonExistingRecord() {
		assertFalse(aerospikeClient.delete(null, nonExistingKey));
	}

	@Test
	public void shouldNotGetNonExistingHeader() {
		assertNull(aerospikeClient.getHeader(null, nonExistingKey));
	}

}
