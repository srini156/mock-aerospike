package com.github.srini156.aerospike.client;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
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
	protected Bin[] bins4 = new Bin[] { new Bin("first", "val"), new Bin("second", 1.0),
										new Bin("third", 1)};
	protected Key nonExistingKey = new Key(namespace, set, "Non-key");
	protected Key key3 = new Key(namespace, set, "Key3");
	protected Key key4 = new Key(namespace, set, "Temp Key");
	public BaseAerospikeClientTest(IAerospikeClient client) {
		this.aerospikeClient = client;
	}

	@Test
	public void shouldPutRecord() {
		aerospikeClient.put(null, key1, bins1);
	}

    @Test
	public void shouldOperateRecord() {
		Record record = aerospikeClient.operate(
                null,key1, Operation.put(new Bin("test1", "test")), Operation.put(new Bin("test2", "test"))
        );
        assertTrue(record.bins.containsKey("test1"));
        assertTrue(record.bins.containsKey("test2"));
	}

	@Test(dependsOnMethods = "shouldPutRecord")
	public void shouldGetRecord() {
		Record record = aerospikeClient.get(null, key1);
		assertNotNull(record);
		assertEquals(record.bins.size(), 5);
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

	@Test(dependsOnMethods = "shouldGetRecord")
	public void shouldTouchRecord() {
		WritePolicy writePolicy = new WritePolicy();
		writePolicy.expiration = 1000;
		int expiration = aerospikeClient.get(null, key1).expiration;
		aerospikeClient.touch(writePolicy, key1);
		int new_expiration = aerospikeClient.get(null, key1).expiration;
		assertFalse(expiration == new_expiration);
	}

	@Test(dependsOnMethods = "shouldNotGetRecord")
	public void shouldThrowOnTouchRecord() {
		assertThrows(AerospikeException.class, () -> {
			WritePolicy writePolicy = new WritePolicy();
			writePolicy.expiration = 1000;
			aerospikeClient.touch(writePolicy, nonExistingKey);
		});
	}

	@Test
	public void putTempKey() {
		aerospikeClient.put(null, key4, bins4);
	}

	@Test(dependsOnMethods = "putTempKey")
	public void shouldGetTempKey() {
		assertTrue(aerospikeClient.exists(null, key4));
	}

	@Test(dependsOnMethods = {"shouldGetTempKey"})
	public void shouldAppendIfRecordExists() {
		assertEquals(aerospikeClient.get(null, key4).bins.get("first"), "val");
		aerospikeClient.append(null, key4, new Bin("first", "ue"));
		assertEquals(aerospikeClient.get(null, key4).bins.get("first"), "value");

		// Undo changes
		aerospikeClient.get(null, key4).bins.put("first", "val");
	}

	@Test(dependsOnMethods="shouldGetTempKey")
	public void shouldThrowOnNonStringAppendRecord() {
		assertThrows(AerospikeException.class, () -> {
			aerospikeClient.append(null, key4, new Bin("second", 2));
		});
	}

	@Test(dependsOnMethods = "shouldGetTempKey")
	public void shouldAddRecord() {
		aerospikeClient.put(null, key4, new Bin("third", 1));
		Record record = aerospikeClient.get(null, key4);
		assertNotNull(record);
		assertTrue(record.bins.containsKey("third"));
		assertEquals(record.bins.get("third"), 1);
		aerospikeClient.add(null, key4, new Bin("third", 1));
		assertEquals(aerospikeClient.get(null, key4).bins.get("third"), 2);

		// Undo changes
		aerospikeClient.get(null, key4).bins.put("third", 1);
	}

	@Test
	public void shouldThrowOnNonIntAddRecord() {
		assertThrows(AerospikeException.class, () -> {
			aerospikeClient.add(null, key4, new Bin("second", 1));
		});
	}

	@Test(dependsOnMethods = "shouldGetTempKey")
	public void shouldAppendIfRecordDoesNotExist() {
		aerospikeClient.delete(null, key4);
		Record record = aerospikeClient.get(null, key4);
		assertNull(record);
		aerospikeClient.append(null, key4, new Bin("first", "val"));
		assertEquals(aerospikeClient.get(null, key4).bins.get("first"), "val");
		putTempKey();
	}

}
