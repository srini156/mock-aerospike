package com.github.srini156.aerospike.client;

import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.AerospikeException.InvalidNode;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.Value.BooleanValue;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.*;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.*;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.google.common.collect.Lists;

/**
 * Mock Implementation of IAerospikeClient to allow cleaner unit testing.
 *
 * @author srinivas.iyengar
 */
public class MockAerospikeClient implements IAerospikeClient {
    private final Map<Key, Record> data = new ConcurrentHashMap<>();

    /**
     * Close all client connections to database server nodes.
     */
    public void close() {
        data.clear();
    }

    /**
     * Determine if we are ready to talk to the database server cluster. <br>
     * Note: Mock always returns true.
     *
     * @return <code>true</code> if cluster is ready, <code>false</code> if
     * cluster is not ready
     */
    public boolean isConnected() {
        return true;
    }

    /**
     * Return array of active server nodes in the cluster. <br>
     * Always returns null - not implemented.
     *
     * @return array of active nodes
     */
    public Node[] getNodes() {
        return new Node[]{};
    }

    /**
     * Return list of active server node names in the cluster.
     *
     * @return list of active node names
     */
    public List<String> getNodeNames() {
        return Lists.newLinkedList();
    }

    /**
     * Return node given its name.
     *
     * @throws AerospikeException.InvalidNode if node does not exist.
     */
    public Node getNode(String nodeName) throws InvalidNode {
        throw new InvalidNode("Node not available");
    }

    @Override
    public ClusterStats getClusterStats() {
        return null;
    }

    /**
     * Write record bin(s). The policy specifies the transaction timeout, record
     * expiration and how the transaction is handled when the record already
     * exists.
     *
     * @param policy write configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @param bins   array of bin name/value pairs
     * @throws AerospikeException if write fails
     */
    public void put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        data.put(key, new Record(convertToMap(bins), 0, 0));
    }

    @Override
    public void put(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {

    }

    /**
     * Append bin string values to existing record bin values. The policy
     * specifies the transaction timeout, record expiration and how the
     * transaction is handled when the record already exists. This call only
     * works for string values.
     *
     * @param policy write configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @param bins   array of bin name/value pairs
     * @throws AerospikeException if append fails
     */
    public void append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        // If Key is not present, create a new record.
        // If Bin is present but not a string, throw
        // com.aerospike.client.AerospikeException: Error Code 12: Bin type
        // error
        // else, append the string.
        if (!data.containsKey(key)) {
            put(policy, key, bins);
        } else {
            final Map<String, Object> recordBins = data.get(key).bins;
            for (final Bin bin : bins) {
                recordBins.computeIfPresent(bin.name, (s, o) -> {
                    if (o instanceof String) {
                        return o.toString() + bin.value.toString();
                    } else {
                        throw new AerospikeException("Error Code 12: Bin type");
                    }
                });
            }
        }
    }

    @Override
    public void append(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {

    }

    /**
     * Prepend bin string values to existing record bin values. The policy
     * specifies the transaction timeout, record expiration and how the
     * transaction is handled when the record already exists. This call works
     * only for string values.
     *
     * @param policy write configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @param bins   array of bin name/value pairs
     * @throws AerospikeException if prepend fails
     */
    public void prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        if (!data.containsKey(key)) {
            put(policy, key, bins);
        } else {
            final Map<String, Object> recordBins = data.get(key).bins;
            for (final Bin bin : bins) {
                recordBins.computeIfPresent(bin.name, (s, o) -> {
                    if (o instanceof String) {
                        return bin.value.toString() + o;
                    } else {
                        throw new AerospikeException("Error Code 12: Bin type");
                    }
                });
            }
        }
    }

    @Override
    public void prepend(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {

    }

    /**
     * Add integer bin values to existing record bin values. The policy
     * specifies the transaction timeout, record expiration and how the
     * transaction is handled when the record already exists. This call only
     * works for integer values.
     *
     * @param policy write configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @param bins   array of bin name/value pairs
     * @throws AerospikeException if add fails
     */
    public void add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        if (!data.containsKey(key)) {
            put(policy, key, bins);
        } else {
            Map<String, Object> recordBins = data.get(key).bins;
            for (Bin bin : bins) {
                recordBins.computeIfPresent(bin.name, (s, o) -> {
                    if (o instanceof Integer) {
                        return bin.value.toInteger() + ((Integer) o);
                    } else if (o instanceof Long) {
                        return bin.value.toLong() + ((Long) o);
                    } else {
                        throw new AerospikeException("Error Code 12: Bin type");
                    }
                });
            }
        }
    }

    @Override
    public void add(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key, Bin... bins) throws AerospikeException {

    }

    /**
     * Delete record for specified key. The policy specifies the transaction
     * timeout.
     *
     * @param policy delete configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @return whether record existed on server before deletion
     * @throws AerospikeException if delete fails
     */
    public boolean delete(WritePolicy policy, Key key) throws AerospikeException {
        if (data.containsKey(key)) {
            data.remove(key);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void delete(EventLoop eventLoop, DeleteListener deleteListener, WritePolicy writePolicy, Key key) throws AerospikeException {

    }

    @Override
    public void truncate(InfoPolicy infoPolicy, String s, String s1, Calendar calendar) throws AerospikeException {

    }

    /**
     * Reset record's time to expiration using the policy's expiration. Fail if
     * the record does not exist.
     *
     * @param policy write configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @throws AerospikeException if touch fails
     */
    public void touch(WritePolicy policy, Key key) throws AerospikeException {
        if (data.containsKey(key)) {
            final Record record = data.remove(key);
            data.put(key, new Record(record.bins, record.generation, policy.expiration));
        } else {
            throw new AerospikeException("Error Code 2: Key not found");
        }
    }

    @Override
    public void touch(EventLoop eventLoop, WriteListener writeListener, WritePolicy writePolicy, Key key) throws AerospikeException {

    }

    /**
     * Determine if a record key exists. The policy can be used to specify
     * timeouts.
     *
     * @param policy generic configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @return whether record exists or not
     * @throws AerospikeException if command fails
     */
    public boolean exists(Policy policy, Key key) throws AerospikeException {
        return data.containsKey(key);
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsListener existsListener, Policy policy, Key key) throws AerospikeException {

    }

    /**
     * Check if multiple record keys exist in one batch call. The returned
     * boolean array is in positional order with the original key array order.
     * The policy can be used to specify timeouts.
     *
     * @param policy generic configuration parameters, pass in null for defaults
     * @param keys   array of unique record identifiers
     * @return array key/existence status pairs
     * @throws AerospikeException if command fails
     * @deprecated Use {@link #exists(BatchPolicy, Key[])} instead.
     */
    @Deprecated
    public boolean[] exists(Policy policy, Key[] keys) throws AerospikeException {
        boolean[] result = new boolean[keys.length];
        for (int idx = 0; idx < keys.length; idx++) {
            result[idx] = data.containsKey(keys[idx]);
        }
        return result;
    }

    /**
     * Check if multiple record keys exist in one batch call. The returned
     * boolean array is in positional order with the original key array order.
     * The policy can be used to specify timeouts and maximum concurrent
     * threads.
     *
     * @param policy batch configuration parameters, pass in null for defaults
     * @param keys   array of unique record identifiers
     * @return array key/existence status pairs
     * @throws AerospikeException if command fails
     */
    public boolean[] exists(BatchPolicy policy, Key[] keys) throws AerospikeException {
        return exists((Policy) policy, keys);
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsArrayListener existsArrayListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {

    }

    @Override
    public void exists(EventLoop eventLoop, ExistsSequenceListener existsSequenceListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {

    }

    /**
     * Read entire record for specified key. The policy can be used to specify
     * timeouts.
     *
     * @param policy generic configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @return if found, return record instance. If not found, return null.
     * @throws AerospikeException if read fails
     */
    public Record get(Policy policy, Key key) throws AerospikeException {
        return data.get(key);
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener recordListener, Policy policy, Key key) throws AerospikeException {

    }

    /**
     * Read record header and bins for specified key. The policy can be used to
     * specify timeouts.
     *
     * @param policy   generic configuration parameters, pass in null for defaults
     * @param key      unique record identifier
     * @param binNames bins to retrieve
     * @return if found, return record instance. If not found, return null.
     * @throws AerospikeException if read fails
     */
    public Record get(Policy policy, Key key, String... binNames) throws AerospikeException {
        final Record record = data.get(key);
        if (record == null) {
            return null;
        } else {
            // filter bins.
            Map<String, Object> filteredBins = new HashMap<>();
            for (String bin : binNames) {
                filteredBins.put(bin, record.bins.get(bin));
            }
            return new Record(filteredBins, record.generation, record.expiration);
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener recordListener, Policy policy, Key key, String... strings) throws AerospikeException {

    }

    /**
     * Read record generation and expiration only for specified key. Bins are
     * not read. The policy can be used to specify timeouts.
     *
     * @param policy generic configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @return if found, return record instance. If not found, return null.
     * @throws AerospikeException if read fails
     */
    public Record getHeader(Policy policy, Key key) throws AerospikeException {
        Record record = data.get(key);

        if (record == null) {
            return null;
        } else {
            return new Record(null, record.generation, record.expiration);
        }

    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordListener recordListener, Policy policy, Key key) throws AerospikeException {

    }

    /**
     * Read multiple records for specified keys in one batch call. The returned
     * records are in positional order with the original key array order. If a
     * key is not found, the positional record will be null. The policy can be
     * used to specify timeouts.
     *
     * @param policy generic configuration parameters, pass in null for defaults
     * @param keys   array of unique record identifiers
     * @return array of records
     * @throws AerospikeException if read fails
     * @deprecated Use {@link #get(BatchPolicy, Key[])} instead.
     */
    @Deprecated
    public Record[] get(Policy policy, Key[] keys) throws AerospikeException {
        Record[] records = new Record[keys.length];
        for (int idx = 0; idx < records.length; idx++) {
            records[idx] = get(policy, keys[idx]);
        }
        return records;
    }

    /**
     * Read multiple records for specified keys in one batch call. The returned
     * records are in positional order with the original key array order. If a
     * key is not found, the positional record will be null. The policy can be
     * used to specify timeouts and maximum concurrent threads.
     *
     * @param policy batch configuration parameters, pass in null for defaults
     * @param keys   array of unique record identifiers
     * @return array of records
     * @throws AerospikeException if read fails
     */
    public Record[] get(BatchPolicy policy, Key[] keys) throws AerospikeException {
        return get((Policy) policy, keys);
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener recordArrayListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {

    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {

    }

    /**
     * Read multiple record headers and bins for specified keys in one batch
     * call. The returned records are in positional order with the original key
     * array order. If a key is not found, the positional record will be null.
     * The policy can be used to specify timeouts.
     *
     * @param policy   generic configuration parameters, pass in null for defaults
     * @param keys     array of unique record identifiers
     * @param binNames array of bins to retrieve
     * @return array of records
     * @throws AerospikeException if read fails
     * @deprecated Use {@link #get(BatchPolicy, Key[], String...)} instead.
     */
    @Deprecated
    public Record[] get(Policy policy, Key[] keys, String... binNames) throws AerospikeException {
        Record[] records = new Record[keys.length];
        for (int idx = 0; idx < records.length; idx++) {
            records[idx] = get(policy, keys[idx], binNames);
        }
        return records;
    }

    /**
     * Read multiple record headers and bins for specified keys in one batch
     * call. The returned records are in positional order with the original key
     * array order. If a key is not found, the positional record will be null.
     * The policy can be used to specify timeouts and maximum concurrent
     * threads.
     *
     * @param policy   batch configuration parameters, pass in null for defaults
     * @param keys     array of unique record identifiers
     * @param binNames array of bins to retrieve
     * @return array of records
     * @throws AerospikeException if read fails
     */
    public Record[] get(BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
        return get((Policy) policy, keys, binNames);
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener recordArrayListener, BatchPolicy batchPolicy, Key[] keys, String... strings) throws AerospikeException {

    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, BatchPolicy batchPolicy, Key[] keys, String... strings) throws AerospikeException {

    }

    /**
     * Read multiple record header data for specified keys in one batch call.
     * The returned records are in positional order with the original key array
     * order. If a key is not found, the positional record will be null. The
     * policy can be used to specify timeouts.
     *
     * @param policy generic configuration parameters, pass in null for defaults
     * @param keys   array of unique record identifiers
     * @return array of records
     * @throws AerospikeException if read fails
     * @deprecated Use {@link #getHeader(BatchPolicy, Key[])} instead.
     */
    @Deprecated
    public Record[] getHeader(Policy policy, Key[] keys) throws AerospikeException {
        Record[] records = new Record[keys.length];
        for (int idx = 0; idx < records.length; idx++) {
            records[idx] = getHeader(policy, keys[idx]);
        }
        return records;
    }

    /**
     * Read multiple record header data for specified keys in one batch call.
     * The returned records are in positional order with the original key array
     * order. If a key is not found, the positional record will be null. The
     * policy can be used to specify timeouts and maximum concurrent threads.
     *
     * @param policy batch configuration parameters, pass in null for defaults
     * @param keys   array of unique record identifiers
     * @return array of records
     * @throws AerospikeException if read fails
     */
    public Record[] getHeader(BatchPolicy policy, Key[] keys) throws AerospikeException {
        return getHeader((Policy) policy, keys);
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordArrayListener recordArrayListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {

    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, BatchPolicy batchPolicy, Key[] keys) throws AerospikeException {

    }

    /**
     * Perform multiple read/write operations on a single key in one batch call.
     * An example would be to add an integer value to an existing record and
     * then read the result, all in one database call.
     * <p>
     * Write operations are always performed first, regardless of operation
     * order relative to read operations.
     *
     * @param policy     write configuration parameters, pass in null for defaults
     * @param key        unique record identifier
     * @param operations database operations to perform
     * @return record if there is a read in the operations list
     * @throws AerospikeException if command fails
     */
    public Record operate(WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
        Map<String, Object> bins = new HashMap<>();
        //Add everything
        for (Operation operation : operations) {
            switch (operation.type) {
                case ADD:
                case APPEND:
                case WRITE:
                case PREPEND:
                    bins.put(operation.binName, operation.value.getObject());
            }
        }

        if (data.containsKey(key)) {
            data.get(key).bins.putAll(bins);
        } else {
            data.put(key, new Record(bins, 0, 0));
        }
        return data.get(key);
    }

    @Override
    public void operate(EventLoop eventLoop, RecordListener recordListener, WritePolicy writePolicy, Key key, Operation... operations) throws AerospikeException {

    }

    /**
     * Read all records in specified namespace and set. If the policy's
     * <code>concurrentNodes</code> is specified, each server node will be read
     * in parallel. Otherwise, server nodes are read in series.
     * <p>
     * This call will block until the scan is complete - callbacks are made
     * within the scope of this call.
     *
     * @param policy    scan configuration parameters, pass in null for defaults
     * @param namespace namespace - equivalent to database name
     * @param setName   optional set name - equivalent to database table
     * @param callback  read callback method - called with record data
     * @param binNames  optional bin to retrieve. All bins will be returned if not
     *                  specified. Aerospike 2 servers ignore this parameter.
     * @throws AerospikeException if scan fails
     */
    public void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback, String... binNames) throws AerospikeException {
        throw new UnsupportedOperationException("scanAll is not supported in MockAerospike");

    }

    @Override
    public void scanAll(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, ScanPolicy scanPolicy, String s, String s1, String... strings) throws AerospikeException {

    }

    /**
     * Read all records in specified namespace and set for one node only. The
     * node is specified by name.
     * <p>
     * This call will block until the scan is complete - callbacks are made
     * within the scope of this call.
     *
     * @param policy    scan configuration parameters, pass in null for defaults
     * @param nodeName  server node name
     * @param namespace namespace - equivalent to database name
     * @param setName   optional set name - equivalent to database table
     * @param callback  read callback method - called with record data
     * @param binNames  optional bin to retrieve. All bins will be returned if not
     *                  specified. Aerospike 2 servers ignore this parameter.
     * @throws AerospikeException if scan fails
     */
    public void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback, String... binNames)
            throws AerospikeException {
        throw new UnsupportedOperationException("scanNode is not supported in MockAerospike");

    }

    /**
     * Read all records in specified namespace and set for one node only.
     * <p>
     * This call will block until the scan is complete - callbacks are made
     * within the scope of this call.
     *
     * @param policy    scan configuration parameters, pass in null for defaults
     * @param node      server node
     * @param namespace namespace - equivalent to database name
     * @param setName   optional set name - equivalent to database table
     * @param callback  read callback method - called with record data
     * @param binNames  optional bin to retrieve. All bins will be returned if not
     *                  specified. Aerospike 2 servers ignore this parameter.
     * @throws AerospikeException if transaction fails
     */
    public void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback, String... binNames)
            throws AerospikeException {
        throw new UnsupportedOperationException("scanNode is not supported in MockAerospike");

    }

    @Override
    public void scanPartitions(ScanPolicy scanPolicy, PartitionFilter partitionFilter, String s, String s1, ScanCallback scanCallback, String... strings) throws AerospikeException {

    }

    @Override
    public void scanPartitions(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, ScanPolicy scanPolicy, PartitionFilter partitionFilter, String s, String s1, String... strings) throws AerospikeException {

    }


    /**
     * Register package containing user defined functions with server. This
     * asynchronous server call will return before command is complete. The user
     * can optionally wait for command completion by using the returned
     * RegisterTask instance.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy     generic configuration parameters, pass in null for defaults
     * @param clientPath path of client file containing user defined functions,
     *                   relative to current directory
     * @param serverPath path to store user defined functions on the server, relative
     *                   to configured script directory.
     * @param language   language of user defined functions
     * @throws AerospikeException if register fails
     */
    public RegisterTask register(Policy policy, String clientPath, String serverPath, Language language) throws AerospikeException {
        throw new UnsupportedOperationException("register is not supported in MockAerospike");
    }

    /**
     * Execute user defined function on server and return results. The function
     * operates on a single record. The package name is used to locate the udf
     * file location:
     * <p>
     * udf file = {server udf dir}/${package name}.lua
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy       generic configuration parameters, pass in null for defaults
     * @param key          unique record identifier
     * @param packageName  server package name where user defined function resides
     * @param functionName user defined function
     * @param args         arguments passed in to user defined function
     * @return return value of user defined function
     * @throws AerospikeException if transaction fails
     * @deprecated Use
     * {@link #execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args)}
     * instead.
     */
    public Object execute(Policy policy, Key key, String packageName, String functionName, Value... args) throws AerospikeException {
        throw new UnsupportedOperationException("execute is not supported in MockAerospike");
    }

    /**
     * Execute user defined function on server and return results. The function
     * operates on a single record. The package name is used to locate the udf
     * file location:
     * <p>
     * udf file = ${server udf dir}/${package name}.lua
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy       write configuration parameters, pass in null for defaults
     * @param key          unique record identifier
     * @param packageName  server package name where user defined function resides
     * @param functionName user defined function
     * @param args         arguments passed in to user defined function
     * @return return value of user defined function
     * @throws AerospikeException if transaction fails
     */
    public Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args) throws AerospikeException {
        throw new UnsupportedOperationException("execute is not supported in MockAerospike");
    }

    @Override
    public void execute(EventLoop eventLoop, ExecuteListener executeListener, WritePolicy writePolicy, Key key, String s, String s1, Value... values) throws AerospikeException {

    }

    /**
     * Apply user defined function on records that match the statement filter.
     * Records are not returned to the client. This asynchronous server call
     * will return before command is complete. The user can optionally wait for
     * command completion by using the returned ExecuteTask instance.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy       scan configuration parameters, pass in null for defaults
     * @param statement    record filter
     * @param packageName  server package where user defined function resides
     * @param functionName function name
     * @param functionArgs to pass to function name, if any
     * @throws AerospikeException if command fails
     * @deprecated Use
     * {@link #execute(WritePolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs)}
     * instead.
     */
    public ExecuteTask execute(Policy policy, Statement statement, String packageName, String functionName, Value... functionArgs)
            throws AerospikeException {
        throw new UnsupportedOperationException("execute is not supported in MockAerospike");
    }

    /**
     * Apply user defined function on records that match the statement filter.
     * Records are not returned to the client. This asynchronous server call
     * will return before command is complete. The user can optionally wait for
     * command completion by using the returned ExecuteTask instance.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy       write configuration parameters, pass in null for defaults
     * @param statement    record filter
     * @param packageName  server package where user defined function resides
     * @param functionName function name
     * @param functionArgs to pass to function name, if any
     * @throws AerospikeException if command fails
     */
    public ExecuteTask execute(WritePolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs)
            throws AerospikeException {
        throw new UnsupportedOperationException("execute is not supported in MockAerospike");
    }

    @Override
    public ExecuteTask execute(WritePolicy writePolicy, Statement statement, Operation... operations) throws AerospikeException {
        return null;
    }

    /**
     * Execute query on all server nodes and return record iterator. The query
     * executor puts records on a queue in separate threads. The calling thread
     * concurrently pops records off the queue through the record iterator.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy    generic configuration parameters, pass in null for defaults
     * @param statement database query command
     * @return record iterator
     * @throws AerospikeException if query fails
     */
    public RecordSet query(QueryPolicy policy, Statement statement) throws AerospikeException {
        throw new UnsupportedOperationException("query is not supported in MockAerospike");
    }

    @Override
    public void query(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, QueryPolicy queryPolicy, Statement statement) throws AerospikeException {

    }

    /**
     * Execute query on a single server node and return record iterator. The
     * query executor puts records on a queue in a separate thread. The calling
     * thread concurrently pops records off the queue through the record
     * iterator.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy    generic configuration parameters, pass in null for defaults
     * @param statement database query command
     * @return record iterator
     * @throws AerospikeException if query fails
     */
    public RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
        throw new UnsupportedOperationException("queryNode is not supported in MockAerospike");
    }

    @Override
    public RecordSet queryPartitions(QueryPolicy queryPolicy, Statement statement, PartitionFilter partitionFilter) throws AerospikeException {
        return null;
    }

    @Override
    public void queryPartitions(EventLoop eventLoop, RecordSequenceListener recordSequenceListener, QueryPolicy queryPolicy, Statement statement, PartitionFilter partitionFilter) throws AerospikeException {

    }

    /**
     * Execute query, apply statement's aggregation function, and return result
     * iterator. The query executor puts results on a queue in separate threads.
     * The calling thread concurrently pops results off the queue through the
     * result iterator.
     * <p>
     * The aggregation function is called on both server and client (final
     * reduce). Therefore, the Lua script files must also reside on both server
     * and client. The package name is used to locate the udf file location:
     * </p>
     * udf file = ${udf dir}/${package name}.lua
     * <p>
     * This method is only supported by Aerospike 3 servers.
     * </p>
     *
     * @param policy       generic configuration parameters, pass in null for defaults
     * @param statement    database query command
     * @param packageName  server package where user defined function resides
     * @param functionName aggregation function name
     * @param functionArgs arguments to pass to function name, if any
     * @return result iterator
     * @throws AerospikeException if query fails
     */
    public ResultSet queryAggregate(QueryPolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs)
            throws AerospikeException {
        throw new UnsupportedOperationException("queryAggregate is not supported in MockAerospike");
    }

    /**
     * Create scalar secondary index. This asynchronous server call will return
     * before command is complete. The user can optionally wait for command
     * completion by using the returned IndexTask instance.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy    generic configuration parameters, pass in null for defaults
     * @param namespace namespace - equivalent to database name
     * @param setName   optional set name - equivalent to database table
     * @param indexName name of secondary index
     * @param binName   bin name that data is indexed on
     * @param indexType underlying data type of secondary index
     * @throws AerospikeException if index create fails
     */
    public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType)
            throws AerospikeException {
        throw new UnsupportedOperationException("createIndex is not supported in MockAerospike");
    }

    /**
     * Create complex secondary index to be used on bins containing collections.
     * This asynchronous server call will return before command is complete. The
     * user can optionally wait for command completion by using the returned
     * IndexTask instance.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy              generic configuration parameters, pass in null for defaults
     * @param namespace           namespace - equivalent to database name
     * @param setName             optional set name - equivalent to database table
     * @param indexName           name of secondary index
     * @param binName             bin name that data is indexed on
     * @param indexType           underlying data type of secondary index
     * @param indexCollectionType index collection type
     * @throws AerospikeException if index create fails
     */
    public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType,
                                 IndexCollectionType indexCollectionType) throws AerospikeException {
        throw new UnsupportedOperationException("createIndex is not supported in MockAerospike");
    }

    @Override
    public void createIndex(EventLoop eventLoop, IndexListener indexListener, Policy policy, String s, String s1, String s2, String s3, IndexType indexType, IndexCollectionType indexCollectionType) throws AerospikeException {

    }

    /**
     * Delete secondary index. This method is only supported by Aerospike 3
     * servers.
     *
     * @param policy    generic configuration parameters, pass in null for defaults
     * @param namespace namespace - equivalent to database name
     * @param setName   optional set name - equivalent to database table
     * @param indexName name of secondary index
     * @return
     * @throws AerospikeException if index create fails
     */
    public IndexTask dropIndex(Policy policy, String namespace, String setName, String indexName) throws AerospikeException {
        throw new UnsupportedOperationException("dropIndex is not supported in MockAerospike");
    }

    @Override
    public void dropIndex(EventLoop eventLoop, IndexListener indexListener, Policy policy, String s, String s1, String s2) throws AerospikeException {

    }

    @Override
    public void info(EventLoop eventLoop, InfoListener infoListener, InfoPolicy infoPolicy, Node node, String... strings) throws AerospikeException {

    }

    /**
     * Create user with password and roles. Clear-text password will be hashed
     * using bcrypt before sending to server.
     *
     * @param policy   admin configuration parameters, pass in null for defaults
     * @param user     user name
     * @param password user password in clear-text format
     * @param roles    variable arguments array of role names. Valid roles are listed
     *                 in Role.cs
     * @throws AerospikeException if command fails
     */
    public void createUser(AdminPolicy policy, String user, String password, List<String> roles) throws AerospikeException {
        throw new UnsupportedOperationException("createUser is not supported in MockAerospike");

    }

    /**
     * Remove user from cluster.
     *
     * @param policy admin configuration parameters, pass in null for defaults
     * @param user   user name
     * @throws AerospikeException if command fails
     */
    public void dropUser(AdminPolicy policy, String user) throws AerospikeException {
        throw new UnsupportedOperationException("dropUser is not supported in MockAerospike");

    }

    /**
     * Change user's password. Clear-text password will be hashed using bcrypt
     * before sending to server.
     *
     * @param policy   admin configuration parameters, pass in null for defaults
     * @param user     user name
     * @param password user password in clear-text format
     * @throws AerospikeException if command fails
     */
    public void changePassword(AdminPolicy policy, String user, String password) throws AerospikeException {
        throw new UnsupportedOperationException("changePassword is not supported in MockAerospike");

    }

    /**
     * Add roles to user's list of roles.
     *
     * @param policy admin configuration parameters, pass in null for defaults
     * @param user   user name
     * @param roles  role names. Valid roles are listed in Role.cs
     * @throws AerospikeException if command fails
     */
    public void grantRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
        throw new UnsupportedOperationException("grantRoles is not supported in MockAerospike");

    }

    /**
     * Remove roles from user's list of roles.
     *
     * @param policy admin configuration parameters, pass in null for defaults
     * @param user   user name
     * @param roles  role names. Valid roles are listed in Role.cs
     * @throws AerospikeException if command fails
     */
    public void revokeRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
        throw new UnsupportedOperationException("revokeRoles is not supported in MockAerospike");

    }

    /**
     * Create user defined role.
     *
     * @param policy     admin configuration parameters, pass in null for defaults
     * @param roleName   role name
     * @param privileges privileges assigned to the role.
     * @throws AerospikeException if command fails
     */
    public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        throw new UnsupportedOperationException("createRole is not supported in MockAerospike");

    }

    /**
     * Drop user defined role.
     *
     * @param policy   admin configuration parameters, pass in null for defaults
     * @param roleName role name
     * @throws AerospikeException if command fails
     */
    public void dropRole(AdminPolicy policy, String roleName) throws AerospikeException {
        throw new UnsupportedOperationException("dropRole is not supported in MockAerospike");

    }

    /**
     * Grant privileges to an user defined role.
     *
     * @param policy     admin configuration parameters, pass in null for defaults
     * @param roleName   role name
     * @param privileges privileges assigned to the role.
     * @throws AerospikeException if command fails
     */
    public void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        throw new UnsupportedOperationException("grantPrivileges is not supported in MockAerospike");

    }

    /**
     * Revoke privileges from an user defined role.
     *
     * @param policy     admin configuration parameters, pass in null for defaults
     * @param roleName   role name
     * @param privileges privileges assigned to the role.
     * @throws AerospikeException if command fails
     */
    public void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        throw new UnsupportedOperationException("revokePrivileges is not supported in MockAerospike");

    }

    /**
     * Retrieve roles for a given user.
     *
     * @param policy admin configuration parameters, pass in null for defaults
     * @param user   user name filter
     * @throws AerospikeException if command fails
     */
    public User queryUser(AdminPolicy policy, String user) throws AerospikeException {
        throw new UnsupportedOperationException("queryUser is not supported in MockAerospike");
    }

    /**
     * Retrieve all users and their roles.
     *
     * @param policy admin configuration parameters, pass in null for defaults
     * @throws AerospikeException if command fails
     */
    public List<User> queryUsers(AdminPolicy policy) throws AerospikeException {
        throw new UnsupportedOperationException("queryUsers is not supported in MockAerospike");
    }

    /**
     * Retrieve role definition.
     *
     * @param policy   admin configuration parameters, pass in null for defaults
     * @param roleName role name filter
     * @throws AerospikeException if command fails
     */
    public Role queryRole(AdminPolicy policy, String roleName) throws AerospikeException {
        throw new UnsupportedOperationException("queryRole is not supported in MockAerospike");
    }

    /**
     * Retrieve all roles.
     *
     * @param policy admin configuration parameters, pass in null for defaults
     * @throws AerospikeException if command fails
     */
    public List<Role> queryRoles(AdminPolicy policy) throws AerospikeException {
        throw new UnsupportedOperationException("queryRoles is not supported in MockAerospike");
    }

    /**
     * @param bins
     * @return
     */
    private Map<String, Object> convertToMap(Bin[] bins) {
        Map<String, Object> binMap = new HashMap<>(bins.length);
        for (Bin bin : bins) {
            if (bin.value instanceof BooleanValue) {
                binMap.put(bin.name, bin.value.toLong());
            } else {
                binMap.put(bin.name, bin.value.getObject());
            }
        }
        return binMap;
    }

    @Override
    public Policy getReadPolicyDefault() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public WritePolicy getWritePolicyDefault() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ScanPolicy getScanPolicyDefault() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryPolicy getQueryPolicyDefault() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BatchPolicy getBatchPolicyDefault() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InfoPolicy getInfoPolicyDefault() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void get(BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
        // TODO Auto-generated method stub

    }

    @Override
    public void get(EventLoop eventLoop, BatchListListener batchListListener, BatchPolicy batchPolicy, List<BatchRead> list) throws AerospikeException {

    }

    @Override
    public void get(EventLoop eventLoop, BatchSequenceListener batchSequenceListener, BatchPolicy batchPolicy, List<BatchRead> list) throws AerospikeException {

    }


    @Override
    public RegisterTask register(Policy policy, ClassLoader resourceLoader, String resourcePath, String serverPath, Language language)
            throws AerospikeException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RegisterTask registerUdfString(Policy policy, String s, String s1, Language language) throws AerospikeException {
        return null;
    }

    @Override
    public void removeUdf(InfoPolicy policy, String serverPath) throws AerospikeException {
        // TODO Auto-generated method stub
    }

    @Override
    public ResultSet queryAggregate(QueryPolicy policy, Statement statement) throws AerospikeException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultSet queryAggregateNode(QueryPolicy queryPolicy, Statement statement, Node node) throws AerospikeException {
        return null;
    }
}
