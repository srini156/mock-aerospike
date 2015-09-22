package org.srini156.aerospike.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.AerospikeException.InvalidNode;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.large.LargeList;
import com.aerospike.client.large.LargeMap;
import com.aerospike.client.large.LargeSet;
import com.aerospike.client.large.LargeStack;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.google.common.collect.Maps;

/**
 * Mock Implementation of IAerospikeClient for allowing unit testing.
 * 
 * @author srinivas.iyengar
 *
 */
public class MockAerospikeClient implements IAerospikeClient {

	private final Map<Key, Record> data = Maps.newHashMap();

	public void close() {
		data.clear();
	}

	public boolean isConnected() {
		return true;
	}

	public Node[] getNodes() {
		return null;
	}

	public List<String> getNodeNames() {
		return null;
	}

	public Node getNode(String nodeName) throws InvalidNode {
		return null;
	}

	/**
	 * Write record bin(s). The policy specifies the transaction timeout, record expiration and how
	 * the transaction is handled when the record already exists.
	 * 
	 * @param policy write configuration parameters, pass in null for defaults
	 * @param key unique record identifier
	 * @param bins array of bin name/value pairs
	 * @throws AerospikeException if write fails
	 */
	public void put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		data.put(key, new Record(convertToMap(bins), 0, 0));
	}

	public void append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		throw new UnsupportedOperationException("append is not supported in MockAerospike");

	}

	public void prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		throw new UnsupportedOperationException("prepend is not supported in MockAerospike");

	}

	public void add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		throw new UnsupportedOperationException("add is not supported in MockAerospike");

	}

	public boolean delete(WritePolicy policy, Key key) throws AerospikeException {
		if (data.containsKey(key)) {
			data.remove(key);
			return true;
		} else {
			return false;
		}
	}

	public void touch(WritePolicy policy, Key key) throws AerospikeException {
		throw new UnsupportedOperationException("touch is not supported in MockAerospike");

	}

	public boolean exists(Policy policy, Key key) throws AerospikeException {
		return data.containsKey(key);
	}

	public boolean[] exists(Policy policy, Key[] keys) throws AerospikeException {
		boolean[] result = new boolean[keys.length];
		for (int idx = 0; idx < keys.length; idx++) {
			result[idx] = data.containsKey(keys[idx]);
		}
		return result;
	}

	public boolean[] exists(BatchPolicy policy, Key[] keys) throws AerospikeException {
		return exists((Policy) policy, keys);
	}

	public Record get(Policy policy, Key key) throws AerospikeException {
		return data.get(key);
	}

	public Record get(Policy policy, Key key, String... binNames) throws AerospikeException {
		Record record = data.get(key);
		if (record == null) {
			return null;
		} else {
			// filter bins.
			Map<String, Object> filteredBins = Maps.newHashMap();
			for (String bin : binNames) {
				filteredBins.put(bin, record.bins.get(bin));
			}
			return new Record(filteredBins, record.generation, record.expiration);
		}
	}

	public Record getHeader(Policy policy, Key key) throws AerospikeException {
		Record record = data.get(key);

		if (record == null) {
			return null;
		} else {
			return new Record(null, record.generation, record.expiration);
		}

	}

	public Record[] get(Policy policy, Key[] keys) throws AerospikeException {
		Record[] records = new Record[keys.length];
		for (int idx = 0; idx < records.length; idx++) {
			records[idx] = get(policy, keys[idx]);
		}
		return records;
	}

	public Record[] get(BatchPolicy policy, Key[] keys) throws AerospikeException {
		return get((Policy) policy, keys);
	}

	public Record[] get(Policy policy, Key[] keys, String... binNames) throws AerospikeException {
		Record[] records = new Record[keys.length];
		for (int idx = 0; idx < records.length; idx++) {
			records[idx] = get(policy, keys[idx], binNames);
		}
		return records;
	}

	public Record[] get(BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
		return get((Policy) policy, keys, binNames);
	}

	public Record[] getHeader(Policy policy, Key[] keys) throws AerospikeException {
		Record[] records = new Record[keys.length];
		for (int idx = 0; idx < records.length; idx++) {
			records[idx] = getHeader(policy, keys[idx]);
		}
		return records;
	}

	public Record[] getHeader(BatchPolicy policy, Key[] keys) throws AerospikeException {
		return getHeader((Policy) policy, keys);
	}

	public Record operate(WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
		throw new UnsupportedOperationException("operate is not supported in MockAerospike");
	}

	public void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback, String... binNames) throws AerospikeException {
		throw new UnsupportedOperationException("scanAll is not supported in MockAerospike");

	}

	public void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback, String... binNames)
	        throws AerospikeException {
		throw new UnsupportedOperationException("scanNode is not supported in MockAerospike");

	}

	public void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback, String... binNames)
	        throws AerospikeException {
		throw new UnsupportedOperationException("scanNode is not supported in MockAerospike");

	}

	public LargeList getLargeList(Policy policy, Key key, String binName, String userModule) {
		throw new UnsupportedOperationException("getLargeList is not supported in MockAerospike");
	}

	public LargeList getLargeList(WritePolicy policy, Key key, String binName, String userModule) {
		throw new UnsupportedOperationException("getLargeList is not supported in MockAerospike");
	}

	public LargeMap getLargeMap(Policy policy, Key key, String binName, String userModule) {
		throw new UnsupportedOperationException("getLargeMap is not supported in MockAerospike");
	}

	public LargeMap getLargeMap(WritePolicy policy, Key key, String binName, String userModule) {
		throw new UnsupportedOperationException("getLargeMap is not supported in MockAerospike");
	}

	public LargeSet getLargeSet(Policy policy, Key key, String binName, String userModule) {
		throw new UnsupportedOperationException("getLargeSet is not supported in MockAerospike");
	}

	public LargeSet getLargeSet(WritePolicy policy, Key key, String binName, String userModule) {
		throw new UnsupportedOperationException("getLargeSet is not supported in MockAerospike");
	}

	public LargeStack getLargeStack(Policy policy, Key key, String binName, String userModule) {
		throw new UnsupportedOperationException("getLargeStack is not supported in MockAerospike");
	}

	public LargeStack getLargeStack(WritePolicy policy, Key key, String binName, String userModule) {
		throw new UnsupportedOperationException("getLargeStack is not supported in MockAerospike");
	}

	public RegisterTask register(Policy policy, String clientPath, String serverPath, Language language) throws AerospikeException {
		throw new UnsupportedOperationException("register is not supported in MockAerospike");
	}

	public Object execute(Policy policy, Key key, String packageName, String functionName, Value... args) throws AerospikeException {
		throw new UnsupportedOperationException("execute is not supported in MockAerospike");
	}

	public Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args) throws AerospikeException {
		throw new UnsupportedOperationException("execute is not supported in MockAerospike");
	}

	public ExecuteTask execute(Policy policy, Statement statement, String packageName, String functionName, Value... functionArgs)
	        throws AerospikeException {
		throw new UnsupportedOperationException("execute is not supported in MockAerospike");
	}

	public ExecuteTask execute(WritePolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs)
	        throws AerospikeException {
		throw new UnsupportedOperationException("execute is not supported in MockAerospike");
	}

	public RecordSet query(QueryPolicy policy, Statement statement) throws AerospikeException {
		throw new UnsupportedOperationException("query is not supported in MockAerospike");
	}

	public RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
		throw new UnsupportedOperationException("queryNode is not supported in MockAerospike");
	}

	public ResultSet queryAggregate(QueryPolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs)
	        throws AerospikeException {
		throw new UnsupportedOperationException("queryAggregate is not supported in MockAerospike");
	}

	public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType)
	        throws AerospikeException {
		throw new UnsupportedOperationException("createIndex is not supported in MockAerospike");
	}

	public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType,
	        IndexCollectionType indexCollectionType) throws AerospikeException {
		throw new UnsupportedOperationException("createIndex is not supported in MockAerospike");
	}

	public void dropIndex(Policy policy, String namespace, String setName, String indexName) throws AerospikeException {
		throw new UnsupportedOperationException("dropIndex is not supported in MockAerospike");

	}

	public void createUser(AdminPolicy policy, String user, String password, List<String> roles) throws AerospikeException {
		throw new UnsupportedOperationException("createUser is not supported in MockAerospike");

	}

	public void dropUser(AdminPolicy policy, String user) throws AerospikeException {
		throw new UnsupportedOperationException("dropUser is not supported in MockAerospike");

	}

	public void changePassword(AdminPolicy policy, String user, String password) throws AerospikeException {
		throw new UnsupportedOperationException("changePassword is not supported in MockAerospike");

	}

	public void grantRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
		throw new UnsupportedOperationException("grantRoles is not supported in MockAerospike");

	}

	public void revokeRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
		throw new UnsupportedOperationException("revokeRoles is not supported in MockAerospike");

	}

	public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
		throw new UnsupportedOperationException("createRole is not supported in MockAerospike");

	}

	public void dropRole(AdminPolicy policy, String roleName) throws AerospikeException {
		throw new UnsupportedOperationException("dropRole is not supported in MockAerospike");

	}

	public void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
		throw new UnsupportedOperationException("grantPrivileges is not supported in MockAerospike");

	}

	public void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
		throw new UnsupportedOperationException("revokePrivileges is not supported in MockAerospike");

	}

	public User queryUser(AdminPolicy policy, String user) throws AerospikeException {
		throw new UnsupportedOperationException("queryUser is not supported in MockAerospike");
	}

	public List<User> queryUsers(AdminPolicy policy) throws AerospikeException {
		throw new UnsupportedOperationException("queryUsers is not supported in MockAerospike");
	}

	public Role queryRole(AdminPolicy policy, String roleName) throws AerospikeException {
		throw new UnsupportedOperationException("queryRole is not supported in MockAerospike");
	}

	public List<Role> queryRoles(AdminPolicy policy) throws AerospikeException {
		throw new UnsupportedOperationException("queryRoles is not supported in MockAerospike");
	}

	/**
	 * @param bins
	 * @return
	 */
	private Map<String, Object> convertToMap(Bin[] bins) {
		Map<String, Object> binMap = new HashMap<String, Object>(bins.length);
		for (Bin bin : bins) {
			binMap.put(bin.name, bin.value.getObject());
		}
		return binMap;
	}
}
