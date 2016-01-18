# mock-aersopike
Just use `MockAerospikeClient` implementation for the interface `IAerospikeClient` in your unit tests.

## Goals
- Lightweight & Embedded
- Easy to use (implements the same interface)
- Reduce effort in unit testing
- Allow extensibility

## Issues with IAerospikeClient
Currently, Aerospike provides for `IAerospikeClient` (in Java) with sole intention of making AerospikeClient testable.
However, there are few issues pointed out - https://github.com/aerospike/aerospike-client-java/issues/34
- Don't mock what you don't own (Mocking `IAerospikeClient`, then mocking `Record`).

Better approach to testing is to create `MockAerospikeClient` which implements `IAerospikeClient`, thus avoiding mocking `RecordSet` and `Record` itself.

## Implementation
Currently, `MockAerospikeClient` supports the following methods:
- put
- get
- delete
- exists
- getHeader

`MockAerospikeClient` internally uses a `HashMap` to store `Record` corresponding to a `Key`

## Setup
- Maven
- Java 1.8 or greater

## Usage
- pom.xml
Added the following dependency to your pom.xml
```
<dependency>
  <groupId>com.github.srini156</groupId>
  <artifactId>mock-aerospike-java</artifactId>
  <version>0.0.2</version>
</dependency>
```

- Code
```
  MockAerospikeClient client = new MockAerospikeClient();
  //Put entry into Aerospike
  client.put(null, new Key("namespace", "set", "key"), new Bin[] { new Bin("bin1", "value1") });
  //Fetch entry from Aerospike
  client.get(null, new Key("namespace","set","key"));
```



## Contributions
It is currently alpha and WIP. Contributions are welcome, please raise a pull request.

