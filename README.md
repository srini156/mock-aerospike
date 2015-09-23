# mock-aersopike
mock-aerospike implements `MockAerospikeClient` which helps in unit testing.
Just use `MockAerospikeClient` implementation for the interface `IAerospikeClient` in your unit tests.

## Issues with IAerospikeClient
Currently, Aerospike provides for `IAerospikeClient` (in Java) with sole intention of making AerospikeClient testable.
However, there are few issues pointed out - https://github.com/aerospike/aerospike-client-java/issues/34

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
- Java 1.7 or greater

## Contributions
It is currently alpha and WIP. Contributions are welcome, please raise a pull request.
