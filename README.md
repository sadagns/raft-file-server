# raft-file-server

A distributed implementation of a simple versioned-file server backed by RAFT consensus algorithm. Written in golang 1.6.

### Synopsis

This is a simple distributed system that provides fault tolerance if atleast a majority of nodes in a cluster are up and running properly. It provides a simple file interface that supports four file operations - read, write, compare-and-swap, delete. In addition, each file is tagged with a version number and an optional expiry time. All the files are kept in memory and only the latest versions of files are available at any time.

### RAFT implementation

Once a cluster of servers is started, the underlying RAFT node in each server communicates with each other and establish consensus. A single leader is chosen among these servers and is assigned the responsibility of replicating all incoming **write** client requests in other servers' log. 

Once a write request is replicated in atleast a majority of servers, the leader proceeds to send a response to the client. This favours in situations where a minority of servers are down or partitioned from the rest. The correctness and safety properties of this process is guaranteed by the RAFT consensus algorithm used underneath. 

This system does not provide replication of **read** requests. In essence, it provides **eventual consistency**. Hence, any server that might be lagging behind the leader would be able to respond stale data to client's read requests. This is acceptable in situations where strict consistency is not a necessity.

### Installation

###### Note: You need to have golang installed in your system.
```
go get github.com/sadagopanns/cs733/assignment4
go install github.com/sadagopanns/cs733/assignment4/server.go
server <server-id>
```

### Starting Cluster

A cluster can be brought up by starting each server using the command. 
```
server <server-id>
```
Once a server is started, it opens config_\<server_id\> .json file to read configuration info for RAFT nodes communication. It is assumed that there are five servers and all of them are listening on the same local host at the port 5000 + \<server-id\>. 

###  Communication with file server

A client can contact any of the servers in the cluster for a **read** file operation, but it needs to contact the leader in case of any other file operation. It can, however, contact a server that is not a leader and can get redirected to the right leader once a leader is elected. Please see the **Errors** section for further details.

### Commands
##### 1. read
Read the latest version of the file from the server.

Syntax:
```
	read <file_name>\r\n
```

**Response:**

Success : 

``` CONTENTS <version> <num_bytes> <exptime>\r\n ```<br />
``` <content_bytes>\r\n ```

Failures :

```ERR_FILE_NOT_FOUND``` : File to be read is not found

##### 2. write

Write a file. If the file is already present, it will be overwritten and a new version will be given.

Syntax:
```
	write <file_name> <num_bytes> [<exp_time>]\r\n
	<content bytes>\r\n
```

**Response:**

Success : 

``` OK <version>\r\n ```

Failures :

None

##### 3. compare-and-swap (cas)

Replace the old file contents with the new contents provided. This will be successful only if the version provided is the latest.

Syntax:
```
	cas <file_name> <num_bytes> [<exp_time>]\r\n
	<content bytes>\r\n
```

**Response:**

Success : 

``` OK <version>\r\n ```

Failures :

```ERR_VERSION <latest_version>``` : Error in version provided. Use the latest version in the error message.

##### 4. delete

Delete a file.

Syntax:
```
	delete <file_name>\r\n
```

**Response:**

Success : 

``` OK\r\n ```

Failures :

```ERR_FILE_NOT_FOUND``` : File to be deleted in not found. Either file is expired or not created at all.

###### NOTE

Errors other than the ones given under failure responses might occur due to the following reasons: internal server problem, incorrect client command/arguments, RAFT consensus leader election. Please read the **Errors** section to understand them.

### File expiry handling

Any file that has a file expiry time will be removed from the server once its time is expired. This facility, along with the compare-and-swap command can be used a coordination service, much like ZooKeeper.


### Errors

``` ERR_FILE_NOT_FOUND ```:                   **File requested is not found** 

``` ERR_VERSION <latest-version> ```:         **Use the latest version**

``` ERR_CMD_ERR ```:                          **Incorrect client command/arguments**

``` ERR_INTERNAL ```:                         **Internal server error**

``` ERR_REDIRECT <leader-ip:leader-port> ```: **Redirect the request to the leader node**

``` ERR_TRY_LATER ```:                        **Leader not found. Try later.**

### Testing raft-file-server

A bunch of test cases has been written in the test file. These include testing sequential file operations, binary data storage, sending a chunk of a client command, sending a batch of commands, timer operation, concurrent writes, concurrent cas, single node failure and leader failure. For the concurrent test cases, the leader was fixed to be node-1 to enable proper working irrespective of irregular timeouts due to huge time incurred during log/state storage in disk.

### Limits and Limitations

- If the command or contents line is in error such that the server cannot reliably figure out the end of the command, the connection is shut down. Examples of such errors are incorrect command name, numbytes is not numeric, the contents line doesn't end with a '\r\n'.

- The first line of the command is constrained to be less than 500 characters long. If not, an error is returned and the connection is shut down.

- There might be irregular timeouts(which are unexpected) because of the huge time incurred during log/state storage in disk.

### Contact

Sadagopan N S <br />
Email: sadagns@gmail.com
