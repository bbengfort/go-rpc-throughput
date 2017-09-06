#!/bin/bash

FIXTURES=fixtures/
RESULTS=$FIXTURES/results
CLIENTS=12

# Clean up any files that were there to start
fab cleanup

# Run rtreq with synchronous server
for i in {1..24}
do
    fab "bench:clients=$i,cmd=rtreq,sync=True,addr=172.31.39.118"
done

# Get the results and cleanup
fab "getmerge:path=$RESULTS,suffix=rep"
fab cleanup

# Run rtreq with asynchronous server
for i in {1..24}
do
    fab "bench:clients=$i,cmd=rtreq,sync=False,addr=172.31.39.118"
done

# Get the results and cleanup
fab "getmerge:path=$RESULTS,suffix=router"
fab cleanup

# Run gRPC
for i in {1..24}
do
    fab "bench:clients=$i,cmd=echgo,addr=172.31.39.118"
done

# Get the results and cleanup
fab "getmerge:path=$RESULTS,suffix=grpc"
fab cleanup
