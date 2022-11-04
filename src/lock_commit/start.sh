#!/bin/bash
# Start a number of nodes running on different sockets. The amount of nodes that start should be passed to the script as its first argument, and the second should be the delta for the timer in miliseconds.
# Example call: ./start.sh 3 100

if [ $# -ne 1 ] && [ $# -ne 2 ]
then
  echo "Incorrect number of arguments. Usage: ./start.sh {number-of-nodes} {timer-delta-ms}" 
  exit 1
fi

i=1; j=0
LIST_PEERS="127.0.0.1:6100"

while [ $i -lt $1 ]; do
   PORT=$((6100+$i))
   LIST_PEERS="${LIST_PEERS} 127.0.0.1:${PORT}"
   i=$((i + 1))
done


while [ $j -lt $1 ]; do
   PORT=$((6100+$j))
   RUST_LOG=INFO ../../target/debug/node_lock_commit --port $PORT --peers "$LIST_PEERS" -v $2 &
   
   j=$((j + 1))
done

# RUST_LOG=DEBUG ../../target/debug/node_lock_commit --port 6100 --peers "127.0.0.1:6100 127.0.0.1:6101"