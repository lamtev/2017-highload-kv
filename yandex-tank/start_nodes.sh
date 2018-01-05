#!/usr/bin/env bash

chmod +x node1.start
chmod +x node2.start
chmod +x node3.start

./node1.start &
./node2.start &
./node3.start &
wait