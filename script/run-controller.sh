#!/bin/bash

backupd controller  \
    -addr :80 \
    -lainlet lainlet.lain:9001 \
    -advertise http://backupctl.lain.local \
    -dport 9002 \
    -data /var/backupctl

