#!/bin/bash

curl -XPOST localhost/api/v2/system/backup -d dir="/var/backupctl-backup"
