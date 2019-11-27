#!/bin/sh
set -e;

mongo <<EOF
use vid-iot

db.createCollection("originTable")
db.originTable.createIndex({"origin_ip": 1}, {unique:true})
db.originTable.createIndex({"origin_id": 1}, {unique:true})
db.originTable.createIndex("origin_id")

db.createCollection("ffmpegProcsTable")

db.createCollection("streamsTable")
db.distTable.createIndex({"stream_id": 1}, {unique:true})

db.createCollection("distTable")
db.distTable.createIndex({"dist_ip": 1, "dist_id": 1}, {unique:true})
db.originTable.createIndex("origin_id")

db.createCollection("archivesTable")

db.createCollection("usersTable")
db.usersTable.createIndex({"username": 1}, {unique:true})

quit()
EOF
