#!/bin/sh
#set -x

{
sleep 5
echo "flush_all"

sleep 5
echo exit
} | telnet 127.0.0.1 11212
