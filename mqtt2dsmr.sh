#!/bin/bash
while true; do
  mosquitto_sub -h atom-ap.hom -t 'smartmeter/E0067005564809720/telegram' | stdbuf -o0 -i0 jq -r '.[]' | stdbuf -o0 sed $'s/$/\r/' | nc -n -l -k -p 5555 > /dev/null
done
