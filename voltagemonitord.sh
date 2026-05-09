#!/bin/bash
set -e
#set -x

# Put configuration here
conf="/etc/voltagemonitord/voltagemonitord.conf"

thresholdlow=207
thresholdhigh=253
meterid="E12345678"
mosquittohost="localhost"
mosquittotopic="smartmeter/${meterid}"
pushovertoken="supersecret"
pushoveruser="supersecret"

[ -r "${conf}" ] && . "${conf}"

lastalert=0
alertmessage=""

mosquitto_sub -h "${mosquittohost}" -v -t "${mosquittotopic}/vl1" -t "${mosquittotopic}/vl2" -t "${mosquittotopic}/vl3" | (while read -r message
do
  ts="$(date '+%s')"

  IFS=' ' read -r -a pmessage <<< "${message}"
  IFS='/' read -r -a mqttchannel <<< "${pmessage[0]}"

  if [ "$(tr -d '.' <<<${pmessage[1]})" -gt "${thresholdhigh}0" ]; then
    alertmessage="HOGE NETSPANNING: ${pmessage[1]}V op $(tr '[:lower:]' '[:upper:]' <<<${mqttchannel[2]:1:2})"
  elif [ "$(tr -d '.' <<<${pmessage[1]})" -lt "${thresholdlow}0" ]; then
    alertmessage="LAGE NETSPANNING: ${pmessage[1]}V op $(tr '[:lower:]' '[:upper:]' <<<${mqttchannel[2]:1:2})"
  else
    continue
  fi

  if [ $((ts - lastalert)) -gt 60 ]; then
    curl -s \
      --form-string "token=${pushovertoken}" \
      --form-string "user=${pushoveruser}" \
      --form-string "priority=1" \
      --form-string "message=${alertmessage}" \
      https://api.pushover.net/1/messages.json &
  fi
  lastalert=$ts
done)
