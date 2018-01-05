#!/usr/bin/env bash

run()
{

  if [ "$#" != "1" ]; then
    echo "Invalid number of arguments!"
    exit 1
  fi
  
  AMMO = "$1"
  
  if [ ! -f $AMMO ]; then
    echo "${AMMO} not found!"
  fi
  
  sudo yandex-tank -c load.yaml $AMMO

}

#. ./loadtest.sh && run $AMMO
