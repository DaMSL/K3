#!/bin/bash

if [ "$#" -ne 1 ]; then
  echo "$0 COMMAND"
fi

git -C /k3/core pull
git -C /k3/driver pull
$1

