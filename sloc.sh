#!/bin/bash
CLIENT=(include/hepnos.hpp include/hepnos/* src/*.cpp src/private/)
SERVER=(include/hepnos-service.h src/service/)
echo "************** CLIENT ***************"
sloccount "${CLIENT[@]}"

echo "************** SERVER ***************"
sloccount "${SERVER[@]}"
