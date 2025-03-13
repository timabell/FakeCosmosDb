#!/bin/sh -v
docker pull mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator
docker run -p 8081:8081 -d mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator
curl https://localhost:8081/_explorer/emulator.pem --insecure
