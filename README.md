# InMemoryCosmosDbMock

This library provides a high-fidelity **in-memory CosmosDB mock** for unit testing, with optional support for running tests against the **real CosmosDB Emulator**.

## Features
✅ Supports SQL-like queries (`SELECT * FROM c WHERE c.Name = 'Alice'`)  
✅ Fully **in-memory**, no dependencies required  
✅ **Multi-container** support  
✅ Can switch between **mock mode and real CosmosDB mode**  
✅ **GitHub Actions CI/CD ready**  

## Installation
To use the in-memory CosmosDB mock, install via NuGet:

```sh
dotnet add package InMemoryCosmosDbMock
```

## Running Tests with the Real CosmosDB Emulator
To run tests against a **real CosmosDB Emulator**, start the emulator in Docker:

```sh
docker run -p 8081:8081 -d mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator
```

Then, run the tests:

```sh
dotnet test
```
