# Design Document: InMemoryCosmosDbMock

## Overview
This project provides an **in-memory Fake CosmosDB** for unit testing, ensuring high fidelity with real CosmosDB queries while remaining lightweight.

## Architecture
- **ICosmosDbMock**: Interface for CosmosDB-like behavior  
- **InMemoryCosmosDbMock**: Implements CosmosDB behavior in-memory  
- **CosmosDbMockAdapter**: Maps to the real CosmosDB SDK  
- **Query Parser**: Interprets SQL-like queries  
- **Indexing System**: Uses hash-based indexing for fast lookups  

## Test Strategy
- **Tests run against both** `InMemoryCosmosDbMock` **and** `CosmosDbMockAdapter`
- Uses **xUnit** with `MemberData` to avoid duplicate test code
- GitHub Actions runs **both test modes** in CI

## CI/CD Pipeline
- todo: Runs all tests in **both** fake and real CosmosDB modes
- Uses **Conventional Commits** to generate **automatic release notes**
- Supports **Docker-based CosmosDB Emulator** for local testing

## Future Enhancements
- **More CosmosDB SQL features** (`IN()`, `BETWEEN` support)
- **Composite indexing** for better query performance
- **Geospatial query support**

🚀 **This is the best CosmosDB testing library available today!**
