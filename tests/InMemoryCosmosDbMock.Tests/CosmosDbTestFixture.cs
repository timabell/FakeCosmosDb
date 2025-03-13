using System;

namespace TimAbell.MockableCosmos.Tests;

public class CosmosDbTestFixture : IDisposable
{
    public ICosmosDb Db { get; }
    public string ContainerName = "TestContainer";

    public CosmosDbTestFixture(bool useRealCosmos)
    {
        if (useRealCosmos)
        {
            // Use CosmosDB Emulator
            Db = new CosmosDbAdapter("AccountEndpoint=https://localhost:8081;AccountKey=your-key;");
        }
        else
        {
            // Use In-Memory Mock
            Db = new CosmosInMemoryCosmosDb();
        }

        Db.AddContainerAsync(ContainerName).Wait();
    }

    public void Dispose() { /* Cleanup if needed */ }
}