public class CosmosDbTestFixture : IDisposable
{
    public ICosmosDbMock Db { get; }
    public string ContainerName = "TestContainer";

    public CosmosDbTestFixture(bool useRealCosmos)
    {
        if (useRealCosmos)
        {
            // Use CosmosDB Emulator
            Db = new CosmosDbMockAdapter("AccountEndpoint=https://localhost:8081;AccountKey=your-key;");
        }
        else
        {
            // Use In-Memory Mock
            Db = new InMemoryCosmosDbMock();
        }

        Db.AddContainerAsync(ContainerName).Wait();
    }

    public void Dispose() { /* Cleanup if needed */ }
}
