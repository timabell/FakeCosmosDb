using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests.Utilities;

public class CosmosDbTestFixture : IDisposable
{
	public ICosmosDb Db { get; }
	public string ContainerName = "TestContainer";
	private readonly ILogger _logger;
	private readonly string TestDatabaseName = "TestDatabase";
	private readonly string TestContainerName = "TestContainer";
	private Container _container;
	private FakeCosmosDb _cosmosDb;

	public CosmosDbTestFixture(bool useRealCosmos, ITestOutputHelper output = null)
	{
		_logger = output != null ? new TestLogger(output) : null;

		if (useRealCosmos)
		{
			// Use CosmosDB Emulator
			var clientOptions = new CosmosClientOptions();
			Db = new CosmosDbAdapter("AccountEndpoint=https://localhost:8081;AccountKey=your-key;", clientOptions, _logger);
		}
		else
		{
			// Use In-Memory Mock
			Db = new FakeCosmosDb(_logger);
		}
	}

	public async Task InitializeAsync()
	{
		_cosmosDb = new FakeCosmosDb();
		var database = await _cosmosDb.CreateDatabaseIfNotExistsAsync(TestDatabaseName);
		_container = _cosmosDb.GetContainer(TestDatabaseName, TestContainerName);
	}

	public void Dispose() { /* Cleanup if needed */ }
}
