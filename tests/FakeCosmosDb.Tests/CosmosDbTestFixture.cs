using System;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests;

public class CosmosDbTestFixture : IDisposable
{
	public ICosmosDb Db { get; }
	public string ContainerName = "TestContainer";
	private readonly ILogger _logger;

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
			Db = new CosmosInMemoryCosmosDb(_logger);
		}

		Db.AddContainerAsync(ContainerName).Wait();
	}

	public void Dispose() { /* Cleanup if needed */ }
}
