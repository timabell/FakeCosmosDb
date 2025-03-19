using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests.CrossAccessTests;

/// <summary>
/// Tests that prove that the data is shared across all the various ways you can read/write in the sprawling CosmosClient API
/// </summary>
public class DataRoundTripTests
{
	private readonly ITestOutputHelper _output;
	private readonly string _databaseName = "testdb";
	private readonly string _containerName = "testcontainer";

	public DataRoundTripTests(ITestOutputHelper output)
	{
		_output = output;
	}

	[Fact]
	public async Task WriteWithContainer_ReadWithICosmosDb_ShouldRoundTrip()
	{
		// Arrange
		var logger = new TestLogger(_output);
		var cosmosDb = new FakeCosmosDb(logger);

		// Add container via ICosmosDb interface
		await cosmosDb.AddContainerAsync(_containerName);

		// Write data using Container API
		var container = cosmosDb.GetContainer(_databaseName, _containerName);
		await container.UpsertItemAsync(new { id = "test3", name = "Test Item 3", value = 42 });

		// Act - Read data using ICosmosDb interface
		var results = await cosmosDb.QueryAsync(_containerName, "SELECT * FROM c WHERE c.id = 'test3'");

		// Assert
		Assert.Single(results);
		Assert.Equal("Test Item 3", results.First()["name"].ToString());
		Assert.Equal(42, (int)results.First()["value"]);
	}

	[Fact]
	public async Task WriteWithICosmosDb_ReadWithContainer_ShouldRoundTrip()
	{
		// Arrange
		var logger = new TestLogger(_output);
		var cosmosDb = new FakeCosmosDb(logger);

		// Add container and write data via ICosmosDb interface
		await cosmosDb.AddContainerAsync(_containerName);
		await cosmosDb.AddItemAsync(_containerName, new { id = "test4", name = "Test Item 4", value = 99 });

		// Act - Read data using Container API
		var container = cosmosDb.GetContainer(_databaseName, _containerName);
		var response = await container.ReadItemAsync<JObject>("test4", new PartitionKey("test4"));

		// Assert
		Assert.Equal("Test Item 4", response.Resource["name"].ToString());
		Assert.Equal(99, (int)response.Resource["value"]);
	}

	[Fact]
	public async Task WriteWithContainer_ReadWithSameNamedContainer_ShouldRoundTrip()
	{
		// Arrange
		var logger = new TestLogger(_output);
		var cosmosDb = new FakeCosmosDb(logger);

		// Write data using Container API with specific name
		var container1 = cosmosDb.GetContainer(_databaseName, _containerName);
		await container1.UpsertItemAsync(new { id = "test5", name = "Test Item 5" });

		// Act - Read data using a different Container instance but same database and container name
		var container2 = cosmosDb.GetContainer(_databaseName, _containerName);
		var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE c.id = 'test5'");
		var iterator = container2.GetItemQueryIterator<JObject>(queryDefinition);
		var response = await iterator.ReadNextAsync();

		// Assert
		Assert.Single(response);
		Assert.Equal("Test Item 5", response.First()["name"].ToString());
	}
}
