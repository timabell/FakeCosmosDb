using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests.FakeContainerTests;

public class ContainerRoundTripTests
{
	private readonly ITestOutputHelper _output;
	private readonly string _databaseName = "testdb";
	private readonly string _containerName = "testcontainer";

	public ContainerRoundTripTests(ITestOutputHelper output)
	{
		_output = output;
	}

	[Fact]
	public async Task GetContainer_ShouldReturnSameContainerInstance()
	{
		// Arrange
		var logger = new TestLogger(_output);
		var cosmosDb = new FakeCosmosDb(logger);

		// Create the database
		await cosmosDb.CreateDatabaseIfNotExistsAsync(_databaseName);

		// Get the container via the CosmosClient interface
		var container = cosmosDb.GetContainer(_databaseName, _containerName);

		// Add an item via the container interface
		var testItem = new { id = "test1", name = "Test Item 1" };
		await container.CreateItemAsync(testItem);

		// Act - Query the container to see if the item is there
		var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE c.id = 'test1'");
		var iterator = container.GetItemQueryIterator<JObject>(queryDefinition);
		var response = await iterator.ReadNextAsync();

		// Assert
		Assert.Single(response);
		Assert.Equal("Test Item 1", response.First()["name"].ToString());
	}

	[Fact]
	public async Task GetContainer_ShouldMaintainDataBetweenCalls()
	{
		// Arrange
		var logger = new TestLogger(_output);
		var cosmosDb = new FakeCosmosDb(logger);

		// Create the database
		await cosmosDb.CreateDatabaseIfNotExistsAsync(_databaseName);

		// Get container and add an item via the CosmosClient interface
		var container1 = cosmosDb.GetContainer(_databaseName, _containerName);
		await container1.UpsertItemAsync(new { id = "test2", name = "Test Item 2" });

		// Act - Get the container again
		var container2 = cosmosDb.GetContainer(_databaseName, _containerName);

		// Query to see if the item is still there
		var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE c.id = 'test2'");
		var iterator = container2.GetItemQueryIterator<JObject>(queryDefinition);
		var response = await iterator.ReadNextAsync();

		// Assert
		Assert.Single(response);
		Assert.Equal("Test Item 2", response.First()["name"].ToString());
	}

	[Fact]
	public async Task GetContainer_DifferentDatabases_ShouldNotShareData()
	{
		// Arrange
		var logger = new TestLogger(_output);
		var cosmosDb = new FakeCosmosDb(logger);

		// Create the databases
		await cosmosDb.CreateDatabaseIfNotExistsAsync("database1");
		await cosmosDb.CreateDatabaseIfNotExistsAsync("database2");

		// Get container from database1 and add an item
		var container1 = cosmosDb.GetContainer("database1", _containerName);
		await container1.UpsertItemAsync(new { id = "test3", name = "Test Item 3" });

		// Get container with same name but from database2
		var container2 = cosmosDb.GetContainer("database2", _containerName);

		// Act - Query for the item in database2's container
		var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE c.id = 'test3'");
		var iterator = container2.GetItemQueryIterator<JObject>(queryDefinition);
		var response = await iterator.ReadNextAsync();

		// Assert - Item should not be found in database2
		Assert.Empty(response);
	}
}
