using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests
{
	public class LoggingTests
	{
		private readonly ITestOutputHelper _output;
		private FakeCosmosDb _cosmosDb;
		private Container _container;
		private readonly string TestDatabaseName = "TestDatabase";
		private readonly string TestContainerName = "TestContainer";
		private readonly TestLogger _logger;

		public LoggingTests(ITestOutputHelper output)
		{
			_output = output;
			_logger = new TestLogger(_output);
		}

		private async Task InitializeAsync()
		{
			_cosmosDb = new FakeCosmosDb(_logger);
			await _cosmosDb.CreateDatabaseIfNotExistsAsync(TestDatabaseName);
			_container = _cosmosDb.GetContainer(TestDatabaseName, TestContainerName);
		}

		private async Task AddTestItemAsync<T>(T item)
		{
			await _container.CreateItemAsync(item);
		}

		[Fact]
		public async Task Can_Log_SQL_Parsing_And_Execution()
		{
			// Arrange
			await InitializeAsync();

			// Create a container and add some test data
			// Container is already created by GetContainer, no need to call CreateContainerIfNotExistsAsync

			var alice = new JObject
			{
				["id"] = "1",
				["Name"] = "Alice",
				["Age"] = 30
			};

			var bob = new JObject
			{
				["id"] = "2",
				["Name"] = "Bob",
				["Age"] = 25
			};

			await AddTestItemAsync(alice);
			await AddTestItemAsync(bob);

			// Act - this will generate detailed logs about the parsing and execution
			var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE c.Name = 'Alice'");
			var iterator = _container.GetItemQueryIterator<JObject>(queryDefinition);
			var response = await iterator.ReadNextAsync();

			// Assert
			Assert.Equal(1, response.Count);
			Assert.Equal("Alice", response.First()["Name"].ToString());
			Assert.Equal(30, (int)response.First()["Age"]);

			// The test logger will have output all the debug information to the test console
		}
	}
}
