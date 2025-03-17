using System.Linq;
using System.Threading.Tasks;
using InMemoryCosmosDbMock.Tests.Utilities;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.MockableCosmos.Tests
{
	public class FakeContainerTests
	{
		private readonly ITestOutputHelper _output;
		private readonly TestLogger _logger;

		public FakeContainerTests(ITestOutputHelper output)
		{
			_output = output;
			_logger = new TestLogger(output);
		}

		[Fact]
		public async Task GetItemQueryIterator_WithQueryDefinition_ShouldReturnCorrectResults()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);

			// Add test data
			var testData = new JObject
			{
				["id"] = "1",
				["name"] = "Test Item",
				["value"] = 42
			};

			// Need to access and populate the Documents property directly for testing
			fakeContainer.Documents.Add(testData);

			// Create a query definition
			var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE c.value = 42");

			// Act
			var iterator = fakeContainer.GetItemQueryIterator<JObject>(queryDefinition);

			// Assert
			Assert.NotNull(iterator);
			Assert.True(iterator.HasMoreResults);

			// Read results from the iterator
			var response = await iterator.ReadNextAsync();

			// Verify response properties
			Assert.Single(response);
			Assert.Equal("Test Item", response.First()["name"].ToString());
			Assert.Equal(42, response.First()["value"].Value<int>());

			// Verify iterator is now empty (consumed)
			Assert.False(iterator.HasMoreResults);

			// Reading again should return empty results
			var emptyResponse = await iterator.ReadNextAsync();
			Assert.Empty(emptyResponse);
		}
	}
}
