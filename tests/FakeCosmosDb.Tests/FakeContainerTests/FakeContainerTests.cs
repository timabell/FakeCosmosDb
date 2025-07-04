using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.Implementation;
using TimAbell.FakeCosmosDb.Tests.Utilities;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests.FakeContainerTests
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
				["value"] = 42,
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

		[Fact]
		public async Task ReadItemAsync_ExistingItem_ReturnsCorrectItemResponse()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);

			// Use simple string constants for partition keys
			const string partitionKeyValue = "partition1";

			// Add test data with the simple string partition key
			var testItem = new JObject
			{
				["id"] = "item1",
				["partitionKey"] = partitionKeyValue,
				["name"] = "Test Item 1",
				["value"] = 42,
			};

			fakeContainer.Documents.Add(testItem);

			// Log data store content
			_output.WriteLine($"Added item to store: {testItem}");
			_output.WriteLine($"Store count: {fakeContainer.Documents.Count}");

			// Act
			var response = await fakeContainer.ReadItemAsync<JObject>("item1", new PartitionKey(partitionKeyValue));

			// Assert
			response.Should().NotBeNull();
			response.StatusCode.Should().Be(HttpStatusCode.OK);
			response.Resource.Should().NotBeNull();
			response.Resource["id"].ToString().Should().Be("item1");
			response.Resource["partitionKey"].ToString().Should().Be(partitionKeyValue);
			response.Resource["name"].ToString().Should().Be("Test Item 1");
			response.Resource["value"].Value<int>().Should().Be(42);
			response.ETag.Should().NotBeNullOrEmpty();
		}

		[Fact]
		public async Task ReadItemAsync_NonExistentItem_ThrowsCosmosExceptionWithNotFound()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);

			// Use simple string constants for partition keys
			const string partitionKeyValue = "partition1";

			var testItem = new JObject
			{
				["id"] = "item1",
				["partitionKey"] = partitionKeyValue,
				["name"] = "Test Item 1",
			};
			fakeContainer.Documents.Add(testItem);

			// Act & Assert
			Func<Task> action = async () => await fakeContainer.ReadItemAsync<JObject>("nonexistent", new PartitionKey(partitionKeyValue));

			await action.Should().ThrowAsync<CosmosException>()
				.Where(ex => ex.StatusCode == HttpStatusCode.NotFound);
		}

		[Fact]
		public async Task ReadItemAsync_SameIdDifferentPartitionKey_ReturnsDifferentItems()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);

			// In real Cosmos DB, the partition key is a path to a property in the document
			// For this test, we'll simulate a container with partition key path "/category"
			fakeContainer.PartitionKeyPath = "/category";

			// Add two items with same ID but different category values (which act as partition keys)
			var item1 = new JObject
			{
				["id"] = "duplicateId",
				["category"] = "electronics",
				["name"] = "First Item",
				["value"] = 42,
			};

			var item2 = new JObject
			{
				["id"] = "duplicateId",
				["category"] = "books",
				["name"] = "Second Item",
				["value"] = 84,
			};

			fakeContainer.Documents.Add(item1);
			fakeContainer.Documents.Add(item2);

			// Act
			var response1 = await fakeContainer.ReadItemAsync<JObject>("duplicateId", new PartitionKey("electronics"));
			var response2 = await fakeContainer.ReadItemAsync<JObject>("duplicateId", new PartitionKey("books"));

			// Assert
			response1.Resource["name"].ToString().Should().Be("First Item");
			response1.Resource["value"].Value<int>().Should().Be(42);

			response2.Resource["name"].ToString().Should().Be("Second Item");
			response2.Resource["value"].Value<int>().Should().Be(84);
		}

		[Fact]
		public async Task ReadItemAsync_DifferentIdSamePartitionKey_ReturnsDifferentItems()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);

			// Use simple string constants for partition keys
			const string partitionKeyValue = "samePartition";

			// Add two items with different IDs but same partition key
			var item1 = new JObject
			{
				["id"] = "id1",
				["partitionKey"] = partitionKeyValue,
				["name"] = "First Item",
				["value"] = 42,
			};

			var item2 = new JObject
			{
				["id"] = "id2",
				["partitionKey"] = partitionKeyValue,
				["name"] = "Second Item",
				["value"] = 84,
			};

			fakeContainer.Documents.Add(item1);
			fakeContainer.Documents.Add(item2);

			// Act
			var response1 = await fakeContainer.ReadItemAsync<JObject>("id1", new PartitionKey(partitionKeyValue));
			var response2 = await fakeContainer.ReadItemAsync<JObject>("id2", new PartitionKey(partitionKeyValue));

			// Assert
			response1.Resource["name"].ToString().Should().Be("First Item");
			response1.Resource["value"].Value<int>().Should().Be(42);

			response2.Resource["name"].ToString().Should().Be("Second Item");
			response2.Resource["value"].Value<int>().Should().Be(84);
		}

		[Fact]
		public async Task ReadItemAsync_EmptyPartitionKey_MatchesAnyPartitionKey()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);

			// Use simple string constants for partition keys
			const string partitionKeyValue = "partition1";

			var testItem = new JObject
			{
				["id"] = "item1",
				["partitionKey"] = partitionKeyValue,
				["name"] = "Test Item 1",
			};
			fakeContainer.Documents.Add(testItem);

			// Act - use empty partition key
			var response = await fakeContainer.ReadItemAsync<JObject>("item1", new PartitionKey(string.Empty));

			// Assert
			response.Should().NotBeNull();
			response.Resource["id"].ToString().Should().Be("item1");
			response.Resource["partitionKey"].ToString().Should().Be(partitionKeyValue);
		}

		[Fact]
		public async Task ReadItemAsync_ComplexType_DeserializesCorrectly()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);

			// Use simple string constants for partition keys
			const string partitionKeyValue = "partition1";

			var testItem = new JObject
			{
				["id"] = "complex1",
				["partitionKey"] = partitionKeyValue,
				["name"] = "Complex Item",
				["nestedObject"] = new JObject
				{
					["property1"] = "value1",
					["property2"] = 42,
				},
				["arrayProperty"] = new JArray { 1, 2, 3 },
			};
			fakeContainer.Documents.Add(testItem);

			// Act
			var response = await fakeContainer.ReadItemAsync<JObject>("complex1", new PartitionKey(partitionKeyValue));

			// Assert
			response.Should().NotBeNull();
			response.Resource["id"].ToString().Should().Be("complex1");
			response.Resource["nestedObject"]["property1"].ToString().Should().Be("value1");
			response.Resource["nestedObject"]["property2"].Value<int>().Should().Be(42);
			response.Resource["arrayProperty"].Type.Should().Be(JTokenType.Array);
			response.Resource["arrayProperty"].ToObject<int[]>().Should().BeEquivalentTo(new[] { 1, 2, 3 });
		}

		[Fact]
		public async Task DebugPartitionKeyBehavior()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);

			// Create a document with explicit format for the partition key
			var testItem = new JObject
			{
				["id"] = "debug1",
				["partitionKey"] = "debugPartition",
				["content"] = "Debug Test Item",
			};
			fakeContainer.Documents.Add(testItem);

			// Log for visibility
			_output.WriteLine($"Added item: {testItem}");

			// Create a PartitionKey and examine its string representation
			var pk = new PartitionKey("debugPartition");
			_output.WriteLine($"PartitionKey as string: '{pk.ToString()}'");
			_output.WriteLine($"PartitionKey.ToString().Trim('\"'): '{pk.ToString().Trim('\"')}'");

			// Manually perform the search logic from ReadItemAsync
			string partitionKeyValue = pk.ToString().Trim('"');
			_output.WriteLine($"partitionKeyValue after trim: '{partitionKeyValue}'");

			// Manual check using the same logic as the method
			var foundItem = fakeContainer.Documents.FirstOrDefault(doc =>
				doc["id"]?.ToString() == "debug1" &&
				(string.IsNullOrEmpty(partitionKeyValue) || doc["partitionKey"]?.ToString() == partitionKeyValue));

			_output.WriteLine($"Manual search result: {(foundItem != null ? "Found" : "Not Found")}");
			if (foundItem != null)
			{
				_output.WriteLine($"Found item: {foundItem}");
				_output.WriteLine($"partitionKey in document: '{foundItem["partitionKey"]}'");
				_output.WriteLine($"Comparison result: {foundItem["partitionKey"]?.ToString() == partitionKeyValue}");
			}

			// Try the actual method
			try
			{
				var response = await fakeContainer.ReadItemAsync<JObject>("debug1", pk);
				_output.WriteLine($"ReadItemAsync succeeded, status: {response.StatusCode}");
			}
			catch (Exception ex)
			{
				_output.WriteLine($"ReadItemAsync failed: {ex.Message}");
			}

			// Assert
			Assert.True(true);  // This test is just for debugging
		}

		[Fact]
		public async Task UpsertItemAsync_NewItem_ShouldAddToContainer()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);
			const string partitionKeyValue = "partition1";

			var newItem = new JObject
			{
				["id"] = "newItem1",
				["partitionKey"] = partitionKeyValue,
				["name"] = "New Item",
				["value"] = 42,
			};

			// Act
			var response = await fakeContainer.UpsertItemAsync<JObject>(newItem, new PartitionKey(partitionKeyValue));

			// Assert
			response.Should().NotBeNull();
			response.StatusCode.Should().Be(HttpStatusCode.OK);
			response.Resource.Should().NotBeNull();
			response.Resource["id"].ToString().Should().Be("newItem1");
			response.Resource["partitionKey"].ToString().Should().Be(partitionKeyValue);
			response.Resource["name"].ToString().Should().Be("New Item");
			response.Resource["value"].Value<int>().Should().Be(42);
			response.ETag.Should().NotBeNullOrEmpty();

			// Verify item was added to container
			fakeContainer.Documents.Should().HaveCount(1);
			fakeContainer.Documents.First().Should().BeEquivalentTo(newItem);
		}

		[Fact]
		public async Task UpsertItemAsync_ExistingItem_ShouldUpdateInContainer()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);
			const string partitionKeyValue = "partition1";

			// Add initial item
			var existingItem = new JObject
			{
				["id"] = "existingItem1",
				["partitionKey"] = partitionKeyValue,
				["name"] = "Original Item",
				["value"] = 42,
			};
			fakeContainer.Documents.Add(existingItem);

			// Create updated version of the item
			var updatedItem = new JObject
			{
				["id"] = "existingItem1",
				["partitionKey"] = partitionKeyValue,
				["name"] = "Updated Item",
				["value"] = 84,
				["newProperty"] = "added property",
			};

			// Act
			var response = await fakeContainer.UpsertItemAsync<JObject>(updatedItem, new PartitionKey(partitionKeyValue));

			// Assert
			response.Should().NotBeNull();
			response.StatusCode.Should().Be(HttpStatusCode.OK);
			response.Resource.Should().NotBeNull();
			response.Resource["id"].ToString().Should().Be("existingItem1");
			response.Resource["partitionKey"].ToString().Should().Be(partitionKeyValue);
			response.Resource["name"].ToString().Should().Be("Updated Item");
			response.Resource["value"].Value<int>().Should().Be(84);
			response.Resource["newProperty"].ToString().Should().Be("added property");
			response.ETag.Should().NotBeNullOrEmpty();

			// Verify container still has only one item (the updated one)
			fakeContainer.Documents.Should().HaveCount(1);
			fakeContainer.Documents.First().Should().BeEquivalentTo(updatedItem);
		}

		[Fact]
		public async Task UpsertItemAsync_SameIdDifferentPartitionKey_ShouldAddNewItem()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);
			const string partitionKey1 = "partition1";
			const string partitionKey2 = "partition2";

			// Add initial item
			var existingItem = new JObject
			{
				["id"] = "duplicateId",
				["partitionKey"] = partitionKey1,
				["name"] = "Item in Partition 1",
				["value"] = 42,
			};
			fakeContainer.Documents.Add(existingItem);

			// Create new item with same ID but different partition key
			var newItem = new JObject
			{
				["id"] = "duplicateId",
				["partitionKey"] = partitionKey2,
				["name"] = "Item in Partition 2",
				["value"] = 84,
			};

			// Act
			var response = await fakeContainer.UpsertItemAsync<JObject>(newItem, new PartitionKey(partitionKey2));

			// Assert
			response.Should().NotBeNull();
			response.StatusCode.Should().Be(HttpStatusCode.OK);
			response.Resource.Should().NotBeNull();
			response.Resource["id"].ToString().Should().Be("duplicateId");
			response.Resource["partitionKey"].ToString().Should().Be(partitionKey2);
			response.Resource["name"].ToString().Should().Be("Item in Partition 2");
			response.Resource["value"].Value<int>().Should().Be(84);
			response.ETag.Should().NotBeNullOrEmpty();

			// Verify container now has two items
			fakeContainer.Documents.Should().HaveCount(2);
			fakeContainer.Documents.Should().ContainEquivalentOf(existingItem);
			fakeContainer.Documents.Should().ContainEquivalentOf(newItem);
		}

		[Fact]
		public async Task ReadItemAsync_WithUppercaseId_ShouldFindItem()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);
			const string partitionKeyValue = "partition1";

			// Add test data with uppercase "Id" property
			var testItem = new JObject
			{
				["Id"] = "uppercaseId1",
				["partitionKey"] = partitionKeyValue,
				["name"] = "Test Item With Uppercase Id",
				["value"] = 42,
			};

			fakeContainer.Documents.Add(testItem);

			// Act
			var response = await fakeContainer.ReadItemAsync<JObject>("uppercaseId1", new PartitionKey(partitionKeyValue));

			// Assert
			response.Should().NotBeNull();
			response.StatusCode.Should().Be(HttpStatusCode.OK);
			response.Resource.Should().NotBeNull();
			response.Resource["Id"].ToString().Should().Be("uppercaseId1");
			response.Resource["partitionKey"].ToString().Should().Be(partitionKeyValue);
			response.Resource["name"].ToString().Should().Be("Test Item With Uppercase Id");
			response.Resource["value"].Value<int>().Should().Be(42);
			response.ETag.Should().NotBeNullOrEmpty();
		}

		[Fact]
		public async Task UpsertItemAsync_WithUppercaseId_ShouldAddToContainer()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);
			const string partitionKeyValue = "partition1";

			var newItem = new JObject
			{
				["Id"] = "newUppercaseItem1",
				["partitionKey"] = partitionKeyValue,
				["name"] = "New Item With Uppercase Id",
				["value"] = 42,
			};

			// Act
			var response = await fakeContainer.UpsertItemAsync<JObject>(newItem, new PartitionKey(partitionKeyValue));

			// Assert
			response.Should().NotBeNull();
			response.StatusCode.Should().Be(HttpStatusCode.OK);
			response.Resource.Should().NotBeNull();
			response.Resource["Id"].ToString().Should().Be("newUppercaseItem1");
			response.Resource["id"].ToString().Should().Be("newUppercaseItem1"); // Should also have lowercase id
			response.Resource["partitionKey"].ToString().Should().Be(partitionKeyValue);
			response.Resource["name"].ToString().Should().Be("New Item With Uppercase Id");
			response.Resource["value"].Value<int>().Should().Be(42);
			response.ETag.Should().NotBeNullOrEmpty();

			// Verify item was added to container
			fakeContainer.Documents.Should().HaveCount(1);
			fakeContainer.Documents.First()["Id"].ToString().Should().Be("newUppercaseItem1");
			fakeContainer.Documents.First()["id"].ToString().Should().Be("newUppercaseItem1");
		}

		[Fact]
		public async Task UpsertItemAsync_UpdateExistingItemWithUppercaseId_ShouldUpdateInContainer()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);
			const string partitionKeyValue = "partition1";

			// Add initial item with uppercase Id
			var existingItem = new JObject
			{
				["Id"] = "existingUppercaseItem1",
				["partitionKey"] = partitionKeyValue,
				["name"] = "Original Item With Uppercase Id",
				["value"] = 42,
			};
			fakeContainer.Documents.Add(existingItem);

			// Create updated version of the item
			var updatedItem = new JObject
			{
				["Id"] = "existingUppercaseItem1",
				["partitionKey"] = partitionKeyValue,
				["name"] = "Updated Item With Uppercase Id",
				["value"] = 84,
				["newProperty"] = "added property",
			};

			// Act
			var response = await fakeContainer.UpsertItemAsync<JObject>(updatedItem, new PartitionKey(partitionKeyValue));

			// Assert
			response.Should().NotBeNull();
			response.StatusCode.Should().Be(HttpStatusCode.OK);
			response.Resource.Should().NotBeNull();
			response.Resource["Id"].ToString().Should().Be("existingUppercaseItem1");
			response.Resource["id"].ToString().Should().Be("existingUppercaseItem1"); // Should also have lowercase id
			response.Resource["partitionKey"].ToString().Should().Be(partitionKeyValue);
			response.Resource["name"].ToString().Should().Be("Updated Item With Uppercase Id");
			response.Resource["value"].Value<int>().Should().Be(84);
			response.Resource["newProperty"].ToString().Should().Be("added property");
			response.ETag.Should().NotBeNullOrEmpty();

			// Verify container still has only one item (the updated one)
			fakeContainer.Documents.Should().HaveCount(1);
			fakeContainer.Documents.First()["Id"].ToString().Should().Be("existingUppercaseItem1");
			fakeContainer.Documents.First()["id"].ToString().Should().Be("existingUppercaseItem1");
			fakeContainer.Documents.First()["name"].ToString().Should().Be("Updated Item With Uppercase Id");
		}

		[Fact]
		public async Task ReadItemAsync_FindItemWithUppercaseIdUsingLowercaseId_ShouldFindItem()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);
			const string partitionKeyValue = "partition1";

			// Add test data with uppercase "Id" property
			var testItem = new JObject
			{
				["Id"] = "mixedCaseTest",
				["partitionKey"] = partitionKeyValue,
				["name"] = "Test Item With Uppercase Id",
				["value"] = 42,
			};

			fakeContainer.Documents.Add(testItem);

			// Act - search using lowercase id
			var response = await fakeContainer.ReadItemAsync<JObject>("mixedCaseTest", new PartitionKey(partitionKeyValue));

			// Assert
			response.Should().NotBeNull();
			response.StatusCode.Should().Be(HttpStatusCode.OK);
			response.Resource.Should().NotBeNull();
			response.Resource["Id"].ToString().Should().Be("mixedCaseTest");
			response.Resource["partitionKey"].ToString().Should().Be(partitionKeyValue);
		}

		[Fact]
		public async Task GetItemQueryIterator_WithTopClause_ShouldLimitResults()
		{
			// Arrange
			var fakeContainer = new FakeContainer(_logger);

			// Add test data
			for (int i = 1; i <= 10; i++)
			{
				var testItem = new JObject
				{
					["id"] = i.ToString(),
					["name"] = $"Test Item {i}",
					["value"] = i * 10,
				};
				fakeContainer.Documents.Add(testItem);
			}

			// Create a query definition with TOP clause
			var queryDefinition = new QueryDefinition("SELECT TOP 3 * FROM c ORDER BY c.value");

			// Act
			var iterator = fakeContainer.GetItemQueryIterator<JObject>(queryDefinition);

			// Assert
			Assert.NotNull(iterator);
			Assert.True(iterator.HasMoreResults);

			// Read results from the iterator
			var response = await iterator.ReadNextAsync();

			// Verify response properties
			response.Should().HaveCount(3);
			response.First()["value"].Value<int>().Should().Be(10);
			response.Last()["value"].Value<int>().Should().Be(30);

			// Verify iterator is now empty (consumed)
			Assert.False(iterator.HasMoreResults);
		}
	}
}
