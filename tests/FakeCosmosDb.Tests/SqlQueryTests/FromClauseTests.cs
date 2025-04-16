using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;

namespace TimAbell.FakeCosmosDb.Tests.SqlQueryTests;

public class FromClauseTests
{
    private readonly FakeCosmosDb _cosmosDb;
    private readonly string _databaseName = "FromClauseTestDb";
    private readonly string _containerName = "FromClauseTestContainer";
    private readonly ITestOutputHelper _output;

    public FromClauseTests(ITestOutputHelper output)
    {
        _output = output;
        _cosmosDb = new FakeCosmosDb();
    }

    private async Task<Container> InitializeContainerAsync()
    {
        await _cosmosDb.CreateDatabaseIfNotExistsAsync(_databaseName);
        return _cosmosDb.GetContainer(_databaseName, _containerName);
    }

    public class TestItem
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Value { get; set; }
    }

    private async Task<List<TestItem>> CreateTestItemsAsync(Container container)
    {
        var testItems = new List<TestItem>();
        for (int i = 1; i <= 3; i++)
        {
            var item = new TestItem
            {
                Id = $"item-{i}",
                Name = $"Test Item {i}",
                Value = i * 10,
            };
            testItems.Add(item);
            await container.CreateItemAsync(item);
        }
        return testItems;
    }

    [Fact]
    public async Task FromClauseIsOptional_WhenNoFilteringOrProjection()
    {
        // Arrange
        var container = await InitializeContainerAsync();
        var testItems = await CreateTestItemsAsync(container);

        // Act - Query without FROM clause
        var query = new QueryDefinition("SELECT *");
        var iterator = container.GetItemQueryIterator<TestItem>(query);
        var results = await iterator.ReadNextAsync();

        // Assert
        results.ToList().Should().BeEquivalentTo(testItems);
    }

    [Fact]
    public async Task CanUseROOTIdentifier_InFromClause()
    {
        // Arrange
        var container = await InitializeContainerAsync();
        var testItems = await CreateTestItemsAsync(container);

        // Act - Query with ROOT identifier
        var query = new QueryDefinition("SELECT * FROM ROOT");
        var iterator = container.GetItemQueryIterator<TestItem>(query);
        var results = await iterator.ReadNextAsync();

        // Assert
        results.ToList().Should().BeEquivalentTo(testItems);
    }

    [Fact]
    public async Task CanAliasContainer_WithASKeyword()
    {
        // Arrange
        var container = await InitializeContainerAsync();
        var testItems = await CreateTestItemsAsync(container);

        // Act - Query with container alias using AS keyword
        var query = new QueryDefinition("SELECT p.Id, p.Name, p.Value FROM c AS p");
        var iterator = container.GetItemQueryIterator<TestItem>(query);
        var results = await iterator.ReadNextAsync();

        // Assert
        results.ToList().Should().BeEquivalentTo(testItems);
    }

    [Fact]
    public async Task CanAliasContainer_WithoutASKeyword()
    {
        // Arrange
        var container = await InitializeContainerAsync();
        var testItems = await CreateTestItemsAsync(container);

        // Act - Query with container alias without AS keyword
        var query = new QueryDefinition("SELECT p.Id, p.Name, p.Value FROM c p");
        var iterator = container.GetItemQueryIterator<TestItem>(query);
        var results = await iterator.ReadNextAsync();

        // Assert
        results.ToList().Should().BeEquivalentTo(testItems);
    }

    [Fact]
    public async Task FromClauseRequired_WhenFiltering()
    {
        // Arrange
        var container = await InitializeContainerAsync();
        var testItems = await CreateTestItemsAsync(container);

        // Act - Query with FROM clause and WHERE filter
        var query = new QueryDefinition("SELECT * FROM c WHERE c.Value > 10");
        var iterator = container.GetItemQueryIterator<TestItem>(query);
        var results = await iterator.ReadNextAsync();

        // Assert
        var expectedItems = testItems.Where(i => i.Value > 10).ToList();
        results.ToList().Should().BeEquivalentTo(expectedItems);
    }

    [Fact]
    public async Task FromClauseRequired_WhenProjecting()
    {
        // Arrange
        var container = await InitializeContainerAsync();
        var testItems = await CreateTestItemsAsync(container);

        // Act - Query with FROM clause and projection
        var query = new QueryDefinition("SELECT c.Id, c.Name FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var results = await iterator.ReadNextAsync();

        // Assert
        results.Count().Should().Be(testItems.Count);
        
        // Verify properties are correctly projected
        results.Should().AllSatisfy(item => 
        {
            item.Should().ContainKey("Id");
            item.Should().ContainKey("Name");
            item.Should().NotContainKey("Value");
        });
    }

    [Fact]
    public async Task PropertiesMustBeFullyQualified_WithContainerAlias()
    {
        // Arrange
        var container = await InitializeContainerAsync();
        var testItems = await CreateTestItemsAsync(container);

        // Act - Query with fully qualified properties
        var query = new QueryDefinition("SELECT p.Id, p.Name FROM c p");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var results = await iterator.ReadNextAsync();

        // Assert
        results.Count().Should().Be(testItems.Count);
        
        // Verify properties are correctly projected
        results.Should().AllSatisfy(item => 
        {
            item.Should().ContainKey("Id");
            item.Should().ContainKey("Name");
            item.Should().NotContainKey("Value");
        });
    }
}
