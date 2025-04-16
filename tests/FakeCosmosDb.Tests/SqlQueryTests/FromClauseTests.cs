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

    [Fact]
    public async Task QueryWithoutFromClause_ShouldReturnAllItems()
    {
        // Arrange
        var container = await InitializeContainerAsync();

        // Create test items
        var testItems = new List<TestItem>();
        for (int i = 1; i <= 5; i++)
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

        _output.WriteLine($"Created {testItems.Count} test items");

        // Act - Query without FROM clause
        var query = new QueryDefinition("SELECT *");
        var iterator = container.GetItemQueryIterator<TestItem>(query);
        var response = await iterator.ReadNextAsync();
        var results = response.ToList();

        _output.WriteLine($"Query returned {results.Count} items");
        foreach (var item in results)
        {
            _output.WriteLine($"Item: Id={item.Id}, Name={item.Name}, Value={item.Value}");
        }

        // Assert
        results.Should().BeEquivalentTo(testItems);
    }

    [Fact]
    public async Task QueryWithExplicitFromClause_ShouldReturnAllItems()
    {
        // Arrange
        var container = await InitializeContainerAsync();

        // Create test items
        var testItems = new List<TestItem>();
        for (int i = 1; i <= 5; i++)
        {
            var item = new TestItem
            {
                Id = $"explicit-{i}",
                Name = $"Explicit Item {i}",
                Value = i * 10,
            };
            testItems.Add(item);
            await container.CreateItemAsync(item);
        }

        _output.WriteLine($"Created {testItems.Count} test items");

        // Act - Query with explicit FROM clause
        var query = new QueryDefinition("SELECT * FROM c");
        var iterator = container.GetItemQueryIterator<TestItem>(query);
        var response = await iterator.ReadNextAsync();
        var results = response.ToList();

        _output.WriteLine($"Query returned {results.Count} items");

        // Assert
        results.Should().BeEquivalentTo(testItems);
    }

    [Fact]
    public async Task QueryWithROOTIdentifier_ShouldReturnAllItems()
    {
        // Arrange
        var container = await InitializeContainerAsync();

        // Create test items
        var testItems = new List<TestItem>();
        for (int i = 1; i <= 5; i++)
        {
            var item = new TestItem
            {
                Id = $"root-{i}",
                Name = $"Root Item {i}",
                Value = i * 10,
            };
            testItems.Add(item);
            await container.CreateItemAsync(item);
        }

        _output.WriteLine($"Created {testItems.Count} test items");

        // Act - Query with ROOT identifier in FROM clause
        var query = new QueryDefinition("SELECT * FROM ROOT");
        var iterator = container.GetItemQueryIterator<TestItem>(query);
        var response = await iterator.ReadNextAsync();
        var results = response.ToList();

        _output.WriteLine($"Query returned {results.Count} items");

        // Assert
        results.Should().BeEquivalentTo(testItems);
    }

    [Fact]
    public async Task QueryWithContainerAlias_ShouldReturnAllItems()
    {
        // Arrange
        var container = await InitializeContainerAsync();

        // Create test items
        var testItems = new List<TestItem>();
        for (int i = 1; i <= 5; i++)
        {
            var item = new TestItem
            {
                Id = $"alias-{i}",
                Name = $"Alias Item {i}",
                Value = i * 10,
            };
            testItems.Add(item);
            await container.CreateItemAsync(item);
        }

        _output.WriteLine($"Created {testItems.Count} test items");

        // Act - Query with container alias using AS keyword
        var query = new QueryDefinition("SELECT p.Id, p.Name, p.Value FROM c AS p");
        var iterator = container.GetItemQueryIterator<TestItem>(query);
        var response = await iterator.ReadNextAsync();
        var results = response.ToList();

        _output.WriteLine($"Query returned {results.Count} items");

        // Assert
        results.Should().BeEquivalentTo(testItems);
    }

    [Fact]
    public async Task QueryWithContainerAliasWithoutAS_ShouldReturnAllItems()
    {
        // Arrange
        var container = await InitializeContainerAsync();

        // Create test items
        var testItems = new List<TestItem>();
        for (int i = 1; i <= 5; i++)
        {
            var item = new TestItem
            {
                Id = $"alias-no-as-{i}",
                Name = $"Alias No AS Item {i}",
                Value = i * 10,
            };
            testItems.Add(item);
            await container.CreateItemAsync(item);
        }

        _output.WriteLine($"Created {testItems.Count} test items");

        // Act - Query with container alias without AS keyword
        var query = new QueryDefinition("SELECT p.Id, p.Name, p.Value FROM c p");
        var iterator = container.GetItemQueryIterator<TestItem>(query);
        var response = await iterator.ReadNextAsync();
        var results = response.ToList();

        _output.WriteLine($"Query returned {results.Count} items");

        // Assert
        results.Should().BeEquivalentTo(testItems);
    }

    [Fact]
    public async Task QueryWithFullyQualifiedProperties_ShouldReturnProjectedItems()
    {
        // Arrange
        var container = await InitializeContainerAsync();

        // Create test items
        var testItems = new List<TestItem>();
        for (int i = 1; i <= 5; i++)
        {
            var item = new TestItem
            {
                Id = $"qualified-{i}",
                Name = $"Qualified Item {i}",
                Value = i * 10,
            };
            testItems.Add(item);
            await container.CreateItemAsync(item);
        }

        _output.WriteLine($"Created {testItems.Count} test items");

        // Act - Query with fully qualified properties
        var query = new QueryDefinition("SELECT p.Id, p.Name FROM c p");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();
        var results = response.ToList();

        _output.WriteLine($"Query returned {results.Count} items");
        foreach (var item in results)
        {
            _output.WriteLine($"Projected Item: {item}");
        }

        // Assert
        results.Count.Should().Be(testItems.Count);
        
        // Verify all items have the expected properties
        results.Should().AllSatisfy(item => 
        {
            item.Should().ContainKey("Id");
            item.Should().ContainKey("Name");
            item.Should().NotContainKey("Value"); // Value should not be included in projection
        });
        
        // Verify that the correct items were returned
        var expectedItems = testItems
            .Select(i => new { i.Id, i.Name })
            .ToList();
            
        results.Select(r => new { 
            Id = r["Id"].ToString(), 
            Name = r["Name"].ToString() 
        }).Should().BeEquivalentTo(expectedItems);
    }

    [Fact]
    public async Task QueryWithRequiredFromClause_WhenFiltering_ShouldReturnFilteredItems()
    {
        // Arrange
        var container = await InitializeContainerAsync();

        // Create test items
        var testItems = new List<TestItem>();
        for (int i = 1; i <= 5; i++)
        {
            var item = new TestItem
            {
                Id = $"filter-{i}",
                Name = $"Filter Item {i}",
                Value = i * 10,
            };
            testItems.Add(item);
            await container.CreateItemAsync(item);
        }

        _output.WriteLine($"Created {testItems.Count} test items");

        // Act - Query with FROM clause and WHERE filter
        var query = new QueryDefinition("SELECT * FROM c WHERE c.Value > 20");
        var iterator = container.GetItemQueryIterator<TestItem>(query);
        var response = await iterator.ReadNextAsync();
        var results = response.ToList();

        _output.WriteLine($"Query returned {results.Count} items");

        // Assert
        var expectedItems = testItems.Where(i => i.Value > 20).ToList();
        results.Should().BeEquivalentTo(expectedItems);
    }

    [Fact]
    public async Task QueryWithRequiredFromClause_WhenProjecting_ShouldReturnProjectedItems()
    {
        // Arrange
        var container = await InitializeContainerAsync();

        // Create test items
        var testItems = new List<TestItem>();
        for (int i = 1; i <= 5; i++)
        {
            var item = new TestItem
            {
                Id = $"project-{i}",
                Name = $"Project Item {i}",
                Value = i * 10,
            };
            testItems.Add(item);
            await container.CreateItemAsync(item);
        }

        _output.WriteLine($"Created {testItems.Count} test items");

        // Act - Query with FROM clause and projection
        var query = new QueryDefinition("SELECT c.Id, c.Name FROM c");
        var iterator = container.GetItemQueryIterator<JObject>(query);
        var response = await iterator.ReadNextAsync();
        var results = response.ToList();

        _output.WriteLine($"Query returned {results.Count} items");

        // Assert
        results.Count.Should().Be(testItems.Count);
        
        // Verify all items have the expected properties
        results.Should().AllSatisfy(item => 
        {
            item.Should().ContainKey("Id");
            item.Should().ContainKey("Name");
            item.Should().NotContainKey("Value"); // Value should not be included in projection
        });
    }
}
