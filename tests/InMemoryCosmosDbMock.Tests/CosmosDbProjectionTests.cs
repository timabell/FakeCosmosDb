using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TimAbell.MockableCosmos.Tests;

public class CosmosDbProjectionTests
{
    private readonly CosmosInMemoryCosmosDb _db = new();
    private readonly string _containerName = "ProjectionTest";

    public CosmosDbProjectionTests()
    {
        _db.AddContainerAsync(_containerName).Wait();
        SeedTestData().Wait();
    }

    private async Task SeedTestData()
    {
        // Add test items with various properties for testing projections
        await _db.AddItemAsync(_containerName, new
        {
            id = "1",
            Name = "John Smith",
            Email = "john.smith@example.com",
            Age = 30,
            Address = new { City = "New York", ZipCode = "10001", Country = "USA" },
            Tags = new[] { "developer", "c#", "azure" }
        });

        await _db.AddItemAsync(_containerName, new
        {
            id = "2",
            Name = "Jane Doe",
            Email = "jane.doe@example.com",
            Age = 25,
            Address = new { City = "Los Angeles", ZipCode = "90001", Country = "USA" },
            Tags = new[] { "designer", "ui", "ux" }
        });

        await _db.AddItemAsync(_containerName, new
        {
            id = "3",
            Name = "Bob Johnson",
            Email = "bob.johnson@example.com",
            Age = 40,
            Address = new { City = "Chicago", ZipCode = "60601", Country = "USA" },
            Tags = new[] { "manager", "agile", "scrum" }
        });
    }

    [Fact]
    public async Task Query_WithSelectProjection_ReturnsOnlyRequestedFields()
    {
        // Query with projection
        var results = await _db.QueryAsync(_containerName, "SELECT c.Name, c.Age FROM c");

        // Should return 3 items with only Name and Age (plus id for consistency)
        Assert.Equal(3, results.Count());

        foreach (var item in results)
        {
            // Verify only requested properties (plus id) are returned
            Assert.Equal(3, item.Count());
            Assert.Contains("id", item.Properties().Select(p => p.Name));
            Assert.Contains("Name", item.Properties().Select(p => p.Name));
            Assert.Contains("Age", item.Properties().Select(p => p.Name));

            // Verify other properties are not included
            Assert.DoesNotContain("Email", item.Properties().Select(p => p.Name));
            Assert.DoesNotContain("Address", item.Properties().Select(p => p.Name));
            Assert.DoesNotContain("Tags", item.Properties().Select(p => p.Name));
        }
    }

    [Fact]
    public async Task Query_WithNestedPropertyProjection_ReturnsNestedStructure()
    {
        // Query with nested property projection
        var results = await _db.QueryAsync(_containerName, "SELECT c.Name, c.Address.City FROM c");

        // Should return 3 items with Name and Address.City
        Assert.Equal(3, results.Count());

        foreach (var item in results)
        {
            // Verify Name property is included
            Assert.Contains("Name", item.Properties().Select(p => p.Name));

            // Verify Address property is included and has City
            Assert.Contains("Address", item.Properties().Select(p => p.Name));
            Assert.NotNull(item["Address"]["City"]);

            // Verify other Address properties are not included
            Assert.Null(item["Address"]["ZipCode"]);
            Assert.Null(item["Address"]["Country"]);
        }
    }

    [Fact]
    public async Task Query_WithLimit_ReturnsLimitedResults()
    {
        // Query with LIMIT
        var results = await _db.QueryAsync(_containerName, "SELECT * FROM c LIMIT 2");

        // Should return only 2 items
        Assert.Equal(2, results.Count());
    }

    [Fact]
    public async Task Query_WithProjectionAndFilter_ReturnsFilteredProjection()
    {
        // Query with both projection and filter
        var results = await _db.QueryAsync(_containerName, "SELECT c.Name, c.Age FROM c WHERE c.Age > 25");

        // Should return 2 items (John and Bob) with only Name and Age
        Assert.Equal(2, results.Count());

        foreach (var item in results)
        {
            // Verify only requested properties are returned
            Assert.Equal(3, item.Count()); // id, Name, Age
            Assert.Contains("id", item.Properties().Select(p => p.Name));
            Assert.Contains("Name", item.Properties().Select(p => p.Name));
            Assert.Contains("Age", item.Properties().Select(p => p.Name));

            // Verify age is > 25
            Assert.True((int)item["Age"] > 25);
        }
    }
}