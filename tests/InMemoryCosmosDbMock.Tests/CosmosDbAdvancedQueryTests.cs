using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TimAbell.MockableCosmos.Tests;

public class CosmosDbAdvancedQueryTests
{
    private readonly CosmosInMemoryCosmosDb _db = new();
    private readonly string _containerName = "AdvancedQueryTest";

    public CosmosDbAdvancedQueryTests()
    {
        _db.AddContainerAsync(_containerName).Wait();
        SeedTestData().Wait();
    }

    private async Task SeedTestData()
    {
        // Add test items with various properties for testing different query functions
        await _db.AddItemAsync(_containerName, new
        {
            id = "1",
            Name = "John Smith",
            Email = "john.smith@example.com",
            Age = 30,
            Address = new { City = "New York", ZipCode = "10001" }
        });

        await _db.AddItemAsync(_containerName, new
        {
            id = "2",
            Name = "Jane Doe",
            Email = "jane.doe@example.com",
            Age = 25,
            Address = new { City = "Los Angeles", ZipCode = "90001" }
        });

        await _db.AddItemAsync(_containerName, new
        {
            id = "3",
            Name = "Bob Johnson",
            Email = "bob.johnson@example.com",
            Age = 40,
            Address = new { City = "Chicago", ZipCode = "60601" }
        });

        await _db.AddItemAsync(_containerName, new
        {
            id = "4",
            Name = "Alice Brown",
            Email = "alice.brown@example.com",
            Age = 35,
            Address = new { City = "San Francisco", ZipCode = "94105" }
        });
    }

    [Fact]
    public async Task Query_WithContains_ReturnsCorrectItems()
    {
        // Query using CONTAINS function
        var results = await _db.QueryAsync(_containerName, "SELECT * FROM c WHERE CONTAINS(c.Name, 'oh')");

        // Should match "John Smith" and "Bob Johnson"
        Assert.Equal(2, results.Count());
        Assert.Contains(results, item => item["Name"].ToString() == "John Smith");
        Assert.Contains(results, item => item["Name"].ToString() == "Bob Johnson");
    }

    [Fact]
    public async Task Query_WithStartsWith_ReturnsCorrectItems()
    {
        // Query using STARTSWITH function
        var results = await _db.QueryAsync(_containerName, "SELECT * FROM c WHERE STARTSWITH(c.Name, 'J')");

        // Should match "John Smith" and "Jane Doe"
        Assert.Equal(2, results.Count());
        Assert.Contains(results, item => item["Name"].ToString() == "John Smith");
        Assert.Contains(results, item => item["Name"].ToString() == "Jane Doe");
    }

    [Fact]
    public async Task Query_WithGreaterThan_ReturnsCorrectItems()
    {
        // Query using greater than comparison
        var results = await _db.QueryAsync(_containerName, "SELECT * FROM c WHERE c.Age > 30");

        // Should match "Bob Johnson" (40) and "Alice Brown" (35)
        Assert.Equal(2, results.Count());
        Assert.Contains(results, item => item["Name"].ToString() == "Bob Johnson");
        Assert.Contains(results, item => item["Name"].ToString() == "Alice Brown");
    }

    [Fact]
    public async Task Query_WithLessThan_ReturnsCorrectItems()
    {
        // Query using less than comparison
        var results = await _db.QueryAsync(_containerName, "SELECT * FROM c WHERE c.Age < 30");

        // Should match "Jane Doe" (25)
        Assert.Single(results);
        Assert.Equal("Jane Doe", results.First()["Name"].ToString());
    }

    [Fact]
    public async Task Query_WithNestedProperty_ReturnsCorrectItems()
    {
        // Query using a nested property
        var results = await _db.QueryAsync(_containerName, "SELECT * FROM c WHERE c.Address.City = 'Chicago'");

        // Should match "Bob Johnson"
        Assert.Single(results);
        Assert.Equal("Bob Johnson", results.First()["Name"].ToString());
    }
}