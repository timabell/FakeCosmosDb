# Tim Abell FakeCosmosDb

This library provides an **in-memory CosmosDB fake** for unit testing, with optional support for running tests against the **real CosmosDB Emulator**.

## Features
✅ Supports SQL-like queries (`SELECT * FROM c WHERE c.Name = 'Alice'`)  
✅ Fully **in-memory**, no dependencies required  
✅ **Multi-container** support  
✅ Can switch between **fake mode and real CosmosDB mode**  
✅ **GitHub Actions CI/CD ready**  
✅ **Efficient indexing** for fast queries  
✅ Support for **pagination** with continuation tokens  
✅ Advanced query features like **CONTAINS**, **STARTSWITH**, and nested properties  
✅ **SELECT projections** to return only specific fields  

## Installation
To use the in-memory CosmosDB fake, install via NuGet:

```sh
dotnet add package # TBC
```

## Quick Start

```csharp
// Create an instance of the fake
var cosmosDb = new FakeCosmosDb();

// Create a container
await cosmosDb.AddContainerAsync("Users");

// Add an item
await cosmosDb.AddItemAsync("Users", new { 
    id = "1", 
    Name = "Alice", 
    Email = "alice@example.com" 
});

// Query items
var results = await cosmosDb.QueryAsync("Users", "SELECT * FROM c WHERE c.Name = 'Alice'");

// Use pagination
var (pageResults, continuationToken) = await cosmosDb.QueryWithPaginationAsync(
    "Users", 
    "SELECT * FROM c", 
    maxItemCount: 10
);

// Get the next page using the continuation token
var (nextPageResults, nextContinuationToken) = await cosmosDb.QueryWithPaginationAsync(
    "Users", 
    "SELECT * FROM c", 
    maxItemCount: 10, 
    continuationToken: continuationToken
);
```

## Advanced Queries

The fake supports a wide range of CosmosDB SQL query features:

### Basic Filtering

```csharp
// Equality
var results = await cosmosDb.QueryAsync("Users", "SELECT * FROM c WHERE c.Name = 'Alice'");

// Numeric comparisons
var results = await cosmosDb.QueryAsync("Users", "SELECT * FROM c WHERE c.Age > 30");

// String functions
var results = await cosmosDb.QueryAsync("Users", "SELECT * FROM c WHERE CONTAINS(c.Name, 'Ali')");
var results = await cosmosDb.QueryAsync("Users", "SELECT * FROM c WHERE STARTSWITH(c.Email, 'alice')");
```

### Projections

```csharp
// Select specific fields
var results = await cosmosDb.QueryAsync("Users", "SELECT c.Name, c.Email FROM c");

// Select nested properties
var results = await cosmosDb.QueryAsync("Users", "SELECT c.Name, c.Address.City FROM c");
```

### Ordering and Limiting

```csharp
// Order by a field
var results = await cosmosDb.QueryAsync("Users", "SELECT * FROM c ORDER BY c.Name");

// Limit results
var results = await cosmosDb.QueryAsync("Users", "SELECT * FROM c LIMIT 10");
```

## Testing with xUnit

### Using the Fake in Tests

```csharp
public class UserServiceTests
{
    private readonly ICosmosDb _db;
    private readonly UserService _userService;

    public UserServiceTests()
    {
        _db = new FakeCosmosDb();
        _db.AddContainerAsync("Users").Wait();
        
        // Inject the fake into your service
        _userService = new UserService(_db);
    }

    [Fact]
    public async Task GetUserByName_ReturnsCorrectUser()
    {
        // Arrange
        await _db.AddItemAsync("Users", new { id = "1", Name = "Alice" });
        
        // Act
        var user = await _userService.GetUserByNameAsync("Alice");
        
        // Assert
        Assert.Equal("Alice", user.Name);
    }
}
```

### Testing with Both Fake and Real CosmosDB

You can run the same tests against both the in-memory fake and the real CosmosDB emulator:

```csharp
public class CosmosDbTests
{
    public static IEnumerable<object[]> TestConfigurations()
    {
        yield return new object[] { new FakeCosmosDb() };
        yield return new object[] { new CosmosDbAdapter("AccountEndpoint=https://localhost:8081;AccountKey=your-key;") };
    }

    [Theory]
    [MemberData(nameof(TestConfigurations))]
    public async Task Can_Insert_And_Query_Item(ICosmosDb db)
    {
        var containerName = "TestContainer";
        await db.AddContainerAsync(containerName);

        var user = new { id = "1", Name = "Alice", Age = 30 };
        await db.AddItemAsync(containerName, user);

        var results = await db.QueryAsync(containerName, "SELECT * FROM c WHERE c.Name = 'Alice'");

        Assert.Single(results);
        Assert.Equal("Alice", results.First()["Name"]);
    }
}
```

## Running Tests with the Real CosmosDB Emulator
To run tests against a **real CosmosDB Emulator**, start the emulator in Docker:

```sh
docker run -p 8081:8081 -d mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator
```

Then, run the tests:

```sh
dotnet test
```

## Using with Dependency Injection

```csharp
public void ConfigureServices(IServiceCollection services)
{
    // For local development and testing
    if (Environment.IsDevelopment())
    {
        var cosmosDb = new FakeCosmosDb();
        cosmosDb.AddContainerAsync("Users").Wait();
        services.AddSingleton<ICosmosDb>(cosmosDb);
    }
    else
    {
        // For production, use the real CosmosDB
        services.AddSingleton<ICosmosDb>(sp => 
            new CosmosDbAdapter(Configuration.GetConnectionString("CosmosDb")));
    }
    
    // Register your services that depend on ICosmosDb
    services.AddScoped<IUserRepository, UserRepository>();
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
