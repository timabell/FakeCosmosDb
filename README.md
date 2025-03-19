# TimAbell.FakeCosmosDb - Fake CosmosClientfor Testing

An in-memory implementation of Azure Cosmos DB for testing .NET applications without external dependencies.

Will store and round-trip saved data for easy integration testing of code that depends on CosmosDB without having to use the unreliable CosmosDB emulator or depend on the Azure cloud for tests.

The CosmosClient API is non-trivial to implement, so if you want realistic tests you either have to create your own simplisitic abstraction and reduce the quality of your integration test coverage, or depend on the real/emulated CosmosDb from Microsoft that results in slower and less reliable tests requiring significantly more setup infrastructure.

I have created this library as a working starting point for some amount of the CosmosClient API, and hope that by providing this under MIT license to the world that as a community we can turn this into a fairly complete implementation that makes for a realistic test fake to give us fast and useful integration tests for systems that have chosen to use CosmosDB.

It would be better to [not use CosmosDb unless you really need it](https://0x5.uk/2025/01/14/don't-use-azure-cosmosdb/) but we don't always get to choose.

This is a [Fake not a Mock](https://martinfowler.com/articles/mocksArentStubs.html) because it doesn't just give pre-canned responses to API calls, it provides a basic working implementation with in-memory storage that's sufficient for tests.

Using this library also makes it trivial to reset test data state as you can just throw away the instance and new up another one at almost no cost.

## Current Status

This project is under active development with several features implemented and others still in progress:

- ✅ In-memory document storage with container support
- ✅ SQL query parsing for basic SELECT, WHERE, and ORDER BY operations
- ✅ Support for common SQL functions (CONTAINS, STARTSWITH, etc.)
- ✅ Pagination with continuation tokens
- ✅ Adapter for switching between fake and real Cosmos DB
- ⚠️ Some advanced query features are partially implemented
- ❌ Stored procedures not yet supported
- ❌ Change feed not yet implemented

**Contributions are welcome!** If you encounter missing features or bugs, please open an issue or submit a pull request.

## Installation

<https://www.nuget.org/packages/FakeCosmosDb>

```sh
dotnet add package FakeCosmosDb
```

## Usage Examples

### Example 1: Direct Substitution for CosmosClient

```csharp
// Production code in Startup.cs or Program.cs
public void ConfigureServices(IServiceCollection services)
{
	// Register CosmosClient for production
	services.AddSingleton(sp =>
		new CosmosClient(Configuration.GetConnectionString("CosmosDb")));

	// Register your services that depend on CosmosClient
	services.AddScoped<IUserRepository, UserRepository>();
}

// Test code
public class ApiTests : IClassFixture<WebApplicationFactory<Program>>
{
	private readonly WebApplicationFactory<Program> _factory;
	private readonly HttpClient _client;

	public ApiTests(WebApplicationFactory<Program> factory)
	{
		// Replace the real CosmosClient with our fake for testing
		_factory = factory.WithWebHostBuilder(builder =>
		{
			builder.ConfigureServices(services =>
			{
				// Remove the registered CosmosClient
				var descriptor = services.SingleOrDefault(d =>
					d.ServiceType == typeof(CosmosClient));
				if (descriptor != null)
					services.Remove(descriptor);

				// Add our fake implementation
				var fakeClient = new FakeCosmosDb();
				fakeClient.AddContainerAsync("users").Wait();

				// Seed test data
				fakeClient.AddItemAsync("users", new { id = "1", name = "Test User" }).Wait();

				// Register the fake client
				services.AddSingleton<CosmosClient>(fakeClient);
			});
		});

		_client = _factory.CreateClient();
	}

	[Fact]
	public async Task GetUser_ReturnsUser()
	{
		// Act - test the API endpoint that uses CosmosClient
		var response = await _client.GetAsync("/api/users/1");

		// Assert
		response.EnsureSuccessStatusCode();
		var content = await response.Content.ReadAsStringAsync();
		Assert.Contains("Test User", content);
	}
}
```

### Example 2: Using with Abstraction Layer

```csharp
// Production code in Startup.cs or Program.cs
public void ConfigureServices(IServiceCollection services)
{
	// For production, use the CosmosDbAdapter with real CosmosClient
	services.AddSingleton<ICosmosDb>(sp =>
		new CosmosDbAdapter(Configuration.GetConnectionString("CosmosDb")));

	// Register your services that depend on ICosmosDb
	services.AddScoped<IUserRepository, UserRepository>();
}

// Test code
public class ApiTests : IClassFixture<WebApplicationFactory<Program>>
{
	private readonly WebApplicationFactory<Program> _factory;
	private readonly HttpClient _client;

	public ApiTests(WebApplicationFactory<Program> factory)
	{
		// Replace the CosmosDbAdapter with our fake for testing
		_factory = factory.WithWebHostBuilder(builder =>
		{
			builder.ConfigureServices(services =>
			{
				// Remove the registered CosmosDbAdapter
				var descriptor = services.SingleOrDefault(d =>
					d.ServiceType == typeof(ICosmosDb));
				if (descriptor != null)
					services.Remove(descriptor);

				// Add our fake implementation
				var fakeDb = new FakeCosmosDb();
				fakeDb.AddContainerAsync("users").Wait();

				// Seed test data
				fakeDb.AddItemAsync("users", new { id = "1", name = "Test User" }).Wait();

				// Register the fake implementation
				services.AddSingleton<ICosmosDb>(fakeDb);
			});
		});

		_client = _factory.CreateClient();
	}

	[Fact]
	public async Task GetUser_ReturnsUser()
	{
		// Act - test the API endpoint that uses ICosmosDb
		var response = await _client.GetAsync("/api/users/1");

		// Assert
		response.EnsureSuccessStatusCode();
		var content = await response.Content.ReadAsStringAsync();
		Assert.Contains("Test User", content);
	}
}
```

## Supported Query Features

```csharp
// Basic filtering
var results = await cosmosDb.QueryAsync("users", "SELECT * FROM c WHERE c.name = 'Alice'");

// Numeric comparisons
var results = await cosmosDb.QueryAsync("users", "SELECT * FROM c WHERE c.age > 30");

// String functions
var results = await cosmosDb.QueryAsync("users", "SELECT * FROM c WHERE CONTAINS(c.name, 'Ali')");

// Projections
var results = await cosmosDb.QueryAsync("users", "SELECT c.name, c.email FROM c");

// Pagination
var (items, token) = await cosmosDb.QueryWithPaginationAsync(
	"users",
	"SELECT * FROM c",
	maxItemCount: 10
);
```

## Test Coverage

The project includes extensive unit tests covering:

- Basic CRUD operations
- SQL query parsing and execution
- Data type handling in queries
- Projections and field selection
- Pagination functionality
- Index-based query optimization
- Adapter functionality for real Cosmos DB

Run the tests with:

```sh
dotnet test -v n
```

## Internal Architecture

The project is organized into several key components:

### Core Components

- **FakeCosmosDb**: Main entry point that implements the ICosmosDb interface
- **FakeContainer**: In-memory implementation of a Cosmos DB container
- **FakeDatabase**: Manages collections of containers

### Query Processing

- **Parsing/CosmosDbSqlQueryParser**: Parses SQL queries into an abstract syntax tree
- **Parsing/CosmosDbSqlAst**: Defines the structure for the abstract syntax tree
- **Parsing/CosmosDbSqlGrammar**: Contains the grammar rules for SQL parsing
- **CosmosDbQueryExecutor**: Executes parsed queries against the in-memory data

### Data Management

- **CosmosDbIndexManager**: Manages indexes for optimizing query performance
- **CosmosDbPaginationManager**: Handles pagination with continuation tokens

### Real Cosmos DB Support

- **CosmosDbAdapter**: Adapter for the real Cosmos DB client, implementing ICosmosDb

## Contributing

Contributions are welcome! Here's how you can help:

1. **Report bugs or request features** by opening an issue
2. **Implement missing functionality** by submitting a pull request
3. **Improve documentation** to help others use the library
4. **Add test cases** to increase coverage

### Development Guidelines

- Follow TDD practices for all new features
- Keep SQL query parsing logic in the parsing code, not in the executor
- Use method chaining syntax (`.Select()`, `.Where()`) instead of LINQ query syntax (`from ... in ... select`) due to netstandard2.1 compatibility
- Respect the .editorconfig settings

## License

MIT License - [License information](LICENSE)
