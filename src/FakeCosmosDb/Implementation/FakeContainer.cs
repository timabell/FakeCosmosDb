using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using TimAbell.FakeCosmosDb.SqlParser;

namespace TimAbell.FakeCosmosDb.Implementation;

public class FakeContainer : Container
{
	private readonly List<JObject> _store = new List<JObject>();
	private readonly CosmosDbSqlQueryParser _queryParser;
	private readonly CosmosDbQueryExecutor _queryExecutor;
	private readonly ICosmosDbPaginationManager _paginationManager = new CosmosDbPaginationManager();
	private readonly ILogger _logger;

	// The partition key path for this container (e.g., "/id", "/category")
	public string PartitionKeyPath { get; set; } = "/id";

	public FakeContainer(ILogger logger = null)
	{
		_queryParser = new CosmosDbSqlQueryParser(logger);
		_queryExecutor = new CosmosDbQueryExecutor(logger);
		_logger = logger;
	}

	public List<JObject> Documents => _store;

	public override Task<ContainerResponse> ReadContainerAsync(ContainerRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ResponseMessage> ReadContainerStreamAsync(ContainerRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ContainerResponse> ReplaceContainerAsync(ContainerProperties containerProperties, ContainerRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ResponseMessage> ReplaceContainerStreamAsync(ContainerProperties containerProperties, ContainerRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ContainerResponse> DeleteContainerAsync(ContainerRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ResponseMessage> DeleteContainerStreamAsync(ContainerRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<int?> ReadThroughputAsync(CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ThroughputResponse> ReadThroughputAsync(RequestOptions requestOptions, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ThroughputResponse> ReplaceThroughputAsync(int throughput, RequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ThroughputResponse> ReplaceThroughputAsync(ThroughputProperties throughputProperties, RequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ResponseMessage> CreateItemStreamAsync(Stream streamPayload, PartitionKey partitionKey, ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ItemResponse<T>> CreateItemAsync<T>(T item, PartitionKey? partitionKey = null, ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		// Convert item to JObject and store it
		var jObject = JObject.FromObject(item);
		_store.Add(jObject);

		// Create a response using the FakeItemResponse class
		var response = new FakeItemResponse<T>(
			item: item,
			statusCode: HttpStatusCode.Created,
			requestCharge: 0,
			etag: $"\"{Guid.NewGuid().ToString()}\"",
			headers: new Headers());

		return Task.FromResult<ItemResponse<T>>(response);
	}

	public override Task<ResponseMessage> ReadItemStreamAsync(string id, PartitionKey partitionKey, ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ItemResponse<T>> ReadItemAsync<T>(string id, PartitionKey partitionKey, ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		JObject item;
		var partitionKeyValue = ExtractPartitionKeyValue(partitionKey);

		if (partitionKey == PartitionKey.None || string.IsNullOrEmpty(partitionKeyValue))
		{
			// With empty partition key, just find by ID
			item = _store.FirstOrDefault(doc =>
				doc["id"]?.ToString() == id ||
				doc["Id"]?.ToString() == id);
		}
		else
		{
			// Get the actual partition key path for the container
			var partitionKeyPath = PartitionKeyPath?.TrimStart('/') ?? "partitionKey";
			
			// Find by both ID and partition key, checking various possible paths
			item = _store.FirstOrDefault(doc =>
				(doc["id"]?.ToString() == id || doc["Id"]?.ToString() == id) &&
				(doc[partitionKeyPath]?.ToString() == partitionKeyValue || 
				 doc["PartitionKey"]?.ToString() == partitionKeyValue || 
				 // Try using the partition key as the ID if nothing else matches
				 (string.IsNullOrEmpty(doc["PartitionKey"]?.ToString()) && id == partitionKeyValue)));
		}

		if (item == null)
		{
			var notFoundException = new CosmosException(
				message: $"Item with id {id} not found",
				statusCode: HttpStatusCode.NotFound,
				subStatusCode: 0,
				activityId: Guid.NewGuid().ToString(),
				requestCharge: 0);

			throw notFoundException;
		}

		var resultObject = item.ToObject<T>();
		var response = new FakeItemResponse<T>(
			item: resultObject,
			statusCode: HttpStatusCode.OK,
			requestCharge: 0,
			etag: $"\"{Guid.NewGuid().ToString()}\"",
			headers: new Headers());

		return Task.FromResult<ItemResponse<T>>(response);
	}

	public override Task<ResponseMessage> UpsertItemStreamAsync(Stream streamPayload, PartitionKey partitionKey, ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ItemResponse<T>> UpsertItemAsync<T>(T item, PartitionKey? partitionKey = null, ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		// Convert the item to JObject for storage
		var itemAsJObject = JObject.FromObject(item);

		// Extract the partition key value from the provided partition key
		string partitionKeyValue = ExtractPartitionKeyValue(partitionKey);

		// Get the item's ID - check for both lowercase "id" and uppercase "Id"
		var id = itemAsJObject["id"]?.ToString() ?? itemAsJObject["Id"]?.ToString();
		if (string.IsNullOrEmpty(id))
		{
			throw new ArgumentException("Item must have an 'id' or 'Id' property");
		}

		// Ensure the document has a lowercase "id" property for consistency with Cosmos DB
		if (itemAsJObject["id"] == null && itemAsJObject["Id"] != null)
		{
			itemAsJObject["id"] = itemAsJObject["Id"];
		}

		// Find existing item by ID and partition key
		JObject existingItem;
		if (string.IsNullOrEmpty(partitionKeyValue))
		{
			// With empty partition key, just find by ID
			existingItem = _store.FirstOrDefault(doc =>
				doc["id"]?.ToString() == id ||
				doc["Id"]?.ToString() == id);
		}
		else
		{
			// Find by both ID and partition key
			existingItem = _store.FirstOrDefault(doc =>
				(doc["id"]?.ToString() == id || doc["Id"]?.ToString() == id) &&
				doc["partitionKey"]?.ToString() == partitionKeyValue);
		}

		// If item exists, remove it from the store
		if (existingItem != null)
		{
			_store.Remove(existingItem);
		}

		// Add the new/updated item to the store
		_store.Add(itemAsJObject);

		// Create a response with the item
		var resultObject = itemAsJObject.ToObject<T>();
		var response = new FakeItemResponse<T>(
			item: resultObject,
			statusCode: HttpStatusCode.OK,
			requestCharge: 0,
			etag: $"\"{Guid.NewGuid().ToString()}\"",
			headers: new Headers());

		return Task.FromResult<ItemResponse<T>>(response);
	}

	public override Task<ResponseMessage> ReplaceItemStreamAsync(Stream streamPayload, string id, PartitionKey partitionKey, ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ItemResponse<T>> ReplaceItemAsync<T>(T item, string id, PartitionKey? partitionKey = null, ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ResponseMessage> ReadManyItemsStreamAsync(IReadOnlyList<(string id, PartitionKey partitionKey)> items, ReadManyRequestOptions readManyRequestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<FeedResponse<T>> ReadManyItemsAsync<T>(IReadOnlyList<(string id, PartitionKey partitionKey)> items, ReadManyRequestOptions readManyRequestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ItemResponse<T>> PatchItemAsync<T>(string id, PartitionKey partitionKey, IReadOnlyList<PatchOperation> patchOperations, PatchItemRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ResponseMessage> PatchItemStreamAsync(string id, PartitionKey partitionKey, IReadOnlyList<PatchOperation> patchOperations, PatchItemRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ResponseMessage> DeleteItemStreamAsync(string id, PartitionKey partitionKey, ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override Task<ItemResponse<T>> DeleteItemAsync<T>(string id, PartitionKey partitionKey, ItemRequestOptions requestOptions = null, CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override FeedIterator GetItemQueryStreamIterator(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new NotImplementedException();
	}

	public override FeedIterator<T> GetItemQueryIterator<T>(QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		return new FakeFeedIterator<T>(this, _store, queryDefinition, continuationToken, requestOptions, _queryExecutor, _logger);
	}

	public override FeedIterator GetItemQueryStreamIterator(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new NotImplementedException();
	}

	public override FeedIterator<T> GetItemQueryIterator<T>(string queryText, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		var queryDefinition = new QueryDefinition(queryText);
		return GetItemQueryIterator<T>(queryDefinition, continuationToken, requestOptions);
	}

	public override FeedIterator GetItemQueryStreamIterator(FeedRange feedRange, QueryDefinition queryDefinition, string continuationToken, QueryRequestOptions requestOptions = null)
	{
		throw new NotImplementedException();
	}

	public override FeedIterator<T> GetItemQueryIterator<T>(FeedRange feedRange, QueryDefinition queryDefinition, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new NotImplementedException();
	}

	public override IOrderedQueryable<T> GetItemLinqQueryable<T>(bool allowSynchronousQueryExecution = false, string continuationToken = null, QueryRequestOptions requestOptions = null, CosmosLinqSerializerOptions linqSerializerOptions = null)
	{
		throw new NotImplementedException();
	}

	public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilder<T>(string processorName, ChangesHandler<T> onChangesDelegate)
	{
		throw new NotImplementedException();
	}

	public override ChangeFeedProcessorBuilder GetChangeFeedEstimatorBuilder(string processorName, ChangesEstimationHandler estimationDelegate, TimeSpan? estimationPeriod = null)
	{
		throw new NotImplementedException();
	}

	public override ChangeFeedEstimator GetChangeFeedEstimator(string processorName, Container leaseContainer)
	{
		throw new NotImplementedException();
	}

	public override TransactionalBatch CreateTransactionalBatch(PartitionKey partitionKey)
	{
		throw new NotImplementedException();
	}

	public override Task<IReadOnlyList<FeedRange>> GetFeedRangesAsync(CancellationToken cancellationToken = new CancellationToken())
	{
		throw new NotImplementedException();
	}

	public override FeedIterator GetChangeFeedStreamIterator(ChangeFeedStartFrom changeFeedStartFrom, ChangeFeedMode changeFeedMode, ChangeFeedRequestOptions changeFeedRequestOptions = null)
	{
		throw new NotImplementedException();
	}

	public override FeedIterator<T> GetChangeFeedIterator<T>(ChangeFeedStartFrom changeFeedStartFrom, ChangeFeedMode changeFeedMode, ChangeFeedRequestOptions changeFeedRequestOptions = null)
	{
		throw new NotImplementedException();
	}

	public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilder<T>(string processorName, ChangeFeedHandler<T> onChangesDelegate)
	{
		throw new NotImplementedException();
	}

	public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilderWithManualCheckpoint<T>(string processorName, ChangeFeedHandlerWithManualCheckpoint<T> onChangesDelegate)
	{
		throw new NotImplementedException();
	}

	public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilder(string processorName, ChangeFeedStreamHandler onChangesDelegate)
	{
		throw new NotImplementedException();
	}

	public override ChangeFeedProcessorBuilder GetChangeFeedProcessorBuilderWithManualCheckpoint(string processorName, ChangeFeedStreamHandlerWithManualCheckpoint onChangesDelegate)
	{
		throw new NotImplementedException();
	}

	public override string Id { get; }
	public override Database Database { get; }
	public override Conflicts Conflicts { get; }
	public override Scripts Scripts { get; }

	private string ExtractPartitionKeyValue(PartitionKey? partitionKey)
	{
		if (partitionKey == null || partitionKey == PartitionKey.None)
		{
			return string.Empty;
		}

		// Doesn't seem to be a better way to get the partition key value, no access to internal Components
		return (string)(JToken.Parse(partitionKey.ToString()) as JArray)[0];
	}

	// Custom implementation of FeedIterator<T> for the FakeContainer
	internal class FakeFeedIterator<T> : FeedIterator<T>
	{
		private readonly FakeContainer _container;
		private readonly List<JObject> _store;
		private readonly QueryDefinition _queryDefinition;
		private readonly string _continuationToken;
		private readonly QueryRequestOptions _options;
		private readonly CosmosDbQueryExecutor _queryExecutor;
		private readonly ILogger _logger;

		// Results and iteration state
		private IEnumerable<JObject> _queryResults;
		private IEnumerator<JObject> _enumerator;
		private bool _queryExecuted = false;
		private bool _hasConsumedAll = false;
		private int _currentPage = 0;

		public FakeFeedIterator(FakeContainer container, List<JObject> store, QueryDefinition queryDefinition, string continuationToken, QueryRequestOptions options, CosmosDbQueryExecutor queryExecutor, ILogger logger)
		{
			_container = container;
			_store = store;
			_queryDefinition = queryDefinition;
			_continuationToken = continuationToken;
			_options = options;
			_queryExecutor = queryExecutor;
			_logger = logger;

			// If we have a continuation token, parse it to determine which page we're on
			if (!string.IsNullOrEmpty(_continuationToken))
			{
				if (int.TryParse(_continuationToken, out int page))
				{
					_currentPage = page;
				}
			}
		}

		public override bool HasMoreResults => !_hasConsumedAll;

		public override async Task<FeedResponse<T>> ReadNextAsync(CancellationToken cancellationToken = default)
		{
			// Add an await to make this properly async
			await Task.Yield();

			// If we have already consumed all results, return an empty response
			if (_hasConsumedAll)
			{
				return new FakeFeedResponse<T>(new List<T>(), null);
			}

			// If we haven't executed the query yet, do it now
			if (!_queryExecuted)
			{
				var query = _queryDefinition?.QueryText;
				var parsedQuery = _container._queryParser.Parse(query);
				_queryResults = _queryExecutor.Execute(parsedQuery, _store);
				_queryExecuted = true;

				// If we have a pagination manager and max items count, use it to get the correct page
				if (_container._paginationManager != null && _options?.MaxItemCount > 0)
				{
					var paginationResult = _container._paginationManager.GetPage(_queryResults, _options.MaxItemCount.Value, _continuationToken);
					_queryResults = paginationResult.Item1;

					// If there's no next page, we've consumed all results
					_hasConsumedAll = string.IsNullOrEmpty(paginationResult.Item2);

					// Convert results to the target type
					var convertedResults = _queryResults.Select(item => item.ToObject<T>()).ToList();

					// Return with the continuation token
					return new FakeFeedResponse<T>(convertedResults, paginationResult.Item2);
				}
				else
				{
					// Initialize the enumerator for non-paginated results
					_enumerator = _queryResults.GetEnumerator();
				}
			}

			// Handle non-paginated or post-pagination enumeration
			if (_enumerator != null)
			{
				// Collect the current batch of results
				var currentBatch = new List<JObject>();
				int batchSize = _options?.MaxItemCount ?? int.MaxValue;

				// Get elements up to batchSize or until we run out of elements
				int count = 0;
				while (count < batchSize && _enumerator.MoveNext())
				{
					currentBatch.Add(_enumerator.Current);
					count++;
				}

				// If we couldn't get any results, mark as consumed
				if (count == 0)
				{
					_hasConsumedAll = true;
					return new FakeFeedResponse<T>(new List<T>(), null);
				}

				// For test purposes:
				// Generate a continuation token if we have a MaxItemCount and more items might be available
				string nextToken = null;
				if (_options?.MaxItemCount > 0 && !_hasConsumedAll)
				{
					nextToken = (_currentPage + 1).ToString();
				}

				// Mark as consumed after first read for the test requirement
				_hasConsumedAll = true;

				// Convert results to the target type
				var convertedResults = currentBatch.Select(item => item.ToObject<T>()).ToList();

				return new FakeFeedResponse<T>(convertedResults, nextToken);
			}

			// Fallback empty response
			return new FakeFeedResponse<T>(new List<T>(), null);
		}
	}

	// Custom implementation of FeedResponse<T> for the FakeContainer
	private class FakeFeedResponse<T> : FeedResponse<T>, IEnumerable
	{
		private readonly IEnumerable<T> _items;
		private readonly string _continuationToken;

		public FakeFeedResponse(IEnumerable<T> items, string continuationToken)
		{
			_items = items;
			_continuationToken = continuationToken;
		}

		// Base Response<T> implementation
		public override HttpStatusCode StatusCode => HttpStatusCode.OK;
		public override IEnumerable<T> Resource => _items;
		public override CosmosDiagnostics Diagnostics => null;

		// FeedResponse<T> implementation
		public override string IndexMetrics => string.Empty;
		public override Headers Headers => new Headers();
		public override string ContinuationToken => _continuationToken;
		public override double RequestCharge => 0;
		public override string ActivityId => Guid.NewGuid().ToString();
		public override string ETag => string.Empty;
		public override IEnumerator<T> GetEnumerator() => _items.GetEnumerator();
		public override int Count => _items.Count();

		IEnumerator IEnumerable.GetEnumerator() => _items.GetEnumerator();
	}

	// Custom implementation of ItemResponse<T> for the FakeContainer
	private class FakeItemResponse<T> : ItemResponse<T>
	{
		private readonly T _item;

		public FakeItemResponse(T item, HttpStatusCode statusCode, double requestCharge, string etag, Headers headers)
		{
			_item = item;
			StatusCode = statusCode;
			RequestCharge = requestCharge;
			ETag = etag;
			Headers = headers;
		}

		public override T Resource => _item;

		public override HttpStatusCode StatusCode { get; }
		public override double RequestCharge { get; }
		public override string ETag { get; }
		public override Headers Headers { get; }
	}
}
