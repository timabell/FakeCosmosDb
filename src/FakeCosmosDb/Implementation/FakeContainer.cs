using System;
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

	// The partition key path for this container (e.g., "/id", "/category")
	public string PartitionKeyPath { get; set; } = "/id";

	public FakeContainer(ILogger logger = null)
	{
		_queryParser = new CosmosDbSqlQueryParser(logger);
		_queryExecutor = new CosmosDbQueryExecutor(logger);
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
		throw new NotImplementedException();
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
			// Find by both ID and partition key
			item = _store.FirstOrDefault(doc =>
				(doc["id"]?.ToString() == id || doc["Id"]?.ToString() == id) &&
				doc["partitionKey"]?.ToString() == partitionKeyValue);
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
		var query = queryDefinition?.QueryText;
		var parsedQuery = _queryParser.Parse(query);
		var results = _queryExecutor.Execute(parsedQuery, _store);

		// Apply pagination if needed
		IEnumerable<JObject> pagedResults = results;
		string nextToken = null;

		if (requestOptions?.MaxItemCount > 0)
		{
			var paginationResult = _paginationManager.GetPage(results, requestOptions.MaxItemCount.Value, continuationToken);
			pagedResults = paginationResult.Item1;
			nextToken = paginationResult.Item2;
		}

		// Convert results to the target type
		var convertedResults = pagedResults.Select(item => item.ToObject<T>()).ToList();

		return new FakeFeedIterator<T>(convertedResults, nextToken);
	}

	public override FeedIterator GetItemQueryStreamIterator(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new NotImplementedException();
	}

	public override FeedIterator<T> GetItemQueryIterator<T>(string queryText = null, string continuationToken = null, QueryRequestOptions requestOptions = null)
	{
		throw new NotImplementedException();
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
	private class FakeFeedIterator<T> : FeedIterator<T>
	{
		private readonly List<T> _results;
		private readonly string _continuationToken;
		private bool _consumed;

		public FakeFeedIterator(IEnumerable<T> results, string continuationToken)
		{
			_results = results.ToList();
			_continuationToken = continuationToken;
			_consumed = false;
		}

		public override bool HasMoreResults => !_consumed;

		public override Task<FeedResponse<T>> ReadNextAsync(CancellationToken cancellationToken = default)
		{
			if (_consumed)
			{
				return Task.FromResult(CreateEmptyResponse());
			}

			_consumed = true;
			return Task.FromResult(CreateResponse(_results, _continuationToken));
		}

		private FeedResponse<T> CreateResponse(IEnumerable<T> results, string continuationToken)
		{
			// Since we can't directly instantiate FeedResponse<T>, create a custom implementation
			return new FakeFeedResponse<T>(results, continuationToken);
		}

		private FeedResponse<T> CreateEmptyResponse()
		{
			return new FakeFeedResponse<T>(Enumerable.Empty<T>(), null);
		}
	}

	// Custom implementation of FeedResponse<T> for the FakeFeedIterator
	private class FakeFeedResponse<T> : FeedResponse<T>
	{
		private readonly List<T> _results;

		public FakeFeedResponse(IEnumerable<T> results, string continuationToken)
		{
			_results = results.ToList();
			ContinuationToken = continuationToken;
		}

		public override string ContinuationToken { get; }

		public override IEnumerator<T> GetEnumerator()
		{
			return _results.GetEnumerator();
		}

		public override int Count => _results.Count;
		public override string IndexMetrics { get; }

		public override HttpStatusCode StatusCode { get; }
		public override double RequestCharge => 0;

		public override Headers Headers => new Headers();
		public override IEnumerable<T> Resource { get; }

		// Additional FeedResponse<T> methods/properties
		public override string ActivityId => string.Empty;
		public override string ETag => string.Empty;
		public override CosmosDiagnostics Diagnostics { get; }
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
