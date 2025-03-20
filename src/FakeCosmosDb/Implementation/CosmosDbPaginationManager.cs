using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace TimAbell.FakeCosmosDb.Implementation;

public interface ICosmosDbPaginationManager
{
	(IEnumerable<JObject> Results, string ContinuationToken) GetPage(IEnumerable<JObject> results, int maxItemCount, string continuationToken = null);
}

public class CosmosDbPaginationManager : ICosmosDbPaginationManager
{
	private readonly Dictionary<string, PaginationState> _paginationStates = new();

	public (IEnumerable<JObject> Results, string ContinuationToken) GetPage(IEnumerable<JObject> results, int maxItemCount, string continuationToken = null)
	{
		var resultList = results.ToList();
		var totalItems = resultList.Count;

		if (totalItems == 0)
		{
			return (new List<JObject>(), null);
		}

		int startIndex = 0;
		if (!string.IsNullOrEmpty(continuationToken))
		{
			if (_paginationStates.TryGetValue(continuationToken, out var state))
			{
				startIndex = state.NextIndex;
			}
			else
			{
				// Invalid token, start from beginning
				startIndex = 0;
			}
		}

		int endIndex = Math.Min(startIndex + maxItemCount, totalItems);
		var pageResults = resultList.Skip(startIndex).Take(endIndex - startIndex).ToList();

		string nextToken = null;
		if (endIndex < totalItems)
		{
			// Create a token for the next page
			nextToken = Guid.NewGuid().ToString();
			_paginationStates[nextToken] = new PaginationState
			{
				NextIndex = endIndex,
				TotalItems = totalItems
			};
		}

		return (pageResults, nextToken);
	}

	private class PaginationState
	{
		public int NextIndex { get; set; }
		public int TotalItems { get; set; }
	}
}
