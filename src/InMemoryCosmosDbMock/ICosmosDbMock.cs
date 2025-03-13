using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;

public interface ICosmosDbMock
{
    Task AddContainerAsync(string containerName);
    Task AddItemAsync(string containerName, object entity);
    Task<IEnumerable<JObject>> QueryAsync(string containerName, string sql);
}
