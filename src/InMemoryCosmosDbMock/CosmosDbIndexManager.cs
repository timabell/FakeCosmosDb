// Indexing logic
namespace TimAbell.MockableCosmos;

public class CosmosDbIndexManager
{
    private readonly Dictionary<string, Dictionary<object, HashSet<string>>> _indexes = new();

    public void Index(JObject entity)
    {
        foreach (var property in entity.Properties())
        {
            var field = property.Name;
            var value = property.Value.ToString();

            if (!_indexes.ContainsKey(field))
                _indexes[field] = new Dictionary<object, HashSet<string>>();

            if (!_indexes[field].ContainsKey(value))
                _indexes[field][value] = new HashSet<string>();

            _indexes[field][value].Add(entity["id"].ToString());
        }
    }
}