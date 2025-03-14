using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using TimAbell.MockableCosmos.Parsing;

namespace TimAbell.MockableCosmos;

/// <summary>
/// Parses CosmosDB SQL queries.
/// </summary>
public class CosmosDbQueryParser : ICosmosDbQueryParser
{
    private readonly SpracheSqlQueryParser _parser = new SpracheSqlQueryParser();

    /// <summary>
    /// Parses a CosmosDB SQL query.
    /// </summary>
    public ParsedQuery Parse(string query)
    {
        return _parser.Parse(query);
    }
}

/// <summary>
/// Interface for CosmosDB query parsers.
/// </summary>
public interface ICosmosDbQueryParser
{
    /// <summary>
    /// Parses a CosmosDB SQL query.
    /// </summary>
    ParsedQuery Parse(string query);
}

/// <summary>
/// Represents a parsed CosmosDB SQL query.
/// </summary>
public class ParsedQuery
{
    /// <summary>
    /// List of property paths to select from the results.
    /// </summary>
    public List<string> PropertyPaths { get; set; } = new List<string>();

    /// <summary>
    /// The name of the container or collection in the FROM clause.
    /// </summary>
    public string FromName { get; set; }

    /// <summary>
    /// The alias used for the FROM source, if any.
    /// </summary>
    public string FromAlias { get; set; }

    /// <summary>
    /// List of conditions in the WHERE clause.
    /// </summary>
    public List<WhereCondition> WhereConditions { get; set; } = new List<WhereCondition>();

    /// <summary>
    /// List of ORDER BY clauses.
    /// </summary>
    public List<OrderInfo> OrderBy { get; set; }

    /// <summary>
    /// LIMIT value, if any.
    /// </summary>
    public int Limit { get; set; }

    /// <summary>
    /// Whether this is a SELECT * query.
    /// </summary>
    public bool IsSelectAll => PropertyPaths.Count == 1 && PropertyPaths[0] == "*";
}

/// <summary>
/// Represents a condition in a WHERE clause.
/// </summary>
public class WhereCondition
{
    /// <summary>
    /// The property path to test.
    /// </summary>
    public string PropertyPath { get; set; }

    /// <summary>
    /// The operator to apply.
    /// </summary>
    public string Operator { get; set; }

    /// <summary>
    /// The value to compare with.
    /// </summary>
    public JToken Value { get; set; }
}

/// <summary>
/// Represents an ORDER BY clause.
/// </summary>
public class OrderInfo
{
    /// <summary>
    /// The property path to order by.
    /// </summary>
    public string PropertyPath { get; set; }

    /// <summary>
    /// The direction to order in (ASC or DESC).
    /// </summary>
    public string Direction { get; set; } = "ASC";
}