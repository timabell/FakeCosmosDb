// Query parsing logic

using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Sprache;

namespace TimAbell.MockableCosmos;

/// <summary>
/// Parses CosmosDB SQL queries using the Sprache parser combinator library.
/// </summary>
public class CosmosDbQueryParser : ICosmosDbQueryParser
{
    // Common parsers for SQL syntax elements
    private static readonly Parser<string> Spaces = Parse.WhiteSpace.Many().Text();

    private static Parser<T> Token<T>(Parser<T> parser)
    {
        return
            from leading in Spaces
            from item in parser
            from trailing in Spaces
            select item;
    }

    // SQL keywords (case insensitive)
    private static Parser<string> Keyword(string word)
    {
        return Parse.IgnoreCase(word).Then(_ => Token(Parse.Return(word)));
    }

    // Identifiers for table and column names
    private static readonly Parser<string> Identifier =
        Parse.Letter.Once().Concat(Parse.LetterOrDigit.Or(Parse.Char('_')).Many())
            .Select(chars => new string(chars.ToArray()));

    // Property path (e.g., "c.Address.City")
    private static readonly Parser<string> PropertyPath =
        Identifier.DelimitedBy(Parse.Char('.')).Select(parts => string.Join(".", parts));

    // String literals
    private static readonly Parser<string> StringLiteral =
        from open in Parse.Char('\'')
        from content in Parse.CharExcept('\'').Many().Text()
        from close in Parse.Char('\'')
        select content;

    // Numeric literals
    private static readonly Parser<double> NumberLiteral =
        from sign in Parse.Char('-').Optional()
        from whole in Parse.Digit.AtLeastOnce().Text()
        from dot in Parse.Char('.').Optional()
        from fraction in dot.IsEmpty
            ? Parse.Return("")
            : Parse.Digit.Many().Text()
        let number = (sign.IsDefined ? "-" : "") + whole + (dot.IsDefined ? "." + fraction : "")
        select double.Parse(number);

    // Boolean literals
    private static readonly Parser<bool> BooleanLiteral =
        Parse.IgnoreCase("true").Token().Return(true)
        .Or(Parse.IgnoreCase("false").Token().Return(false));

    // Null literal
    private static readonly Parser<object> NullLiteral =
        Parse.IgnoreCase("null").Token().Return((object)null);

    // Any literal value
    private static readonly Parser<object> Literal =
        StringLiteral.Select(s => (object)s)
        .Or(NumberLiteral.Select(n => (object)n))
        .Or(BooleanLiteral.Select(b => (object)b))
        .Or(NullLiteral);

    // Parsing SELECT clause
    private static readonly Parser<IEnumerable<string>> SelectClauseParser =
        from select in Keyword("SELECT")
        from items in Parse.Char('*').Token().Return(new[] { "*" })
                       .Or(PropertyPath.DelimitedBy(Parse.Char(',').Token()))
        select items;

    // Parsing FROM clause
    private static readonly Parser<Tuple<string, string>> FromClauseParser =
        from from in Keyword("FROM")
        from source in Identifier.Token()
        from alias in (
            from as_kw in Keyword("AS").Optional()
            from alias_id in Identifier.Token()
            select alias_id
        ).Optional()
        select Tuple.Create(source, alias.GetOrDefault());

    // Parse WHERE conditions
    private static readonly Parser<Tuple<string, string, object>> SimpleCondition =
        from field in PropertyPath.Token()
        from op in Parse.String("=").Or(Parse.String("!=")).Or(Parse.String("<>"))
                .Or(Parse.String("<=")).Or(Parse.String(">="))
                .Or(Parse.String("<")).Or(Parse.String(">")).Token()
        from value in Literal
        select Tuple.Create(field, op, value);

    private static readonly Parser<Tuple<string, string, object>> FunctionCondition =
        from name in Parse.IgnoreCase("CONTAINS").Or(Parse.IgnoreCase("STARTSWITH"))
        from lparen in Parse.Char('(').Token()
        from field in PropertyPath.Token()
        from comma in Parse.Char(',').Token()
        from value in Literal
        from rparen in Parse.Char(')').Token()
        select Tuple.Create(field, name.ToUpperInvariant(), value);

    private static readonly Parser<IEnumerable<Tuple<string, string, object>>> WhereClauseParser =
        from where in Keyword("WHERE")
        from conditions in (
            from condition in SimpleCondition.Or(FunctionCondition)
            from and in Keyword("AND").Optional()
            select condition
        ).AtLeastOnce()
        select conditions;

    // Parse ORDER BY
    private static readonly Parser<Tuple<string, string>> OrderByItem =
        from field in PropertyPath.Token()
        from direction in Parse.IgnoreCase("DESC").Token().Return("DESC")
                        .Or(Parse.IgnoreCase("ASC").Token().Return("ASC"))
                        .Optional()
        select Tuple.Create(field, direction.GetOrDefault("ASC"));

    private static readonly Parser<IEnumerable<Tuple<string, string>>> OrderByClauseParser =
        from orderBy in Parse.IgnoreCase("ORDER").Token()
                        .Then(_ => Parse.IgnoreCase("BY").Token())
        from items in OrderByItem.DelimitedBy(Parse.Char(',').Token())
        select items;

    // Parse LIMIT
    private static readonly Parser<int> LimitClauseParser =
        from limit in Keyword("LIMIT")
        from value in Parse.Number
        select int.Parse(value);

    // Parse complete query
    private static readonly Parser<ParsedQuery> QueryParser =
        from select in SelectClauseParser
        from from in FromClauseParser
        from where in WhereClauseParser.Optional()
        from orderBy in OrderByClauseParser.Optional()
        from limit in LimitClauseParser.Optional()
        select CreateParsedQuery(select, from, where, orderBy, limit);

    private static ParsedQuery CreateParsedQuery(
        IEnumerable<string> select,
        Tuple<string, string> from,
        IEnumerable<Tuple<string, string, object>> where,
        IEnumerable<Tuple<string, string>> orderBy,
        int? limit)
    {
        var result = new ParsedQuery
        {
            PropertyPaths = select.ToList(),
            FromName = from.Item1,
            FromAlias = from.Item2,
            Limit = limit.GetValueOrDefault()
        };

        if (where != null)
        {
            result.WhereConditions = where.Select(c => new WhereCondition
            {
                PropertyPath = c.Item1,
                Operator = c.Item2,
                Value = c.Item3 != null ? JToken.FromObject(c.Item3) : null
            }).ToList();
        }

        if (orderBy != null)
        {
            result.OrderBy = orderBy.Select(o => new OrderInfo
            {
                PropertyPath = o.Item1,
                Direction = o.Item2
            }).ToList();
        }

        return result;
    }

    /// <summary>
    /// Parses a CosmosDB SQL query.
    /// </summary>
    public ParsedQuery Parse(string query)
    {
        try
        {
            return QueryParser.End().Parse(query);
        }
        catch (ParseException ex)
        {
            throw new FormatException($"Failed to parse CosmosDB SQL query: {ex.Message}", ex);
        }
    }
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