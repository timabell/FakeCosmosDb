using System;
using System.Collections.Generic;
using System.Linq;
using Sprache;

namespace TimAbell.MockableCosmos.Parsing;

/// <summary>
/// Grammar parser for CosmosDB SQL queries using Sprache.
/// </summary>
public static class CosmosDbSqlGrammar
{
    // Common parsers for SQL syntax elements

    // Whitespace handling
    private static readonly Parser<string> Spaces = Parse.WhiteSpace.Many().Text();
    private static readonly Parser<T> Token<T>(this Parser<T> parser) =>
        from leading in Spaces
        from item in parser
        from trailing in Spaces
        select item;

    // SQL keywords (case insensitive)
    private static Parser<string> Keyword(string word) =>
        Parse.IgnoreCase(word).Token();

    // Identifiers for table and column names
    public static readonly Parser<string> Identifier =
        from first in Parse.Letter.Once()
        from rest in Parse.LetterOrDigit.Or(Parse.Char('_')).Many()
        select new string(first.Concat(rest).ToArray());

    // Property path (e.g., "c.Address.City")
    public static readonly Parser<string> PropertyPath =
        from parts in Identifier.DelimitedBy(Parse.Char('.'))
        select string.Join(".", parts);

    // String literals
    public static readonly Parser<string> StringLiteral =
        from open in Parse.Char('\'')
        from content in Parse.CharExcept('\'').Many().Text()
        from close in Parse.Char('\'')
        select content;

    // Numeric literals
    public static readonly Parser<double> NumberLiteral =
        from sign in Parse.Char('-').Optional()
        from whole in Parse.Digit.AtLeastOnce().Text()
        from dot in Parse.Char('.').Optional()
        from fraction in dot.IsEmpty
            ? Parse.Return("")
            : Parse.Digit.Many().Text()
        select double.Parse((sign.IsDefined ? "-" : "") + whole + (dot.IsDefined ? "." + fraction : ""));

    // Boolean literals
    public static readonly Parser<bool> BooleanLiteral =
        Keyword("true").Return(true)
        .Or(Keyword("false").Return(false));

    // Null literal
    public static readonly Parser<object> NullLiteral =
        Keyword("null").Return((object)null);

    // Any literal value
    public static readonly Parser<object> Literal =
        StringLiteral.Select(s => (object)s)
        .Or(NumberLiteral.Select(n => (object)n))
        .Or(BooleanLiteral.Select(b => (object)b))
        .Or(NullLiteral);

    // Basic expressions
    private static readonly Parser<Expression> LiteralExpression =
        Literal.Select(value => new ConstantExpression(value));

    private static readonly Parser<Expression> PropertyExpression =
        PropertyPath.Select(path => new PropertyExpression(path));

    // Function calls like CONTAINS() and STARTSWITH()
    private static readonly Parser<Expression> FunctionCallExpression =
        from name in Identifier
        from lparen in Parse.Char('(').Token()
        from args in Parse.Ref(() => Expression).DelimitedBy(Parse.Char(',').Token())
        from rparen in Parse.Char(')').Token()
        select new FunctionCallExpression(name.ToUpperInvariant(), args.ToList());

    // Parenthesized expressions
    private static readonly Parser<Expression> ParenthesizedExpression =
        from lparen in Parse.Char('(').Token()
        from expr in Parse.Ref(() => Expression)
        from rparen in Parse.Char(')').Token()
        select expr;

    // Primary expressions
    private static readonly Parser<Expression> PrimaryExpression =
        LiteralExpression
        .Or(FunctionCallExpression)
        .Or(PropertyExpression)
        .Or(ParenthesizedExpression);

    // Binary operators with precedence
    private static readonly Parser<BinaryOperator> EqualityOperator =
        Keyword("=").Return(BinaryOperator.Equal)
        .Or(Keyword("!=").Return(BinaryOperator.NotEqual))
        .Or(Keyword("<>").Return(BinaryOperator.NotEqual));

    private static readonly Parser<BinaryOperator> ComparisonOperator =
        Keyword(">=").Return(BinaryOperator.GreaterThanOrEqual)
        .Or(Keyword(">").Return(BinaryOperator.GreaterThan))
        .Or(Keyword("<=").Return(BinaryOperator.LessThanOrEqual))
        .Or(Keyword("<").Return(BinaryOperator.LessThan));

    private static readonly Parser<BinaryOperator> LogicalOperator =
        Keyword("AND").Return(BinaryOperator.And)
        .Or(Keyword("OR").Return(BinaryOperator.Or));

    // Build expressions with operator precedence
    public static readonly Parser<Expression> Expression = Parse.ChainOperator(
        LogicalOperator,
        ParseComparisonExpression,
        (op, left, right) => new BinaryExpression(left, op, right));

    private static readonly Parser<Expression> ParseComparisonExpression = Parse.ChainOperator(
        EqualityOperator.Or(ComparisonOperator),
        PrimaryExpression,
        (op, left, right) => new BinaryExpression(left, op, right));

    // Parse SELECT clause
    private static readonly Parser<SelectItem> SelectAllItem =
        Parse.Char('*').Select(_ => (SelectItem)SelectAllItem.Instance);

    private static readonly Parser<SelectItem> PropertySelectItem =
        PropertyPath.Select(path => (SelectItem)new PropertySelectItem(path));

    private static readonly Parser<SelectItem> SelectItemParser =
        SelectAllItem.Or(PropertySelectItem);

    private static readonly Parser<SelectClause> SelectClauseParser =
        from select in Keyword("SELECT")
        from items in SelectItemParser.DelimitedBy(Parse.Char(',').Token())
        select new SelectClause(items.ToList());

    // Parse FROM clause
    private static readonly Parser<FromClause> FromClauseParser =
        from from in Keyword("FROM")
        from source in Identifier.Token()
        from alias in (
            from as_kw in Keyword("AS").Optional()
            from alias_id in Identifier.Token()
            select alias_id
        ).Optional()
        select new FromClause(source, alias.GetOrDefault());

    // Parse WHERE clause
    private static readonly Parser<WhereClause> WhereClauseParser =
        from where in Keyword("WHERE")
        from condition in Expression
        select new WhereClause(condition);

    // Parse ORDER BY clause
    private static readonly Parser<OrderByItem> OrderByItemParser =
        from prop in PropertyPath.Token()
        from direction in Keyword("DESC").Return(true).Or(Keyword("ASC").Return(false)).Optional()
        select new OrderByItem(prop, direction.GetOrDefault());

    private static readonly Parser<OrderByClause> OrderByClauseParser =
        from orderby in Keyword("ORDER").Then(_ => Keyword("BY"))
        from items in OrderByItemParser.DelimitedBy(Parse.Char(',').Token())
        select new OrderByClause(items.ToList());

    // Parse LIMIT clause (CosmosDB uses OFFSET/LIMIT)
    private static readonly Parser<LimitClause> LimitClauseParser =
        from limit in Keyword("LIMIT")
        from value in Parse.Number.Select(int.Parse)
        select new LimitClause(value);

    // Parse a complete CosmosDB SQL query
    public static readonly Parser<CosmosDbSqlQuery> QueryParser =
        from select in SelectClauseParser
        from from in FromClauseParser
        from where in WhereClauseParser.Optional()
        from orderBy in OrderByClauseParser.Optional()
        from limit in LimitClauseParser.Optional()
        select new CosmosDbSqlQuery(
            select,
            from,
            where.GetOrDefault(),
            orderBy.GetOrDefault(),
            limit.GetOrDefault());

    // Helper method to actually parse a query string
    public static CosmosDbSqlQuery ParseQuery(string query)
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
