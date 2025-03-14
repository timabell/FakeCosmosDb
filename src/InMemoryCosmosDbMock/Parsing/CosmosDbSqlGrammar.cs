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
    
    private static Parser<T> Token<T>(this Parser<T> parser)
    {
        return parser.Then(item => 
            Spaces.Then(_ => Parse.Return(item)))
            .Preturn(Spaces);
    }

    // SQL keywords (case insensitive)
    private static Parser<string> Keyword(string word)
    {
        return Parse.IgnoreCase(word).Token();
    }

    // Identifiers for table and column names
    public static readonly Parser<string> Identifier =
        Parse.Letter.Once().Concat(Parse.LetterOrDigit.Or(Parse.Char('_')).Many())
            .Select(chars => new string(chars.ToArray()));

    // Property path (e.g., "c.Address.City")
    public static readonly Parser<string> PropertyPath =
        Identifier.DelimitedBy(Parse.Char('.')).Select(parts => string.Join(".", parts));

    // String literals
    public static readonly Parser<string> StringLiteral =
        Parse.Char('\'').Then(_ => 
            Parse.CharExcept('\'').Many().Text().Then(content => 
                Parse.Char('\'').Return(content)));

    // Numeric literals
    public static readonly Parser<double> NumberLiteral =
        Parse.Char('-').Optional().Then(sign => 
            Parse.Digit.AtLeastOnce().Text().Then(whole => 
                Parse.Char('.').Optional().Then(dot => 
                    (dot.IsDefined 
                        ? Parse.Digit.Many().Text() 
                        : Parse.Return("")).Select(fraction => {
                            string number = (sign.IsDefined ? "-" : "") + whole + (dot.IsDefined ? "." + fraction : "");
                            return double.Parse(number);
                        }))));

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

    // Expression parsers
    private static readonly Parser<Expression> ConstantExpr =
        Literal.Select(value => (Expression)new ConstantExpression(value));

    private static readonly Parser<Expression> PropertyExpr =
        PropertyPath.Token().Select(path => (Expression)new PropertyExpression(path));

    // Function call expressions (CONTAINS, STARTSWITH, etc.)
    private static Parser<Expression> FunctionCallExpr(string name)
    {
        return Parse.IgnoreCase(name).Token()
            .Then(_ => Parse.Char('(').Token())
            .Then(_ => Parse.Ref(() => ExpressionParser).DelimitedBy(Parse.Char(',').Token()))
            .Then(args => Parse.Char(')').Token().Return((Expression)new FunctionCallExpression(name, args.ToList())));
    }

    // Parsers for function expressions
    private static readonly Parser<Expression> FunctionExpr =
        FunctionCallExpr("CONTAINS")
        .Or(FunctionCallExpr("STARTSWITH"));

    // Binary operators
    private static readonly Parser<BinaryOperator> ComparisonOperator =
        Parse.String("=").Token().Return(BinaryOperator.Equal)
        .Or(Parse.String("!=").Token().Return(BinaryOperator.NotEqual))
        .Or(Parse.String("<>").Token().Return(BinaryOperator.NotEqual))
        .Or(Parse.String("<=").Token().Return(BinaryOperator.LessThanOrEqual))
        .Or(Parse.String(">=").Token().Return(BinaryOperator.GreaterThanOrEqual))
        .Or(Parse.String("<").Token().Return(BinaryOperator.LessThan))
        .Or(Parse.String(">").Token().Return(BinaryOperator.GreaterThan));

    private static readonly Parser<BinaryOperator> AndOperator =
        Keyword("AND").Return(BinaryOperator.And);

    private static readonly Parser<BinaryOperator> OrOperator =
        Keyword("OR").Return(BinaryOperator.Or);

    // Forward reference for expression parser (to handle recursion)
    private static readonly Parser<Expression> ExpressionRef = Parse.Ref(() => ExpressionParser);

    // Atom expressions (constants, property refs, parenthesized expressions)
    private static readonly Parser<Expression> AtomExpr =
        ConstantExpr
        .Or(FunctionExpr)
        .Or(PropertyExpr)
        .Or(Parse.Char('(').Token()
            .Then(_ => ExpressionRef)
            .Then(expr => Parse.Char(')').Token().Return(expr)));

    // Binary expression with operator precedence
    private static Parser<Expression> Binary(Parser<Expression> operand, Parser<BinaryOperator> op)
    {
        return operand.Then(first => 
            op.Then(operator1 => operand).Many().Select(rest => {
                Expression result = first;
                foreach (var pair in rest)
                {
                    result = new BinaryExpression(result, pair.Item1, pair.Item2);
                }
                return result;
            }));
    }

    // Main expression parser with operator precedence
    private static readonly Parser<Expression> ComparisonExpr =
        AtomExpr.Then(left => 
            ComparisonOperator.Then(op => 
                AtomExpr.Select(right => 
                    (Expression)new BinaryExpression(left, op, right))));

    private static readonly Parser<Expression> SimpleExpr =
        ComparisonExpr.Or(AtomExpr);

    private static readonly Parser<Expression> AndExpr =
        Binary(SimpleExpr, AndOperator);

    private static readonly Parser<Expression> OrExpr =
        Binary(AndExpr.Or(SimpleExpr), OrOperator);

    // The main expression parser with precedence: OR > AND > Comparison > Atom
    private static readonly Parser<Expression> ExpressionParser =
        OrExpr.Or(AndExpr).Or(SimpleExpr);

    // Helper extension methods for Optional results
    private static T GetOrDefault<T>(this IOption<T> option, T defaultValue = default)
    {
        return option.IsDefined ? option.Get() : defaultValue;
    }

    // SELECT clause parsing
    private static readonly Parser<SelectClause> SelectClauseParser =
        Keyword("SELECT").Then(_ => 
            Parse.Char('*').Token().Select(_ => (IReadOnlyList<SelectItem>)new List<SelectItem> { new SelectAllItem() })
            .Or(PropertyPath.Token().DelimitedBy(Parse.Char(',').Token())
                .Select(paths => (IReadOnlyList<SelectItem>)paths.Select(p => (SelectItem)new PropertySelectItem(p)).ToList()))
            .Select(items => new SelectClause(items)));

    // FROM clause parsing
    private static readonly Parser<FromClause> FromClauseParser =
        Keyword("FROM").Then(_ => 
            Identifier.Token().Then(source => 
                Keyword("AS").Optional().Then(_ => 
                    Identifier.Token().Optional().Select(alias => 
                        new FromClause(source, alias.GetOrDefault())))));

    // WHERE clause parsing
    private static readonly Parser<WhereClause> WhereClauseParser =
        Keyword("WHERE").Then(_ => 
            ExpressionParser.Select(condition => 
                new WhereClause(condition)));

    // ORDER BY clause parsing
    private static readonly Parser<OrderByItem> OrderByItemParser =
        PropertyPath.Token().Then(path => 
            Keyword("DESC").Return(true).Or(Keyword("ASC").Return(false)).Optional()
                .Select(direction => new OrderByItem(path, direction.GetOrDefault(false))));

    private static readonly Parser<OrderByClause> OrderByClauseParser =
        Keyword("ORDER").Then(_ => 
            Keyword("BY").Then(_ => 
                OrderByItemParser.DelimitedBy(Parse.Char(',').Token())
                    .Select(items => new OrderByClause(items.ToList()))));

    // LIMIT clause parsing
    private static readonly Parser<LimitClause> LimitClauseParser =
        Keyword("LIMIT").Then(_ => 
            Parse.Number.Select(value => 
                new LimitClause(int.Parse(value))));

    // Complete SQL query parsing
    private static readonly Parser<CosmosDbSqlQuery> QueryParser =
        SelectClauseParser.Then(select => 
            FromClauseParser.Then(from => 
                WhereClauseParser.Optional().Then(where => 
                    OrderByClauseParser.Optional().Then(orderBy => 
                        LimitClauseParser.Optional().Select(limit => 
                            new CosmosDbSqlQuery(
                                select, 
                                from, 
                                where.GetOrDefault(), 
                                orderBy.GetOrDefault(), 
                                limit.GetOrDefault()
                            ))))));

    /// <summary>
    /// Parses a CosmosDB SQL query string into an AST.
    /// </summary>
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
