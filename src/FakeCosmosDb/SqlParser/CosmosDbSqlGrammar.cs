using System;
using System.Collections.Generic;
using System.Linq;
using Sprache;

namespace TimAbell.FakeCosmosDb.SqlParser;

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
		return Spaces.Then(_ => parser).Then(item => Spaces.Return(item));
	}

	// SQL keywords (case insensitive)
	private static Parser<string> Keyword(string word)
	{
		return Parse.IgnoreCase(word).Text().Token();
	}

	// Identifiers for table and column names
	private static readonly Parser<string> Identifier =
		Parse.Letter.AtLeastOnce().Text().Then(first =>
			Parse.LetterOrDigit.Or(Parse.Char('_')).Many().Text().Select(rest =>
				first + rest
			)
		);

	// Parameter identifier (e.g. @param)
	private static readonly Parser<string> ParameterIdentifier =
		Parse.Char('@').Then(_ =>
			Parse.Letter.AtLeastOnce().Text().Then(first =>
				Parse.LetterOrDigit.Or(Parse.Char('_')).Many().Text().Select(rest =>
					"@" + first + rest
				)
			)
		);

	// Property path (e.g., "c.Address.City")
	private static readonly Parser<string> PropertyPath =
		Identifier.DelimitedBy(Parse.Char('.')).Select(parts => string.Join(".", parts));

	// String literals
	private static readonly Parser<string> StringLiteral =
		Parse.Char('\'').Then(_ =>
			Parse.CharExcept('\'').Many().Text().Then(content =>
				Parse.Char('\'').Return(content)));

	// Numeric literals
	private static readonly Parser<double> NumberLiteral =
		Parse.Char('-').Optional().Then(sign =>
			Parse.Digit.AtLeastOnce().Text().Then(whole =>
				Parse.Char('.').Optional().Then(dot =>
					(dot.IsDefined
						? Parse.Digit.Many().Text()
						: Parse.Return("")).Select(fraction =>
						{
							string number = (sign.IsDefined ? "-" : "") + whole + (dot.IsDefined ? "." + fraction : "");
							return double.Parse(number);
						}))));

	// Boolean literals
	private static readonly Parser<bool> BooleanLiteral =
		Keyword("true").Return(true)
		.Or(Keyword("false").Return(false));

	// Null literal
	private static readonly Parser<object> NullLiteral =
		Keyword("null").Return((object)null);

	// Any literal value
	private static readonly Parser<object> Literal =
		StringLiteral.Select(s => (object)s)
		.Or(NumberLiteral.Select(n => (object)n))
		.Or(BooleanLiteral.Select(b => (object)b))
		.Or(NullLiteral);

	// Expression parsers
	private static readonly Parser<Expression> ConstantExpr =
		Literal.Select(value => (Expression)new ConstantExpression(value));

	private static readonly Parser<Expression> PropertyExpr =
		PropertyPath.Token().Select(path => (Expression)new PropertyExpression(path));

	// Parameter expression (@param)
	private static readonly Parser<Expression> ParameterExpr =
		ParameterIdentifier.Token().Select(paramName => (Expression)new ParameterExpression(paramName.Substring(1))); // Strip the @ symbol

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
		.Or(FunctionCallExpr("STARTSWITH"))
		.Or(FunctionCallExpr("IS_NULL"))
		.Or(FunctionCallExpr("IS_DEFINED"))
		.Or(FunctionCallExpr("ARRAY_CONTAINS"));

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

	// Forward reference for term expression parser to break circular dependency
	private static readonly Parser<Expression> TermExprRef = Parse.Ref(() => TermExpr);

	// BETWEEN operator parser - using the forward reference to avoid circular dependency
	private static readonly Parser<Expression> BetweenExpr =
		TermExprRef.Then(left =>
			Keyword("BETWEEN").Then(_ =>
				TermExprRef.Then(lower =>
					Keyword("AND").Then(_ =>
						TermExprRef.Select(upper =>
							(Expression)new BinaryExpression(
								left,
								BinaryOperator.Between,
								new BetweenExpression(lower, upper)
							)
						)
					)
				)
			)
		);

	// NOT operator
	private static readonly Parser<Expression> NotExpr =
		Keyword("NOT").Token().Then(_ =>
			Parse.Ref(() => AtomExpr)
				.Select(expr => (Expression)new UnaryExpression(UnaryOperator.Not, expr)));

	// Atom expressions (constants, property refs, parameters, parenthesized expressions)
	private static readonly Parser<Expression> AtomExpr =
		ConstantExpr
		.Or(FunctionExpr)
		.Or(ParameterExpr)
		.Or(PropertyExpr)
		.Or(Parse.Char('(').Token()
			.Then(_ => ExpressionRef)
			.Then(expr => Parse.Char(')').Token().Return(expr)));

	// Expression types in order of precedence (lowest to highest)
	private static readonly Parser<Expression> TermExpr =
		NotExpr
		.Or(AtomExpr);

	// Binary expression with operator precedence
	private static Parser<Expression> Binary(Parser<Expression> operand, Parser<BinaryOperator> op)
	{
		return operand.Then(first =>
			op.Then(operator1 =>
				operand.Select(operand1 =>
					new Tuple<BinaryOperator, Expression>(operator1, operand1)
				)
			).Many().Select(rest =>
			{
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
		BetweenExpr
		.Or(TermExpr.Then(left =>
			ComparisonOperator.Then(op =>
				TermExpr.Select(right =>
					(Expression)new BinaryExpression(left, op, right)))));

	private static readonly Parser<Expression> SimpleExpr =
		ComparisonExpr.Or(TermExpr);

	private static readonly Parser<Expression> AndExpr =
		Binary(SimpleExpr, AndOperator);

	private static readonly Parser<Expression> OrExpr =
		Binary(AndExpr.Or(SimpleExpr), OrOperator);

	// The main expression parser with precedence: OR > AND > Comparison > Atom
	private static readonly Parser<Expression> ExpressionParser =
		OrExpr.Or(AndExpr).Or(SimpleExpr);

	// Define reserved keywords
	private static readonly Parser<string> ReservedKeyword =
		Keyword("SELECT").Or(Keyword("FROM")).Or(Keyword("WHERE"))
			.Or(Keyword("ORDER")).Or(Keyword("BY")).Or(Keyword("LIMIT"))
			.Or(Keyword("ASC")).Or(Keyword("DESC")).Or(Keyword("AND")).Or(Keyword("OR"))
			.Or(Keyword("TOP"));

	// Helper extension methods for Optional results
	private static T GetOrDefault<T>(this IOption<T> option, T defaultValue = default)
	{
		return option.IsDefined ? option.Get() : defaultValue;
	}

	// TOP clause parsing
	private static readonly Parser<TopClause> TopClauseParser =
		Keyword("TOP").Token().Then(_ =>
			Parse.Number.Token().Select(value =>
				new TopClause(int.Parse(value))));

	// SELECT clause parsing
	private static readonly Parser<SelectClause> SelectClauseParser =
		Keyword("SELECT").Then(_ =>
			TopClauseParser.Optional().Then(top =>
				Parse.Char('*').Token().Select(_ => (IReadOnlyList<SelectItem>)new List<SelectItem> { new SelectAllItem() })
				.Or(PropertyPath.Token().DelimitedBy(Parse.Char(',').Token())
					.Select(paths => (IReadOnlyList<SelectItem>)paths.Select(p => (SelectItem)new PropertySelectItem(p)).ToList()))
				.Select(items => new SelectClause(items, top.GetOrDefault()))));

	// FROM clause parsing
	private static readonly Parser<FromClause> FromClauseParser =
		Keyword("FROM").Then(_ =>
			Identifier.Token().Then(source =>
			{
				// Handle explicit AS keyword followed by an identifier
				var withAsKeyword = Keyword("AS").Token().Then(_ => Identifier.Token());

				// If alias is not present, don't consume anything
				var aliasParser = withAsKeyword.Optional();

				return aliasParser.Select(alias => new FromClause(source, alias.GetOrDefault()));
			}));

	// WHERE clause parsing
	private static readonly Parser<WhereClause> WhereClauseParser =
		Keyword("WHERE").Then(_ =>
			ExpressionParser.Select(condition =>
				new WhereClause(condition)));

	// ORDER BY clause parsing
	private static readonly Parser<OrderByItem> OrderByItemParser =
		PropertyPath.Token().Then(path =>
			Keyword("DESC").Token().Return(true)
				.Or(Keyword("ASC").Token().Return(false))
				.Optional()
				.Select(direction => new OrderByItem(path, direction.GetOrDefault(false))));

	private static readonly Parser<OrderByClause> OrderByClauseParser =
		Keyword("ORDER").Token().Then(_ =>
			Keyword("BY").Token().Then(_ =>
				OrderByItemParser.DelimitedBy(Parse.Char(',').Token())
					.Where(items => items.Count() > 0)
					.Select(items => new OrderByClause(items.ToList()))));

	// LIMIT clause parsing
	private static readonly Parser<LimitClause> LimitClauseParser =
		Keyword("LIMIT").Token().Then(_ =>
			Parse.Number.Token().Select(value =>
				new LimitClause(int.Parse(value))));

	// Main query parser, combining all clauses
	private static readonly Parser<CosmosDbSqlQuery> QueryParser =
		SelectClauseParser.Token()
			.Then(select => FromClauseParser.Token()
				.Then(from => WhereClauseParser.Token().Optional()
					.Then(where => OrderByClauseParser.Token().Optional()
						.Then(orderBy => LimitClauseParser.Token().Optional()
							.Select(limit => new CosmosDbSqlQuery(
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
			// Remove the .End() call to allow the parser to handle the entire query
			// and trim whitespace to avoid issues with extraneous spaces
			return QueryParser.Parse(query.Trim());
		}
		catch (ParseException ex)
		{
			throw new FormatException($"Failed to parse CosmosDB SQL query: {ex.Message}", ex);
		}
	}
}
