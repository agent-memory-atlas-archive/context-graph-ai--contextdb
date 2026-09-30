//! The ISO SQL row-value constructor predicate: `(a, b) IN ((x, y), (z, w))`.
//!
//! A consumer that means a list of PAIRS has to be able to say so. Two
//! independent `IN` lists mean their cross product, which is correct SQL and
//! stays correct SQL — so the pair list needs its own standard spelling rather
//! than a guess about correlation between two lists. Postgres, MySQL and SQLite
//! all accept this one.

use contextdb_parser::ast::{Expr, Statement};
use contextdb_parser::parse;

fn where_clause(sql: &str) -> Expr {
    let Statement::Select(select) =
        parse(sql).unwrap_or_else(|error| panic!("SQL must parse: {sql}\nerror: {error:?}"))
    else {
        panic!("not a SELECT: {sql}");
    };
    select
        .body
        .where_clause
        .unwrap_or_else(|| panic!("statement has no WHERE: {sql}"))
}

#[test]
fn a_row_value_in_list_parses_as_one_term_per_tuple() {
    let expr = where_clause(
        "SELECT id FROM rows WHERE (context_id, generation_id) IN \
         (($c1, $g1), ($c2, $g2), ($c3, $g3))",
    );
    let Expr::InList {
        expr,
        list,
        negated,
    } = expr
    else {
        panic!("a row-value IN is an IN list: {expr:?}");
    };
    assert!(!negated);
    let Expr::Row(columns) = *expr else {
        panic!("the left of the IN is a row value");
    };
    assert_eq!(columns.len(), 2, "the row names two columns");
    assert_eq!(
        list.len(),
        3,
        "one term per tuple, not one per column value"
    );
    for tuple in &list {
        let Expr::Row(values) = tuple else {
            panic!("each element of the list is a row value: {tuple:?}");
        };
        assert_eq!(values.len(), 2, "each tuple carries two values");
    }
}

#[test]
fn a_negated_row_value_in_list_parses() {
    let expr = where_clause("SELECT id FROM rows WHERE (a, b) NOT IN ((1, 2), (3, 4))");
    let Expr::InList { negated, list, .. } = expr else {
        panic!("a negated row-value IN is still an IN list");
    };
    assert!(negated);
    assert_eq!(list.len(), 2);
}

#[test]
fn a_row_of_two_columns_against_a_row_of_three_values_is_refused() {
    let error = parse("SELECT id FROM rows WHERE (a, b) IN ((1, 2), (3, 4, 5))")
        .expect_err("mismatched arity is refused");
    let error = format!("{error:?}");
    assert!(
        error.contains("2 columns") && error.contains("3 values"),
        "the refusal names both arities: {error}"
    );
}

#[test]
fn a_row_against_single_values_is_refused() {
    let error = parse("SELECT id FROM rows WHERE (a, b) IN (1, 2)")
        .expect_err("a row compared against scalars is refused");
    assert!(
        format!("{error:?}").contains("single value"),
        "the refusal says the right-hand side is not a row"
    );
}

#[test]
fn a_single_value_against_rows_is_refused() {
    let error = parse("SELECT id FROM rows WHERE a IN ((1, 2), (3, 4))")
        .expect_err("a scalar compared against rows is refused");
    assert!(
        format!("{error:?}").contains("row value"),
        "the refusal says the left-hand side is not a row"
    );
}

#[test]
fn a_row_value_outside_in_is_refused() {
    parse("SELECT id FROM rows WHERE (a, b) = (1, 2)")
        .expect_err("a row value is only meaningful on either side of IN");
}

/// The shapes that already worked keep working, including the cross product of
/// two independent `IN` lists — which is what it has always meant.
#[test]
fn every_existing_in_shape_still_parses() {
    for sql in [
        "SELECT id FROM rows WHERE a IN (1, 2, 3)",
        "SELECT id FROM rows WHERE a NOT IN ('x', 'y')",
        "SELECT id FROM rows WHERE a IN ($p)",
        "SELECT id FROM rows WHERE a IN (SELECT id FROM other)",
        "SELECT id FROM rows WHERE a IN (1) AND b IN (2, 3)",
        "SELECT id FROM rows WHERE (a) IN (1, 2)",
    ] {
        parse(sql).unwrap_or_else(|error| panic!("SQL must still parse: {sql}\nerror: {error:?}"));
    }
}
