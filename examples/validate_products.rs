use dataguard_core::{CsvTable, NumericColumnBuilder, StringColumnBuilder, Table};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("DataGuard Example - Product Validation\n");

    // Index: integer with uniqueness, not null, and positive checks
    let mut index_column = NumericColumnBuilder::<i64>::new("Index".to_string());
    index_column
        .is_unique(0.0)
        .is_not_null(0.0)
        .is_positive(0.0);

    // Name: string with min/max length and not null
    let mut name_column = StringColumnBuilder::new("Name".to_string());
    name_column
        .is_not_null(0.0)
        .with_min_length(3, 0.0)
        .with_max_length(100, 0.0);

    // Description: string with min length
    let mut description_column = StringColumnBuilder::new("Description".to_string());
    description_column.with_min_length(1, 0.0);

    // Currency: string with allowed values
    let mut currency_column = StringColumnBuilder::new("Currency".to_string());
    currency_column.is_in(
        vec![
            "USD".to_string(),
            "EUR".to_string(),
            "GBP".to_string(),
            "JPY".to_string(),
            "CAD".to_string(),
        ],
        0.0,
    );

    // Price: integer with range and positive checks
    let mut price_column = NumericColumnBuilder::<i64>::new("Price".to_string());
    price_column.between(1, 10000, 0.0).is_positive(0.0);

    // Stock: integer with non-negative check
    let mut stock_column = NumericColumnBuilder::<i64>::new("Stock".to_string());
    stock_column.is_non_negative(0.0);

    // EAN: string with exact length and numeric validation
    let mut ean_column = StringColumnBuilder::new("EAN".to_string());
    ean_column.is_exact_length(13, 0.0);
    ean_column.is_numeric(0.0)?;

    // Availability: string with enumerated values
    let mut availability_column = StringColumnBuilder::new("Availability".to_string());
    availability_column.is_in(
        vec![
            "in_stock".to_string(),
            "out_of_stock".to_string(),
            "limited_stock".to_string(),
            "backorder".to_string(),
            "pre_order".to_string(),
            "discontinued".to_string(),
        ],
        0.0,
    );

    // Internal ID: integer with positive check
    let mut internal_id_column = NumericColumnBuilder::<i64>::new("Internal ID".to_string());
    internal_id_column.is_positive(0.0);

    let mut table = CsvTable::new(
        "examples/data/products.csv".to_string(),
        "Products Large Dataset".to_string(),
    )?;

    table.prepare(
        vec![
            Box::new(index_column),
            Box::new(name_column),
            Box::new(description_column),
            Box::new(currency_column),
            Box::new(price_column),
            Box::new(stock_column),
            Box::new(ean_column),
            Box::new(availability_column),
            Box::new(internal_id_column),
        ],
        vec![],
    )?;

    let result = table.validate()?;

    println!("Validation Results:");
    println!("  Table: {}", result.table_name);
    println!("  Total rows: {}", result.total_rows);

    let (passed, total) = result.is_passed();
    println!("  Rules: {}/{} passed", passed, total);

    if passed == total {
        println!("\n✓ All validation rules passed!");
    } else {
        println!("\n✗ Some validation rules failed.");
        println!("  Check the detailed report above for specific errors.");
    }

    Ok(())
}
