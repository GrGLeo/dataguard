use arrow_array::{Date32Array, StringArray};
use chrono::NaiveDate;

pub fn parse_date_column(array: &StringArray, format: &str) -> Date32Array {
    array
        .iter()
        .map(|opt_str| opt_str.and_then(|str_date| parse_date(str_date, format)))
        .collect()
}

fn parse_date(str_date: &str, format: &str) -> Option<i32> {
    NaiveDate::parse_from_str(str_date, format)
        .ok()
        .map(|date| {
            // We can safely unwrap as 1970-01-01 is a valid existing date
            date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                .num_days() as i32
        })
}
