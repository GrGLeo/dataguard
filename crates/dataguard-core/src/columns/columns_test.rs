#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_column_builder() {
        let mut builder = StringColumnBuilder::new("name".to_string());
        builder.with_min_length(3).with_max_length(50);

        assert_eq!(builder.name(), "name");
        assert_eq!(builder.column_type(), ColumnType::String);
        assert_eq!(builder.rules().len(), 2);
    }

    #[test]
    fn test_string_column_with_regex() {
        let mut builder = StringColumnBuilder::new("email".to_string());
        builder.is_email().unwrap();

        assert_eq!(builder.column_type(), ColumnType::String);
        assert_eq!(builder.rules().len(), 1);
        match &builder.rules()[0] {
            ColumnRule::StringRegex { pattern, .. } => {
                assert!(pattern.contains("@"));
            }
            _ => panic!("Expected StringRegex rule"),
        }
    }

    #[test]
    fn test_string_column_invalid_regex() {
        let mut builder = StringColumnBuilder::new("test".to_string());
        let result = builder.with_regex("[invalid(".to_string(), None);

        assert!(result.is_err());
    }

    #[test]
    fn test_integer_column_builder() {
        let mut builder = NumericColumnBuilder::<i64>::new("age".to_string());
        builder.between(0, 120);

        assert_eq!(builder.name(), "age");
        assert_eq!(builder.column_type(), ColumnType::Integer);
        assert_eq!(builder.rules().len(), 1);
    }

    #[test]
    fn test_integer_column_is_positive() {
        let mut builder = NumericColumnBuilder::<i64>::new("count".to_string());
        builder.is_positive();

        match &builder.rules()[0] {
            ColumnRule::NumericRange { min, max } => {
                assert_eq!(min, &Some(1.0));
                assert_eq!(max, &None);
            }
            _ => panic!("Expected NumericRange rule"),
        }
    }

    #[test]
    fn test_float_column_builder() {
        let mut builder = NumericColumnBuilder::<f64>::new("price".to_string());
        builder.between(0.0, 1000.0);

        assert_eq!(builder.name(), "price");
        assert_eq!(builder.column_type(), ColumnType::Float);
        assert_eq!(builder.rules().len(), 1);
    }

    #[test]
    fn test_float_column_monotonicity() {
        let mut builder = NumericColumnBuilder::<f64>::new("timestamp".to_string());
        builder.is_monotonically_increasing();

        match &builder.rules()[0] {
            ColumnRule::Monotonicity { ascending } => {
                assert!(ascending);
            }
            _ => panic!("Expected Monotonicity rule"),
        }
    }

    #[test]
    fn test_column_chaining() {
        let mut builder = StringColumnBuilder::new("username".to_string());
        builder
            .with_min_length(3)
            .with_max_length(20)
            .is_alphanumeric()
            .unwrap()
            .is_unique();

        assert_eq!(builder.rules().len(), 4);
    }
}
