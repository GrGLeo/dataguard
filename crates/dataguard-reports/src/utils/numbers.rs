pub fn format_numbers(n: usize) -> String {
    match n {
        n if n > 1_000_000_000 => format!("{:0.1}B", n as f64 / 1_000_000_000.0),
        n if n > 1_000_000 => format!("{:0.1}M", n as f64 / 1_000_000.0),
        n if n > 1_000 => format!("{:0.1}K", n as f64 / 1_000.0),
        _ => n.to_string(),
    }
}

#[cfg(test)]
mod test {
    use crate::utils::numbers::format_numbers;

    #[test]
    fn test_format_b() {
        let n = 2_736_123_123usize;
        let s = format_numbers(n);
        assert_eq!(s, "2.7B".to_string())
    }

    #[test]
    fn test_format_m() {
        let n = 2_336_123usize;
        let s = format_numbers(n);
        assert_eq!(s, "2.3M".to_string())
    }

    #[test]
    fn test_format_k() {
        let n = 4_536usize;
        let s = format_numbers(n);
        assert_eq!(s, "4.5K".to_string())
    }

    #[test]
    fn test_format() {
        let n = 789usize;
        let s = format_numbers(n);
        assert_eq!(s, "789".to_string())
    }
}
