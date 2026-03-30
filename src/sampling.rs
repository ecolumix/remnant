use anyhow::Result;
use polars::prelude::*;

/// Sample a percentage of rows from a DataFrame.
///
/// `percent` is expressed as a human-readable value (e.g. 10.0 means 10%).
pub fn sample_df(df: &DataFrame, percent: f64, seed: Option<u64>) -> Result<DataFrame> {
    let frac = percent / 100.0;
    let n = ((frac * df.shape().0 as f64).floor()) as usize;
    let sampled = df.sample_n_literal(n, false, false, seed)?;
    Ok(sampled)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_df(n_rows: usize) -> DataFrame {
        let ids: Vec<i32> = (0..n_rows as i32).collect();
        DataFrame::new(vec![Series::new("id", &ids)]).unwrap()
    }

    #[test]
    fn test_sample_10_percent() {
        let df = make_test_df(100);
        let sampled = sample_df(&df, 10.0, Some(42)).unwrap();
        assert_eq!(sampled.shape().0, 10);
    }

    #[test]
    fn test_sample_50_percent() {
        let df = make_test_df(200);
        let sampled = sample_df(&df, 50.0, Some(42)).unwrap();
        assert_eq!(sampled.shape().0, 100);
    }

    #[test]
    fn test_sample_0_percent() {
        let df = make_test_df(100);
        let sampled = sample_df(&df, 0.0, Some(42)).unwrap();
        assert_eq!(sampled.shape().0, 0);
    }

    #[test]
    fn test_sample_100_percent() {
        let df = make_test_df(50);
        let sampled = sample_df(&df, 100.0, Some(42)).unwrap();
        assert_eq!(sampled.shape().0, 50);
    }

    #[test]
    fn test_sample_deterministic_with_seed() {
        let df = make_test_df(100);
        let a = sample_df(&df, 20.0, Some(123)).unwrap();
        let b = sample_df(&df, 20.0, Some(123)).unwrap();
        assert_eq!(a.frame_equal(&b), true);
    }

    #[test]
    fn test_sample_empty_df() {
        let df = make_test_df(0);
        let sampled = sample_df(&df, 10.0, Some(42)).unwrap();
        assert_eq!(sampled.shape().0, 0);
    }
}
