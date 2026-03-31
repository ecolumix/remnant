//! Core sampling logic for DataFrames.
//!
//! This module provides [`sample_df`], which randomly samples a percentage of
//! rows from a Polars [`DataFrame`]. Both the [`csv`](crate::csv) and
//! [`pg`](crate::pg) modules delegate to this function for their sampling step.

use anyhow::Result;
use polars::prelude::*;

/// Sample a percentage of rows from a DataFrame.
///
/// Sampling is performed without replacement.
///
/// # Arguments
///
/// * `df` — The input DataFrame.
/// * `percent` — Percentage of rows to retain, expressed as a human-readable
///   value (e.g. `10.0` means 10%).
/// * `seed` — Optional random seed for deterministic output.
///
/// # Examples
///
/// ```
/// use polars::prelude::*;
/// use remnant::sampling::sample_df;
///
/// let df = DataFrame::new(vec![
///     Series::new("x", &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
/// ]).unwrap();
/// let sampled = sample_df(&df, 50.0, Some(42)).unwrap();
/// assert_eq!(sampled.shape().0, 5);
/// ```
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
