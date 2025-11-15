# ABOUTME: Machine learning feature engineering for time-series metrics data
# ABOUTME: Extracts rolling statistics, seasonality patterns, derivatives, and FFT features for anomaly detection

"""
Feature Engineering for LogicMonitor Time-Series Data

Transforms raw time-series metrics into feature vectors suitable for machine learning models.

Features extracted:
- Rolling statistics (mean, std, percentiles) over multiple windows
- Seasonality patterns (hourly, daily, weekly)
- Rate of change and derivatives
- Cyclical features using Fast Fourier Transform (FFT)
- Aggregated feature vectors for each metric

All functions are pure (no side effects) and return structured data.
"""

import numpy as np
import pandas as pd
from scipy import stats, fft, signal
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta


@dataclass
class TimeSeriesData:
    """Raw time-series data for feature engineering."""
    timestamps: np.ndarray  # Unix timestamps
    values: np.ndarray  # Metric values
    metric_name: str
    resource_hash: str
    unit: Optional[str] = None


@dataclass
class RollingStatistics:
    """Rolling statistics over different time windows."""
    window_size: str  # e.g., "1h", "6h", "24h"
    mean: np.ndarray
    std: np.ndarray
    min: np.ndarray
    max: np.ndarray
    median: np.ndarray
    p25: np.ndarray  # 25th percentile
    p75: np.ndarray  # 75th percentile
    p95: np.ndarray  # 95th percentile
    p99: np.ndarray  # 99th percentile


@dataclass
class SeasonalityFeatures:
    """Seasonality pattern detection."""
    hourly_pattern: np.ndarray  # 24 values (one per hour)
    daily_pattern: np.ndarray  # 7 values (one per day of week)
    weekly_pattern: np.ndarray  # 4 values (one per week of month)
    has_hourly_seasonality: bool
    has_daily_seasonality: bool
    has_weekly_seasonality: bool
    seasonality_strength: float  # 0-1, strength of detected patterns


@dataclass
class DerivativeFeatures:
    """Rate of change and derivative features."""
    first_derivative: np.ndarray  # Rate of change
    second_derivative: np.ndarray  # Acceleration
    velocity_mean: float
    velocity_std: float
    acceleration_mean: float
    acceleration_std: float


@dataclass
class FFTFeatures:
    """Frequency domain features from Fast Fourier Transform."""
    dominant_frequencies: np.ndarray  # Top 5 frequencies
    dominant_periods: np.ndarray  # Corresponding periods in seconds
    spectral_entropy: float  # Measure of signal complexity
    spectral_energy: float  # Total energy in frequency domain
    low_freq_energy: float  # Energy in low frequencies
    high_freq_energy: float  # Energy in high frequencies


@dataclass
class FeatureVector:
    """Complete feature vector for a metric."""
    metric_name: str
    resource_hash: str
    timestamp: datetime

    # Raw statistics
    current_value: float
    value_mean: float
    value_std: float
    value_min: float
    value_max: float

    # Rolling statistics (latest values from each window)
    rolling_1h_mean: float
    rolling_1h_std: float
    rolling_6h_mean: float
    rolling_6h_std: float
    rolling_24h_mean: float
    rolling_24h_std: float

    # Seasonality indicators
    hour_of_day: int
    day_of_week: int
    is_weekend: bool
    hourly_seasonal_score: float
    daily_seasonal_score: float

    # Derivatives
    rate_of_change: float
    acceleration: float
    velocity_zscore: float  # Z-score of current velocity

    # FFT features
    dominant_period_seconds: float
    spectral_entropy: float
    low_freq_ratio: float  # low_freq_energy / total_energy

    # Anomaly indicators (computed from features)
    zscore: float  # Z-score of current value
    is_outlier: bool  # Based on IQR method
    deviation_from_trend: float


def create_dataframe(ts_data: TimeSeriesData) -> pd.DataFrame:
    """
    Convert TimeSeriesData to pandas DataFrame for easier manipulation.

    Args:
        ts_data: Raw time-series data

    Returns:
        DataFrame with timestamp index and value column
    """
    df = pd.DataFrame({
        'timestamp': pd.to_datetime(ts_data.timestamps, unit='s'),
        'value': ts_data.values
    })
    df = df.set_index('timestamp').sort_index()
    return df


def calculate_rolling_statistics(
    ts_data: TimeSeriesData,
    windows: List[str] = ['1h', '6h', '24h']
) -> List[RollingStatistics]:
    """
    Calculate rolling statistics over multiple time windows.

    Args:
        ts_data: Raw time-series data
        windows: List of window sizes (pandas time strings)

    Returns:
        List of RollingStatistics for each window
    """
    df = create_dataframe(ts_data)
    results = []

    for window in windows:
        rolling = df['value'].rolling(window=window, min_periods=1)

        stats = RollingStatistics(
            window_size=window,
            mean=rolling.mean().values,
            std=rolling.std().values,
            min=rolling.min().values,
            max=rolling.max().values,
            median=rolling.median().values,
            p25=rolling.quantile(0.25).values,
            p75=rolling.quantile(0.75).values,
            p95=rolling.quantile(0.95).values,
            p99=rolling.quantile(0.99).values
        )
        results.append(stats)

    return results


def detect_seasonality(ts_data: TimeSeriesData) -> SeasonalityFeatures:
    """
    Detect seasonality patterns in time-series data.

    Uses autocorrelation to detect hourly, daily, and weekly patterns.

    Args:
        ts_data: Raw time-series data

    Returns:
        SeasonalityFeatures with detected patterns
    """
    df = create_dataframe(ts_data)

    # Extract time-based features
    df['hour'] = df.index.hour
    df['day_of_week'] = df.index.dayofweek
    df['week_of_month'] = (df.index.day - 1) // 7

    # Calculate hourly pattern (average value per hour)
    hourly_pattern = df.groupby('hour')['value'].mean().values
    if len(hourly_pattern) < 24:
        hourly_pattern = np.pad(hourly_pattern, (0, 24 - len(hourly_pattern)),
                               mode='constant', constant_values=np.nan)

    # Calculate daily pattern (average value per day of week)
    daily_pattern = df.groupby('day_of_week')['value'].mean().values
    if len(daily_pattern) < 7:
        daily_pattern = np.pad(daily_pattern, (0, 7 - len(daily_pattern)),
                              mode='constant', constant_values=np.nan)

    # Calculate weekly pattern (average value per week of month)
    weekly_pattern = df.groupby('week_of_month')['value'].mean().values
    if len(weekly_pattern) < 4:
        weekly_pattern = np.pad(weekly_pattern, (0, 4 - len(weekly_pattern)),
                               mode='constant', constant_values=np.nan)

    # Detect seasonality using autocorrelation
    # Hourly: lag of 24 (for hourly data) or 1440 (for minute data)
    # Daily: lag of 7
    # Weekly: lag of 4

    values = df['value'].values
    if len(values) > 30:  # Need sufficient data
        # Calculate autocorrelation at different lags
        hourly_acf = _safe_autocorr(values, lag=min(24, len(values) // 2))
        daily_acf = _safe_autocorr(values, lag=min(7, len(values) // 2))
        weekly_acf = _safe_autocorr(values, lag=min(4, len(values) // 2))

        # Threshold for detecting seasonality
        threshold = 0.3
        has_hourly = abs(hourly_acf) > threshold
        has_daily = abs(daily_acf) > threshold
        has_weekly = abs(weekly_acf) > threshold

        # Overall seasonality strength
        seasonality_strength = (abs(hourly_acf) + abs(daily_acf) + abs(weekly_acf)) / 3
    else:
        has_hourly = has_daily = has_weekly = False
        seasonality_strength = 0.0

    return SeasonalityFeatures(
        hourly_pattern=hourly_pattern,
        daily_pattern=daily_pattern,
        weekly_pattern=weekly_pattern,
        has_hourly_seasonality=has_hourly,
        has_daily_seasonality=has_daily,
        has_weekly_seasonality=has_weekly,
        seasonality_strength=seasonality_strength
    )


def _safe_autocorr(values: np.ndarray, lag: int) -> float:
    """Calculate autocorrelation safely, handling edge cases."""
    if len(values) <= lag or lag <= 0:
        return 0.0

    try:
        # Use pandas for autocorrelation
        series = pd.Series(values)
        acf = series.autocorr(lag=lag)
        return acf if not np.isnan(acf) else 0.0
    except:
        return 0.0


def compute_derivatives(ts_data: TimeSeriesData) -> DerivativeFeatures:
    """
    Compute rate of change (first derivative) and acceleration (second derivative).

    Args:
        ts_data: Raw time-series data

    Returns:
        DerivativeFeatures with velocity and acceleration metrics
    """
    df = create_dataframe(ts_data)

    # Calculate time differences in seconds
    time_diff = df.index.to_series().diff().dt.total_seconds().values
    time_diff[0] = time_diff[1] if len(time_diff) > 1 else 1.0

    # First derivative (velocity) - rate of change
    value_diff = np.diff(df['value'].values, prepend=df['value'].values[0])
    first_derivative = value_diff / time_diff

    # Second derivative (acceleration) - rate of change of rate of change
    velocity_diff = np.diff(first_derivative, prepend=first_derivative[0])
    time_diff_2nd = time_diff[1:] if len(time_diff) > 1 else time_diff
    if len(time_diff_2nd) < len(velocity_diff):
        time_diff_2nd = np.append(time_diff_2nd, time_diff_2nd[-1])
    second_derivative = velocity_diff / time_diff_2nd

    # Calculate statistics
    velocity_mean = np.mean(first_derivative)
    velocity_std = np.std(first_derivative)
    acceleration_mean = np.mean(second_derivative)
    acceleration_std = np.std(second_derivative)

    return DerivativeFeatures(
        first_derivative=first_derivative,
        second_derivative=second_derivative,
        velocity_mean=velocity_mean,
        velocity_std=velocity_std,
        acceleration_mean=acceleration_mean,
        acceleration_std=acceleration_std
    )


def extract_fft_features(ts_data: TimeSeriesData, top_n: int = 5) -> FFTFeatures:
    """
    Extract frequency domain features using Fast Fourier Transform.

    Args:
        ts_data: Raw time-series data
        top_n: Number of dominant frequencies to extract

    Returns:
        FFTFeatures with frequency domain characteristics
    """
    values = ts_data.values

    # Remove mean (detrend)
    values_detrended = values - np.mean(values)

    # Apply FFT
    fft_values = fft.fft(values_detrended)
    fft_freq = fft.fftfreq(len(values))

    # Get power spectrum (magnitude squared)
    power_spectrum = np.abs(fft_values) ** 2

    # Only consider positive frequencies
    positive_freq_idx = fft_freq > 0
    positive_freqs = fft_freq[positive_freq_idx]
    positive_power = power_spectrum[positive_freq_idx]

    # Find dominant frequencies
    if len(positive_power) > 0:
        top_indices = np.argsort(positive_power)[-top_n:][::-1]
        dominant_frequencies = positive_freqs[top_indices]

        # Convert frequencies to periods (in seconds)
        # Assume timestamps are in seconds with uniform sampling
        if len(ts_data.timestamps) > 1:
            sampling_rate = 1.0 / np.mean(np.diff(ts_data.timestamps))
            dominant_periods = 1.0 / (dominant_frequencies * sampling_rate)
        else:
            dominant_periods = np.zeros(len(dominant_frequencies))
    else:
        dominant_frequencies = np.zeros(top_n)
        dominant_periods = np.zeros(top_n)

    # Calculate spectral entropy (measure of complexity)
    power_norm = positive_power / np.sum(positive_power) if np.sum(positive_power) > 0 else positive_power
    spectral_entropy = -np.sum(power_norm * np.log2(power_norm + 1e-10))

    # Calculate spectral energy (only positive frequencies for consistency)
    spectral_energy = np.sum(positive_power)

    # Divide spectrum into low and high frequency bands
    median_freq_idx = len(positive_power) // 2
    low_freq_energy = np.sum(positive_power[:median_freq_idx])
    high_freq_energy = np.sum(positive_power[median_freq_idx:])

    return FFTFeatures(
        dominant_frequencies=dominant_frequencies,
        dominant_periods=dominant_periods,
        spectral_entropy=spectral_entropy,
        spectral_energy=spectral_energy,
        low_freq_energy=low_freq_energy,
        high_freq_energy=high_freq_energy
    )


def build_feature_vector(
    ts_data: TimeSeriesData,
    index: int = -1
) -> FeatureVector:
    """
    Build complete feature vector for a specific point in time.

    Args:
        ts_data: Raw time-series data
        index: Index of the point to create features for (-1 for latest)

    Returns:
        FeatureVector with all computed features
    """
    # Calculate all feature types
    rolling_stats = calculate_rolling_statistics(ts_data)
    seasonality = detect_seasonality(ts_data)
    derivatives = compute_derivatives(ts_data)
    fft_features = extract_fft_features(ts_data)

    # Extract values at specified index
    current_value = ts_data.values[index]
    current_timestamp = datetime.fromtimestamp(ts_data.timestamps[index])

    # Extract rolling statistics at index
    rolling_1h = rolling_stats[0] if len(rolling_stats) > 0 else None
    rolling_6h = rolling_stats[1] if len(rolling_stats) > 1 else None
    rolling_24h = rolling_stats[2] if len(rolling_stats) > 2 else None

    # Calculate basic statistics
    value_mean = np.mean(ts_data.values)
    value_std = np.std(ts_data.values)
    value_min = np.min(ts_data.values)
    value_max = np.max(ts_data.values)

    # Calculate Z-score
    zscore = (current_value - value_mean) / value_std if value_std > 0 else 0.0

    # Detect outliers using IQR method
    q25, q75 = np.percentile(ts_data.values, [25, 75])
    iqr = q75 - q25
    lower_bound = q25 - 1.5 * iqr
    upper_bound = q75 + 1.5 * iqr
    is_outlier = current_value < lower_bound or current_value > upper_bound

    # Calculate deviation from trend (using rolling mean)
    rolling_mean = rolling_24h.mean[index] if rolling_24h else value_mean
    deviation_from_trend = current_value - rolling_mean

    # Extract time-based features
    hour_of_day = current_timestamp.hour
    day_of_week = current_timestamp.weekday()
    is_weekend = day_of_week >= 5

    # Seasonal scores (how much current hour/day deviates from pattern)
    hourly_expected = seasonality.hourly_pattern[hour_of_day] if hour_of_day < len(seasonality.hourly_pattern) else value_mean
    daily_expected = seasonality.daily_pattern[day_of_week] if day_of_week < len(seasonality.daily_pattern) else value_mean

    hourly_seasonal_score = abs(current_value - hourly_expected) / value_std if value_std > 0 else 0.0
    daily_seasonal_score = abs(current_value - daily_expected) / value_std if value_std > 0 else 0.0

    # Velocity and acceleration
    rate_of_change = derivatives.first_derivative[index]
    acceleration = derivatives.second_derivative[index]
    velocity_zscore = (rate_of_change - derivatives.velocity_mean) / derivatives.velocity_std if derivatives.velocity_std > 0 else 0.0

    # FFT features
    dominant_period = fft_features.dominant_periods[0] if len(fft_features.dominant_periods) > 0 else 0.0
    total_energy = fft_features.spectral_energy
    low_freq_ratio = fft_features.low_freq_energy / total_energy if total_energy > 0 else 0.0

    return FeatureVector(
        metric_name=ts_data.metric_name,
        resource_hash=ts_data.resource_hash,
        timestamp=current_timestamp,
        current_value=float(current_value),
        value_mean=float(value_mean),
        value_std=float(value_std),
        value_min=float(value_min),
        value_max=float(value_max),
        rolling_1h_mean=float(rolling_1h.mean[index]) if rolling_1h else float(value_mean),
        rolling_1h_std=float(rolling_1h.std[index]) if rolling_1h else float(value_std),
        rolling_6h_mean=float(rolling_6h.mean[index]) if rolling_6h else float(value_mean),
        rolling_6h_std=float(rolling_6h.std[index]) if rolling_6h else float(value_std),
        rolling_24h_mean=float(rolling_24h.mean[index]) if rolling_24h else float(value_mean),
        rolling_24h_std=float(rolling_24h.std[index]) if rolling_24h else float(value_std),
        hour_of_day=hour_of_day,
        day_of_week=day_of_week,
        is_weekend=is_weekend,
        hourly_seasonal_score=float(hourly_seasonal_score),
        daily_seasonal_score=float(daily_seasonal_score),
        rate_of_change=float(rate_of_change),
        acceleration=float(acceleration),
        velocity_zscore=float(velocity_zscore),
        dominant_period_seconds=float(dominant_period),
        spectral_entropy=float(fft_features.spectral_entropy),
        low_freq_ratio=float(low_freq_ratio),
        zscore=float(zscore),
        is_outlier=bool(is_outlier),
        deviation_from_trend=float(deviation_from_trend)
    )


def build_feature_matrix(
    ts_data: TimeSeriesData,
    window_size: int = 100
) -> List[FeatureVector]:
    """
    Build feature vectors for multiple points in the time series.

    Args:
        ts_data: Raw time-series data
        window_size: Minimum number of points needed before generating features

    Returns:
        List of FeatureVectors, one for each point after window_size
    """
    if len(ts_data.values) < window_size:
        return []

    feature_vectors = []

    # Generate features for points after window_size
    for i in range(window_size, len(ts_data.values)):
        # Create a sliding window of data up to point i
        window_data = TimeSeriesData(
            timestamps=ts_data.timestamps[:i+1],
            values=ts_data.values[:i+1],
            metric_name=ts_data.metric_name,
            resource_hash=ts_data.resource_hash,
            unit=ts_data.unit
        )

        # Build feature vector for this point
        features = build_feature_vector(window_data, index=-1)
        feature_vectors.append(features)

    return feature_vectors
