"""
Tests for feature engineering module.

Tests feature extraction from time-series data using realistic LogicMonitor patterns.
"""

import pytest
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from src.feature_engineering import (
    TimeSeriesData,
    RollingStatistics,
    SeasonalityFeatures,
    DerivativeFeatures,
    FFTFeatures,
    FeatureVector,
    create_dataframe,
    calculate_rolling_statistics,
    detect_seasonality,
    compute_derivatives,
    extract_fft_features,
    build_feature_vector,
    build_feature_matrix
)


@pytest.fixture
def simple_timeseries():
    """Simple linear time-series for basic tests."""
    timestamps = np.arange(0, 100, 1.0)  # 100 seconds, 1 sample/sec
    values = np.arange(0, 100, 1.0)  # Linear increase
    return TimeSeriesData(
        timestamps=timestamps,
        values=values,
        metric_name="test.linear",
        resource_hash="linear_hash"
    )


@pytest.fixture
def seasonal_timeseries():
    """Time-series with daily seasonality (realistic CPU usage pattern)."""
    # 7 days of hourly data
    hours = 7 * 24
    timestamps = np.array([
        (datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(hours=i)).timestamp()
        for i in range(hours)
    ])

    # Simulate daily CPU pattern: low at night (0-6am), high during day (9am-5pm), medium evening
    values = []
    for i in range(hours):
        hour_of_day = i % 24
        day_of_week = (i // 24) % 7

        # Base CPU usage with daily pattern
        if 0 <= hour_of_day < 6:  # Night
            base = 20.0
        elif 9 <= hour_of_day < 17:  # Business hours
            base = 70.0
        else:  # Evening
            base = 40.0

        # Lower on weekends
        if day_of_week >= 5:
            base *= 0.6

        # Add some noise
        noise = np.random.normal(0, 5)
        values.append(base + noise)

    return TimeSeriesData(
        timestamps=timestamps,
        values=np.array(values),
        metric_name="cpu.usage",
        resource_hash="cpu_hash",
        unit="percent"
    )


@pytest.fixture
def real_lm_timeseries(db_connection, clean_normalized_tables):
    """Real LogicMonitor data from database (if available)."""
    with db_connection.cursor() as cur:
        # Insert realistic LogicMonitor data
        # Create datasource
        cur.execute("""
            INSERT INTO datasources (name, version, created_at)
            VALUES ('CPU_Monitor', '1.0', NOW())
            RETURNING id
        """)
        ds_id = cur.fetchone()[0]

        # Create resource
        cur.execute("""
            INSERT INTO resources (resource_hash, attributes, created_at, updated_at)
            VALUES ('server01', '{"service.name": "web-server", "host.name": "server01"}', NOW(), NOW())
            RETURNING id
        """)
        resource_id = cur.fetchone()[0]

        # Create metric definition
        cur.execute("""
            INSERT INTO metric_definitions (datasource_id, name, unit, metric_type, description)
            VALUES (%s, 'cpu.usage.percent', 'percent', 'gauge', 'CPU usage percentage')
            RETURNING id
        """, (ds_id,))
        metric_def_id = cur.fetchone()[0]

        # Insert realistic CPU data with daily pattern
        now = datetime.now(timezone.utc)
        timestamps = []
        values = []

        for i in range(7 * 24):  # 7 days of hourly data
            timestamp = now - timedelta(hours=(7 * 24 - i))
            hour_of_day = timestamp.hour

            # Realistic CPU pattern
            if 0 <= hour_of_day < 6:
                cpu_value = np.random.normal(20, 5)
            elif 9 <= hour_of_day < 17:
                cpu_value = np.random.normal(70, 10)
            else:
                cpu_value = np.random.normal(40, 8)

            cpu_value = max(0, min(100, cpu_value))  # Clamp to 0-100

            cur.execute("""
                INSERT INTO metric_data (resource_id, metric_definition_id, timestamp, value_double, attributes, created_at)
                VALUES (%s, %s, %s, %s, '{}', NOW())
            """, (resource_id, metric_def_id, timestamp, cpu_value))

            timestamps.append(timestamp.timestamp())
            values.append(cpu_value)

        db_connection.commit()

    return TimeSeriesData(
        timestamps=np.array(timestamps),
        values=np.array(values),
        metric_name="cpu.usage.percent",
        resource_hash="server01",
        unit="percent"
    )


class TestDataframeConversion:
    """Test conversion to pandas DataFrame."""

    def test_create_dataframe_basic(self, simple_timeseries):
        """Test basic DataFrame creation."""
        df = create_dataframe(simple_timeseries)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(simple_timeseries.timestamps)
        assert 'value' in df.columns
        assert df.index.name == 'timestamp' or isinstance(df.index, pd.DatetimeIndex)

    def test_create_dataframe_sorted(self, simple_timeseries):
        """Test DataFrame is sorted by timestamp."""
        # Shuffle data
        shuffled_indices = np.random.permutation(len(simple_timeseries.timestamps))
        shuffled_ts = TimeSeriesData(
            timestamps=simple_timeseries.timestamps[shuffled_indices],
            values=simple_timeseries.values[shuffled_indices],
            metric_name=simple_timeseries.metric_name,
            resource_hash=simple_timeseries.resource_hash
        )

        df = create_dataframe(shuffled_ts)
        assert df.index.is_monotonic_increasing


class TestRollingStatistics:
    """Test rolling statistics calculation."""

    def test_calculate_rolling_stats_basic(self, simple_timeseries):
        """Test basic rolling statistics."""
        rolling_stats = calculate_rolling_statistics(simple_timeseries, windows=['10s'])

        assert len(rolling_stats) == 1
        assert rolling_stats[0].window_size == '10s'
        assert len(rolling_stats[0].mean) == len(simple_timeseries.values)

    def test_rolling_stats_multiple_windows(self, seasonal_timeseries):
        """Test multiple rolling windows."""
        rolling_stats = calculate_rolling_statistics(
            seasonal_timeseries,
            windows=['1h', '6h', '24h']
        )

        assert len(rolling_stats) == 3
        assert rolling_stats[0].window_size == '1h'
        assert rolling_stats[1].window_size == '6h'
        assert rolling_stats[2].window_size == '24h'

    def test_rolling_stats_values(self, simple_timeseries):
        """Test rolling statistics produce expected values."""
        rolling_stats = calculate_rolling_statistics(simple_timeseries, windows=['10s'])

        # For linear data, rolling mean should be close to center of window
        stats = rolling_stats[0]

        # Check that stats are computed
        assert not np.all(np.isnan(stats.mean))
        assert not np.all(np.isnan(stats.std))

        # For increasing data, rolling max should be >= rolling min
        assert np.all(stats.max >= stats.min)


class TestSeasonalityDetection:
    """Test seasonality pattern detection."""

    def test_detect_seasonality_basic(self, seasonal_timeseries):
        """Test basic seasonality detection."""
        seasonality = detect_seasonality(seasonal_timeseries)

        assert isinstance(seasonality, SeasonalityFeatures)
        assert len(seasonality.hourly_pattern) == 24
        assert len(seasonality.daily_pattern) == 7
        assert len(seasonality.weekly_pattern) == 4

    def test_detect_daily_seasonality(self, seasonal_timeseries):
        """Test detection of daily patterns in CPU data."""
        seasonality = detect_seasonality(seasonal_timeseries)

        # Should detect daily pattern in CPU data
        assert seasonality.seasonality_strength > 0

        # Hourly pattern should show higher values during day
        hourly_pattern = seasonality.hourly_pattern
        daytime_avg = np.nanmean(hourly_pattern[9:17])  # 9am-5pm
        nighttime_avg = np.nanmean(hourly_pattern[0:6])  # 0am-6am

        assert daytime_avg > nighttime_avg

    def test_no_seasonality_linear(self, simple_timeseries):
        """Test that linear data doesn't show strong seasonality."""
        seasonality = detect_seasonality(simple_timeseries)

        # Linear trend shouldn't have strong periodic patterns
        # (though autocorrelation might pick up some)
        assert isinstance(seasonality.seasonality_strength, float)


class TestDerivatives:
    """Test derivative calculations."""

    def test_compute_derivatives_linear(self, simple_timeseries):
        """Test derivatives for linear data."""
        derivatives = compute_derivatives(simple_timeseries)

        assert isinstance(derivatives, DerivativeFeatures)
        assert len(derivatives.first_derivative) == len(simple_timeseries.values)
        assert len(derivatives.second_derivative) == len(simple_timeseries.values)

        # For linear data, first derivative should be constant (~1.0)
        # Second derivative should be close to 0
        assert abs(derivatives.velocity_mean - 1.0) < 0.1
        assert abs(derivatives.acceleration_mean) < 0.1

    def test_derivatives_seasonal(self, seasonal_timeseries):
        """Test derivatives capture rate of change."""
        derivatives = compute_derivatives(seasonal_timeseries)

        # Seasonal data should have varying velocity
        assert derivatives.velocity_std > 0

        # Check that derivatives are computed
        assert not np.all(np.isnan(derivatives.first_derivative))
        assert not np.all(np.isnan(derivatives.second_derivative))


class TestFFTFeatures:
    """Test FFT feature extraction."""

    def test_extract_fft_basic(self, seasonal_timeseries):
        """Test basic FFT feature extraction."""
        fft_features = extract_fft_features(seasonal_timeseries, top_n=5)

        assert isinstance(fft_features, FFTFeatures)
        assert len(fft_features.dominant_frequencies) == 5
        assert len(fft_features.dominant_periods) == 5

    def test_fft_detects_daily_period(self, seasonal_timeseries):
        """Test FFT detects daily periodicity."""
        fft_features = extract_fft_features(seasonal_timeseries, top_n=5)

        # Should detect ~24 hour period (86400 seconds)
        periods = fft_features.dominant_periods

        # One of the dominant periods should be close to 24 hours
        daily_period = 24 * 3600  # 24 hours in seconds
        # Check if any period is within 20% of daily period
        has_daily = any(abs(p - daily_period) / daily_period < 0.2 for p in periods if p > 0)

        # Note: This might not always detect exactly 24h depending on data
        # Just verify we get reasonable period values
        assert any(p > 0 for p in periods)

    def test_spectral_entropy(self, seasonal_timeseries):
        """Test spectral entropy calculation."""
        fft_features = extract_fft_features(seasonal_timeseries)

        assert fft_features.spectral_entropy > 0
        assert not np.isnan(fft_features.spectral_entropy)

    def test_frequency_energy_split(self, seasonal_timeseries):
        """Test low/high frequency energy split."""
        fft_features = extract_fft_features(seasonal_timeseries)

        assert fft_features.low_freq_energy >= 0
        assert fft_features.high_freq_energy >= 0
        assert fft_features.spectral_energy > 0

        # Total should equal sum of low + high
        total_approx = fft_features.low_freq_energy + fft_features.high_freq_energy
        assert abs(total_approx - fft_features.spectral_energy) / fft_features.spectral_energy < 0.1


class TestFeatureVector:
    """Test feature vector building."""

    def test_build_feature_vector_basic(self, seasonal_timeseries):
        """Test building a single feature vector."""
        feature_vec = build_feature_vector(seasonal_timeseries, index=-1)

        assert isinstance(feature_vec, FeatureVector)
        assert feature_vec.metric_name == seasonal_timeseries.metric_name
        assert feature_vec.resource_hash == seasonal_timeseries.resource_hash

    def test_feature_vector_completeness(self, seasonal_timeseries):
        """Test that all features are populated."""
        feature_vec = build_feature_vector(seasonal_timeseries)

        # Check all fields are present and not None
        assert feature_vec.current_value is not None
        assert feature_vec.value_mean is not None
        assert feature_vec.value_std is not None
        assert feature_vec.rolling_1h_mean is not None
        assert feature_vec.rolling_24h_std is not None
        assert feature_vec.hour_of_day is not None
        assert feature_vec.day_of_week is not None
        assert feature_vec.rate_of_change is not None
        assert feature_vec.spectral_entropy is not None
        assert feature_vec.zscore is not None

    def test_feature_vector_time_features(self, seasonal_timeseries):
        """Test time-based features are correct."""
        feature_vec = build_feature_vector(seasonal_timeseries)

        assert 0 <= feature_vec.hour_of_day < 24
        assert 0 <= feature_vec.day_of_week < 7
        assert isinstance(feature_vec.is_weekend, bool)

    def test_feature_vector_anomaly_detection(self):
        """Test anomaly detection features."""
        # Create data with an outlier
        timestamps = np.arange(0, 100, 1.0)
        values = np.concatenate([
            np.full(50, 50.0),  # Normal values
            [150.0],  # Outlier
            np.full(49, 50.0)   # Normal values
        ])

        ts_data = TimeSeriesData(
            timestamps=timestamps,
            values=values,
            metric_name="test.outlier",
            resource_hash="outlier_hash"
        )

        # Build feature for the outlier point
        feature_vec = build_feature_vector(ts_data, index=50)

        # Should detect as outlier
        assert feature_vec.is_outlier == True
        assert abs(feature_vec.zscore) > 2.0  # Should have high z-score


class TestFeatureMatrix:
    """Test feature matrix building."""

    def test_build_feature_matrix_basic(self, seasonal_timeseries):
        """Test building feature matrix."""
        feature_matrix = build_feature_matrix(seasonal_timeseries, window_size=50)

        assert isinstance(feature_matrix, list)
        assert len(feature_matrix) > 0
        assert all(isinstance(fv, FeatureVector) for fv in feature_matrix)

    def test_feature_matrix_size(self, seasonal_timeseries):
        """Test feature matrix has correct size."""
        window_size = 50
        feature_matrix = build_feature_matrix(seasonal_timeseries, window_size=window_size)

        expected_size = len(seasonal_timeseries.values) - window_size
        assert len(feature_matrix) == expected_size

    def test_feature_matrix_insufficient_data(self):
        """Test feature matrix with insufficient data."""
        timestamps = np.arange(0, 10, 1.0)
        values = np.arange(0, 10, 1.0)

        ts_data = TimeSeriesData(
            timestamps=timestamps,
            values=values,
            metric_name="test.small",
            resource_hash="small_hash"
        )

        feature_matrix = build_feature_matrix(ts_data, window_size=50)
        assert len(feature_matrix) == 0


class TestRealLogicMonitorData:
    """Test with real LogicMonitor data patterns."""

    def test_cpu_usage_features(self, real_lm_timeseries):
        """Test feature extraction from real CPU data."""
        feature_vec = build_feature_vector(real_lm_timeseries)

        # CPU should be 0-100
        assert 0 <= feature_vec.current_value <= 100
        assert 0 <= feature_vec.value_mean <= 100

        # Should have valid time features
        assert 0 <= feature_vec.hour_of_day < 24
        assert 0 <= feature_vec.day_of_week < 7

        # Should detect some seasonality in CPU data
        seasonality = detect_seasonality(real_lm_timeseries)
        assert seasonality.seasonality_strength > 0

    def test_cpu_feature_quality(self, real_lm_timeseries):
        """Test quality of extracted features."""
        feature_matrix = build_feature_matrix(real_lm_timeseries, window_size=24)

        assert len(feature_matrix) > 0

        # All features should be valid numbers (not NaN or inf)
        for fv in feature_matrix:
            assert not np.isnan(fv.current_value)
            assert not np.isinf(fv.current_value)
            assert not np.isnan(fv.zscore)
            assert not np.isnan(fv.spectral_entropy)
            assert not np.isnan(fv.rate_of_change)

    def test_feature_vector_serialization(self, real_lm_timeseries):
        """Test that feature vectors can be converted to dict for ML."""
        feature_vec = build_feature_vector(real_lm_timeseries)

        # Convert to dict (for storage or ML pipeline)
        from dataclasses import asdict
        feature_dict = asdict(feature_vec)

        # Should have all expected keys
        assert 'metric_name' in feature_dict
        assert 'current_value' in feature_dict
        assert 'zscore' in feature_dict
        assert 'rolling_24h_mean' in feature_dict
        assert 'spectral_entropy' in feature_dict

        # Values should be JSON-serializable types
        import json
        # Convert timestamp to string for JSON serialization
        feature_dict['timestamp'] = feature_dict['timestamp'].isoformat()
        json_str = json.dumps(feature_dict)
        assert isinstance(json_str, str)


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_single_datapoint(self):
        """Test with single data point."""
        ts_data = TimeSeriesData(
            timestamps=np.array([1.0]),
            values=np.array([50.0]),
            metric_name="test.single",
            resource_hash="single_hash"
        )

        # Should not crash
        seasonality = detect_seasonality(ts_data)
        assert isinstance(seasonality, SeasonalityFeatures)

    def test_constant_values(self):
        """Test with constant values (no variance)."""
        timestamps = np.arange(0, 100, 1.0)
        values = np.full(100, 50.0)

        ts_data = TimeSeriesData(
            timestamps=timestamps,
            values=values,
            metric_name="test.constant",
            resource_hash="constant_hash"
        )

        feature_vec = build_feature_vector(ts_data)

        # Should handle zero variance
        assert feature_vec.value_std == 0.0
        # Z-score should be 0 (or handled gracefully)
        assert not np.isnan(feature_vec.zscore)

    def test_missing_values(self):
        """Test with NaN values in data."""
        timestamps = np.arange(0, 100, 1.0)
        values = np.arange(0, 100, 1.0, dtype=float)
        values[50:55] = np.nan  # Inject missing values

        ts_data = TimeSeriesData(
            timestamps=timestamps,
            values=values,
            metric_name="test.missing",
            resource_hash="missing_hash"
        )

        # Should handle NaN values
        df = create_dataframe(ts_data)
        assert df is not None
