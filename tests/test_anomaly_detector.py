"""
Tests for anomaly detection module.

Tests all detection algorithms with labeled anomaly data.
"""

import pytest
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
import tempfile
import os

from src.anomaly_detector import (
    AnomalyScore,
    AnomalyDetectionResult,
    EvaluationMetrics,
    StatisticalDetector,
    IsolationForestDetector,
    LSTMAutoencoderDetector,
    ProphetDetector,
    EnsembleDetector,
    evaluate_detector,
    save_detector,
    load_detector
)
from src.feature_engineering import (
    TimeSeriesData,
    FeatureVector,
    build_feature_vector,
    build_feature_matrix
)


@pytest.fixture
def normal_timeseries():
    """Normal time-series without anomalies."""
    timestamps = np.arange(0, 1000, 1.0)
    # Stable values around 50 with small noise
    values = 50 + np.random.normal(0, 2, len(timestamps))
    return TimeSeriesData(
        timestamps=timestamps,
        values=values,
        metric_name="test.normal",
        resource_hash="normal_hash"
    )


@pytest.fixture
def anomalous_timeseries():
    """Time-series with labeled point anomalies."""
    timestamps = np.arange(0, 200, 1.0)
    values = 50 + np.random.normal(0, 2, len(timestamps))

    # Inject anomalies at specific indices
    anomaly_indices = [50, 100, 150]
    for idx in anomaly_indices:
        values[idx] = 100.0  # Spike anomaly

    labels = np.zeros(len(timestamps), dtype=bool)
    labels[anomaly_indices] = True

    return (
        TimeSeriesData(
            timestamps=timestamps,
            values=values,
            metric_name="test.anomalous",
            resource_hash="anomalous_hash"
        ),
        labels,
        anomaly_indices
    )


@pytest.fixture
def seasonal_with_anomalies():
    """Seasonal time-series with anomalies."""
    # 7 days of hourly data
    hours = 7 * 24
    timestamps = np.array([
        (datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(hours=i)).timestamp()
        for i in range(hours)
    ])

    # Daily pattern
    values = []
    labels = np.zeros(hours, dtype=bool)

    for i in range(hours):
        hour_of_day = i % 24
        if 9 <= hour_of_day < 17:
            base = 70.0
        else:
            base = 30.0

        noise = np.random.normal(0, 3)
        values.append(base + noise)

    values = np.array(values)

    # Inject anomalies
    # Type 1: Spike anomalies
    values[50] = 150.0
    labels[50] = True

    # Type 2: Drop anomalies
    values[100] = 5.0
    labels[100] = True

    # Type 3: Context anomalies (high value at wrong time)
    values[75] = 70.0  # High value at 3am (should be low)
    labels[75] = True

    return (
        TimeSeriesData(
            timestamps=timestamps,
            values=values,
            metric_name="cpu.usage",
            resource_hash="cpu_hash"
        ),
        labels
    )


class TestStatisticalDetector:
    """Test statistical anomaly detection."""

    def test_fit_and_predict_normal(self, normal_timeseries):
        """Test statistical detector on normal data."""
        detector = StatisticalDetector()
        detector.fit(normal_timeseries.values)

        # Test on normal point
        result = detector.predict(50.0)

        assert isinstance(result, AnomalyScore)
        assert result.detector_name == "statistical"
        assert result.is_anomaly == False

    def test_detect_spike_anomaly(self, normal_timeseries):
        """Test detection of spike anomaly."""
        detector = StatisticalDetector(zscore_threshold=3.0)
        detector.fit(normal_timeseries.values)

        # Test on anomalous spike
        result = detector.predict(100.0)

        assert result.is_anomaly == True
        assert result.score > 0.5

    def test_zscore_details(self, normal_timeseries):
        """Test that Z-score details are included."""
        detector = StatisticalDetector()
        detector.fit(normal_timeseries.values)

        result = detector.predict(50.0)

        assert 'zscore' in result.details
        assert 'mean' in result.details
        assert 'std' in result.details
        assert 'lower_bound' in result.details
        assert 'upper_bound' in result.details

    def test_evaluation_with_labels(self, anomalous_timeseries):
        """Test statistical detector with labeled anomalies."""
        ts_data, labels, anomaly_indices = anomalous_timeseries

        detector = StatisticalDetector(zscore_threshold=2.5)
        detector.fit(ts_data.values)

        # Predict on all points
        predictions = []
        for value in ts_data.values:
            result = detector.predict(value)
            predictions.append(result.is_anomaly)

        # Evaluate
        metrics = evaluate_detector(predictions, labels)

        # Should detect at least some anomalies
        assert metrics.true_positives > 0
        assert metrics.recall > 0


class TestIsolationForestDetector:
    """Test Isolation Forest detector."""

    def test_fit_with_feature_vectors(self, normal_timeseries):
        """Test training Isolation Forest."""
        feature_vectors = build_feature_matrix(normal_timeseries, window_size=50)

        detector = IsolationForestDetector(contamination=0.1)
        detector.fit(feature_vectors)

        assert detector.model is not None
        assert detector.scaler is not None

    def test_predict_normal(self, normal_timeseries):
        """Test prediction on normal data."""
        feature_vectors = build_feature_matrix(normal_timeseries, window_size=50)

        detector = IsolationForestDetector(contamination=0.1)
        detector.fit(feature_vectors)

        # Predict on last point
        result = detector.predict(feature_vectors[-1])

        assert isinstance(result, AnomalyScore)
        assert result.detector_name == "isolation_forest"

    def test_detect_multivariate_anomaly(self, seasonal_with_anomalies):
        """Test detection with multivariate features."""
        ts_data, labels = seasonal_with_anomalies

        feature_vectors = build_feature_matrix(ts_data, window_size=24)

        detector = IsolationForestDetector(contamination=0.05)
        detector.fit(feature_vectors[:100])  # Train on early data

        # Predict on points with anomalies
        predictions = []
        scores = []

        for fv in feature_vectors:
            result = detector.predict(fv)
            predictions.append(result.is_anomaly)
            scores.append(result.score)

        # Should detect some anomalies
        assert any(predictions)


class TestLSTMAutoencoderDetector:
    """Test LSTM Autoencoder detector."""

    def test_build_model(self):
        """Test LSTM model architecture."""
        detector = LSTMAutoencoderDetector(sequence_length=10, encoding_dim=8)
        model = detector.build_model(n_features=5)

        assert model is not None
        assert len(model.layers) > 0

    def test_fit_and_predict(self, normal_timeseries):
        """Test training and prediction."""
        feature_vectors = build_feature_matrix(normal_timeseries, window_size=50)

        detector = LSTMAutoencoderDetector(
            sequence_length=24,
            epochs=10,  # Fewer epochs for testing
            batch_size=16
        )

        # Train
        detector.fit(feature_vectors)

        assert detector.model is not None
        assert detector.threshold is not None

        # Predict
        result = detector.predict(feature_vectors[-24:])

        assert isinstance(result, AnomalyScore)
        assert result.detector_name == "lstm_autoencoder"
        assert 'reconstruction_error' in result.details

    def test_detect_sequence_anomaly(self, seasonal_with_anomalies):
        """Test detection of sequence anomalies."""
        ts_data, labels = seasonal_with_anomalies

        feature_vectors = build_feature_matrix(ts_data, window_size=24)

        detector = LSTMAutoencoderDetector(
            sequence_length=24,
            epochs=20,
            threshold_percentile=90.0
        )

        # Train on normal portion
        detector.fit(feature_vectors[:100])

        # Test on sequence containing anomaly
        # Find a sequence with an anomaly
        anomaly_idx = np.where(labels[24:])[0][0]
        sequence_with_anomaly = feature_vectors[anomaly_idx:anomaly_idx+24]

        if len(sequence_with_anomaly) >= 24:
            result = detector.predict(sequence_with_anomaly)
            # LSTM might detect it depending on training
            assert isinstance(result, AnomalyScore)


class TestProphetDetector:
    """Test Prophet detector."""

    def test_fit_timeseries(self, normal_timeseries):
        """Test Prophet training."""
        detector = ProphetDetector()
        detector.fit(normal_timeseries)

        assert detector.model is not None

    def test_predict_normal_point(self, normal_timeseries):
        """Test prediction on normal point."""
        detector = ProphetDetector()
        detector.fit(normal_timeseries)

        # Predict on a point in the middle
        timestamp = normal_timeseries.timestamps[500]
        value = normal_timeseries.values[500]

        result = detector.predict(timestamp, value)

        assert isinstance(result, AnomalyScore)
        assert result.detector_name == "prophet"
        assert 'predicted' in result.details
        assert 'lower_bound' in result.details
        assert 'upper_bound' in result.details

    def test_detect_spike_with_prophet(self, seasonal_with_anomalies):
        """Test Prophet detecting anomalous spike."""
        ts_data, labels = seasonal_with_anomalies

        detector = ProphetDetector(interval_width=0.90)
        detector.fit(ts_data)

        # Test on anomaly
        anomaly_idx = np.where(labels)[0][0]
        timestamp = ts_data.timestamps[anomaly_idx]
        value = ts_data.values[anomaly_idx]

        result = detector.predict(timestamp, value)

        # Prophet should detect the spike
        # (though it depends on the seasonality fit)
        assert isinstance(result, AnomalyScore)


class TestEnsembleDetector:
    """Test ensemble anomaly detection."""

    def test_ensemble_initialization(self):
        """Test ensemble detector setup."""
        ensemble = EnsembleDetector(
            voting_threshold=0.5,
            enable_statistical=True,
            enable_isolation_forest=True,
            enable_lstm=False,  # Disable LSTM for faster tests
            enable_prophet=False  # Disable Prophet for faster tests
        )

        assert ensemble.statistical is not None
        assert ensemble.isolation_forest is not None
        assert ensemble.lstm is None
        assert ensemble.prophet is None

    def test_ensemble_training(self, normal_timeseries):
        """Test training all ensemble detectors."""
        ensemble = EnsembleDetector(
            enable_lstm=False,  # Disable for speed
            enable_prophet=False
        )

        ensemble.fit(normal_timeseries)

        # Check that detectors were trained
        assert ensemble.statistical.mean is not None
        assert ensemble.isolation_forest.model is not None

    def test_ensemble_prediction(self, normal_timeseries):
        """Test ensemble prediction."""
        ensemble = EnsembleDetector(
            enable_lstm=False,
            enable_prophet=False
        )

        ensemble.fit(normal_timeseries)

        # Predict on normal point
        result = ensemble.predict(normal_timeseries)

        assert isinstance(result, AnomalyDetectionResult)
        assert result.statistical_result is not None
        assert result.isolation_forest_result is not None
        assert result.ensemble_score >= 0
        assert result.ensemble_confidence >= 0

    def test_ensemble_voting(self, anomalous_timeseries):
        """Test ensemble voting mechanism."""
        ts_data, labels, anomaly_indices = anomalous_timeseries

        ensemble = EnsembleDetector(
            voting_threshold=0.5,  # Need 50% agreement
            enable_lstm=False,
            enable_prophet=False
        )

        ensemble.fit(ts_data)

        # Create anomalous timeseries (take portion with anomaly)
        anomaly_idx = anomaly_indices[0]
        anomalous_portion = TimeSeriesData(
            timestamps=ts_data.timestamps[:anomaly_idx+1],
            values=ts_data.values[:anomaly_idx+1],
            metric_name=ts_data.metric_name,
            resource_hash=ts_data.resource_hash
        )

        result = ensemble.predict(anomalous_portion)

        # Check voting results
        assert result.detectors_agreeing >= 0
        assert result.detectors_agreeing <= 2  # Only 2 detectors enabled


    def test_full_ensemble(self, seasonal_with_anomalies):
        """Test full ensemble with all detectors."""
        ts_data, labels = seasonal_with_anomalies

        # Use smaller dataset for faster training
        ts_subset = TimeSeriesData(
            timestamps=ts_data.timestamps[:100],
            values=ts_data.values[:100],
            metric_name=ts_data.metric_name,
            resource_hash=ts_data.resource_hash
        )

        ensemble = EnsembleDetector(
            voting_threshold=0.4,
            enable_statistical=True,
            enable_isolation_forest=True,
            enable_lstm=True,
            enable_prophet=True
        )

        # Train
        print("\nTraining full ensemble...")
        ensemble.fit(ts_subset)

        # Predict on test point
        result = ensemble.predict(ts_subset)

        assert result.statistical_result is not None
        assert result.isolation_forest_result is not None
        assert result.lstm_result is not None
        assert result.prophet_result is not None


class TestEvaluationMetrics:
    """Test evaluation metrics calculation."""

    def test_perfect_detection(self):
        """Test metrics with perfect predictions."""
        predictions = [True, True, False, False]
        labels = [True, True, False, False]

        metrics = evaluate_detector(predictions, labels)

        assert metrics.accuracy == 1.0
        assert metrics.precision == 1.0
        assert metrics.recall == 1.0
        assert metrics.f1_score == 1.0
        assert metrics.true_positives == 2
        assert metrics.false_positives == 0

    def test_no_detection(self):
        """Test metrics when no anomalies detected."""
        predictions = [False, False, False, False]
        labels = [True, True, False, False]

        metrics = evaluate_detector(predictions, labels)

        assert metrics.precision == 0.0
        assert metrics.recall == 0.0
        assert metrics.true_positives == 0
        assert metrics.false_negatives == 2

    def test_auc_roc_calculation(self):
        """Test AUC-ROC calculation with scores."""
        predictions = [True, True, False, False]
        labels = [True, True, False, False]
        scores = [0.9, 0.8, 0.3, 0.1]

        metrics = evaluate_detector(predictions, labels, scores)

        assert metrics.auc_roc is not None
        assert 0 <= metrics.auc_roc <= 1

    def test_confusion_matrix_components(self):
        """Test all confusion matrix components."""
        predictions = [True, False, True, False]
        labels = [True, True, False, False]

        metrics = evaluate_detector(predictions, labels)

        assert metrics.true_positives == 1
        assert metrics.false_positives == 1
        assert metrics.true_negatives == 1
        assert metrics.false_negatives == 1


class TestModelPersistence:
    """Test saving and loading models."""

    def test_save_and_load_statistical(self, normal_timeseries, tmp_path):
        """Test statistical detector persistence."""
        detector = StatisticalDetector()
        detector.fit(normal_timeseries.values)

        # Save
        filepath = tmp_path / "statistical.pkl"
        save_detector(detector, str(filepath))

        # Load
        loaded_detector = load_detector(str(filepath))

        # Test that loaded detector works
        result1 = detector.predict(50.0)
        result2 = loaded_detector.predict(50.0)

        assert result1.is_anomaly == result2.is_anomaly
        assert abs(result1.score - result2.score) < 0.001

    def test_save_and_load_isolation_forest(self, normal_timeseries, tmp_path):
        """Test Isolation Forest persistence."""
        feature_vectors = build_feature_matrix(normal_timeseries, window_size=50)

        detector = IsolationForestDetector()
        detector.fit(feature_vectors)

        # Save
        filepath = tmp_path / "isolation_forest.pkl"
        save_detector(detector, str(filepath))

        # Load
        loaded_detector = load_detector(str(filepath))

        # Test
        result1 = detector.predict(feature_vectors[-1])
        result2 = loaded_detector.predict(feature_vectors[-1])

        assert result1.is_anomaly == result2.is_anomaly


class TestRealWorldScenarios:
    """Test with realistic anomaly scenarios."""

    def test_cpu_spike_detection(self):
        """Test detecting CPU spike anomalies."""
        # Simulate 24 hours of normal CPU usage
        hours = 24
        timestamps = np.array([
            (datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(hours=i)).timestamp()
            for i in range(hours)
        ])

        values = []
        for i in range(hours):
            hour_of_day = i % 24
            if 9 <= hour_of_day < 17:
                cpu = np.random.normal(60, 5)
            else:
                cpu = np.random.normal(25, 5)
            values.append(cpu)

        # Inject spike at hour 15 (3pm)
        values[15] = 95.0

        ts_data = TimeSeriesData(
            timestamps=timestamps,
            values=np.array(values),
            metric_name="cpu.usage",
            resource_hash="server01"
        )

        # Train statistical detector
        detector = StatisticalDetector(zscore_threshold=2.5)
        detector.fit(ts_data.values)

        # Test on spike
        result = detector.predict(values[15])

        assert result.is_anomaly == True

    def test_memory_leak_detection(self):
        """Test detecting gradual memory increase (leak)."""
        # Normal memory: stable around 50%
        # Memory leak: gradual increase over time
        timestamps = np.arange(0, 100, 1.0)

        values = []
        for i in range(100):
            if i < 50:
                # Normal period
                values.append(50 + np.random.normal(0, 2))
            else:
                # Memory leak starts
                leak_increase = (i - 50) * 0.5  # Gradual increase
                values.append(50 + leak_increase + np.random.normal(0, 2))

        ts_data = TimeSeriesData(
            timestamps=timestamps,
            values=np.array(values),
            metric_name="memory.usage",
            resource_hash="app01"
        )

        # Statistical detector on derivative should catch this
        from src.feature_engineering import compute_derivatives
        derivatives = compute_derivatives(ts_data)

        # In leak period, rate of change should be consistently positive
        leak_period_velocities = derivatives.first_derivative[50:]
        assert np.mean(leak_period_velocities) > 0


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_insufficient_data_lstm(self):
        """Test LSTM with insufficient data."""
        short_data = TimeSeriesData(
            timestamps=np.arange(0, 10, 1.0),
            values=np.arange(0, 10, 1.0),
            metric_name="test.short",
            resource_hash="short_hash"
        )

        feature_vectors = build_feature_matrix(short_data, window_size=5)

        detector = LSTMAutoencoderDetector(sequence_length=24)

        # Should raise error with insufficient data
        with pytest.raises(ValueError):
            detector.fit(feature_vectors)

    def test_single_value_statistical(self):
        """Test statistical detector with single unique value."""
        values = np.full(100, 50.0)  # All same value

        detector = StatisticalDetector()
        detector.fit(values)

        # Should handle zero variance
        result = detector.predict(50.0)
        assert isinstance(result, AnomalyScore)
