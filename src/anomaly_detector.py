# ABOUTME: Multi-algorithm anomaly detection system for time-series metrics
# ABOUTME: Combines statistical, ML, and deep learning methods with ensemble voting

"""
Anomaly Detection for LogicMonitor Time-Series Data

Implements multiple detection algorithms:
1. Statistical Detection - Z-scores and IQR for univariate anomalies
2. Isolation Forest - Unsupervised ML for multivariate anomalies
3. LSTM Autoencoder - Deep learning for sequence anomalies
4. Prophet - Facebook's time-series forecasting for trend anomalies
5. Ensemble Voting - Combines all methods for robust detection

All detectors can be trained, evaluated, and persisted.
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import json
import pickle
from pathlib import Path

# Statistical methods
from scipy import stats

# Machine Learning
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix, roc_auc_score

# Deep Learning
import tensorflow as tf
from tensorflow import keras
from keras import layers, Model

# Time-series forecasting
from prophet import Prophet

# Feature engineering
from src.feature_engineering import (
    TimeSeriesData,
    FeatureVector,
    build_feature_vector,
    build_feature_matrix
)


@dataclass
class AnomalyScore:
    """Anomaly score from a single detector."""
    detector_name: str
    is_anomaly: bool
    score: float  # Confidence score (0-1)
    threshold: float
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AnomalyDetectionResult:
    """Combined result from all detectors."""
    timestamp: datetime
    metric_name: str
    resource_hash: str
    current_value: float

    # Individual detector results
    statistical_result: Optional[AnomalyScore] = None
    isolation_forest_result: Optional[AnomalyScore] = None
    lstm_result: Optional[AnomalyScore] = None
    prophet_result: Optional[AnomalyScore] = None

    # Ensemble result
    ensemble_is_anomaly: bool = False
    ensemble_score: float = 0.0
    ensemble_confidence: float = 0.0
    detectors_agreeing: int = 0


@dataclass
class EvaluationMetrics:
    """Model evaluation metrics."""
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    true_positives: int
    false_positives: int
    true_negatives: int
    false_negatives: int
    auc_roc: Optional[float] = None


class StatisticalDetector:
    """
    Statistical anomaly detection using Z-scores and IQR.

    Combines two methods:
    - Z-score: Points beyond threshold standard deviations
    - IQR: Points outside 1.5 * IQR from quartiles
    """

    def __init__(self, zscore_threshold: float = 3.0, iqr_multiplier: float = 1.5):
        self.zscore_threshold = zscore_threshold
        self.iqr_multiplier = iqr_multiplier
        self.mean = None
        self.std = None
        self.q25 = None
        self.q75 = None
        self.iqr = None

    def fit(self, values: np.ndarray):
        """Train on normal data to learn distribution."""
        self.mean = np.mean(values)
        self.std = np.std(values)
        self.q25, self.q75 = np.percentile(values, [25, 75])
        self.iqr = self.q75 - self.q25

    def predict(self, value: float) -> AnomalyScore:
        """Detect if value is an anomaly."""
        # Z-score method
        zscore = abs((value - self.mean) / self.std) if self.std > 0 else 0
        zscore_anomaly = zscore > self.zscore_threshold

        # IQR method
        lower_bound = self.q25 - self.iqr_multiplier * self.iqr
        upper_bound = self.q75 + self.iqr_multiplier * self.iqr
        iqr_anomaly = value < lower_bound or value > upper_bound

        # Combine both methods (OR logic)
        is_anomaly = zscore_anomaly or iqr_anomaly

        # Normalized score (0-1 based on z-score)
        score = min(1.0, zscore / (self.zscore_threshold * 2))

        return AnomalyScore(
            detector_name="statistical",
            is_anomaly=is_anomaly,
            score=score,
            threshold=self.zscore_threshold,
            details={
                "zscore": float(zscore),
                "zscore_anomaly": zscore_anomaly,
                "iqr_anomaly": iqr_anomaly,
                "value": float(value),
                "mean": float(self.mean),
                "std": float(self.std),
                "lower_bound": float(lower_bound),
                "upper_bound": float(upper_bound)
            }
        )


class IsolationForestDetector:
    """
    Isolation Forest for multivariate anomaly detection.

    Uses sklearn's IsolationForest to detect anomalies in feature space.
    """

    def __init__(self, contamination: float = 0.1, n_estimators: int = 100):
        self.contamination = contamination
        self.model = IsolationForest(
            contamination=contamination,
            n_estimators=n_estimators,
            random_state=42
        )
        self.scaler = StandardScaler()
        self.feature_names = None

    def fit(self, feature_vectors: List[FeatureVector]):
        """Train on feature vectors."""
        # Extract features for training
        X = self._extract_features(feature_vectors)
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled)

    def predict(self, feature_vector: FeatureVector) -> AnomalyScore:
        """Detect if feature vector is anomalous."""
        # Extract and scale features
        X = self._extract_features([feature_vector])
        X_scaled = self.scaler.transform(X)

        # Predict (-1 = anomaly, 1 = normal)
        prediction = self.model.predict(X_scaled)[0]
        is_anomaly = prediction == -1

        # Get anomaly score (lower = more anomalous)
        # Score is roughly between -0.5 and 0.5, normalize to 0-1
        raw_score = self.model.score_samples(X_scaled)[0]
        score = max(0, min(1, -raw_score))  # Invert and normalize

        return AnomalyScore(
            detector_name="isolation_forest",
            is_anomaly=is_anomaly,
            score=score,
            threshold=0.0,
            details={
                "raw_score": float(raw_score),
                "prediction": int(prediction)
            }
        )

    def _extract_features(self, feature_vectors: List[FeatureVector]) -> np.ndarray:
        """Extract numerical features from feature vectors."""
        features = []
        for fv in feature_vectors:
            # Select relevant numerical features
            row = [
                fv.current_value,
                fv.value_mean,
                fv.value_std,
                fv.rolling_1h_mean,
                fv.rolling_1h_std,
                fv.rolling_6h_mean,
                fv.rolling_6h_std,
                fv.rolling_24h_mean,
                fv.rolling_24h_std,
                fv.hour_of_day / 24.0,  # Normalize
                fv.day_of_week / 7.0,  # Normalize
                float(fv.is_weekend),
                fv.hourly_seasonal_score,
                fv.daily_seasonal_score,
                fv.rate_of_change,
                fv.acceleration,
                fv.velocity_zscore,
                fv.dominant_period_seconds / 86400.0,  # Normalize to days
                fv.spectral_entropy,
                fv.low_freq_ratio,
                fv.zscore,
                fv.deviation_from_trend
            ]
            features.append(row)

        if self.feature_names is None:
            self.feature_names = [
                'current_value', 'value_mean', 'value_std',
                'rolling_1h_mean', 'rolling_1h_std',
                'rolling_6h_mean', 'rolling_6h_std',
                'rolling_24h_mean', 'rolling_24h_std',
                'hour_of_day_norm', 'day_of_week_norm', 'is_weekend',
                'hourly_seasonal_score', 'daily_seasonal_score',
                'rate_of_change', 'acceleration', 'velocity_zscore',
                'dominant_period_days', 'spectral_entropy', 'low_freq_ratio',
                'zscore', 'deviation_from_trend'
            ]

        return np.array(features)


class LSTMAutoencoderDetector:
    """
    LSTM Autoencoder for sequence anomaly detection.

    Learns to reconstruct normal sequences. High reconstruction error = anomaly.
    """

    def __init__(
        self,
        sequence_length: int = 24,
        encoding_dim: int = 16,
        epochs: int = 50,
        batch_size: int = 32,
        threshold_percentile: float = 95.0
    ):
        self.sequence_length = sequence_length
        self.encoding_dim = encoding_dim
        self.epochs = epochs
        self.batch_size = batch_size
        self.threshold_percentile = threshold_percentile
        self.model = None
        self.scaler = StandardScaler()
        self.threshold = None

    def build_model(self, n_features: int):
        """Build LSTM Autoencoder architecture."""
        # Encoder
        inputs = layers.Input(shape=(self.sequence_length, n_features))
        encoded = layers.LSTM(self.encoding_dim * 2, return_sequences=True)(inputs)
        encoded = layers.LSTM(self.encoding_dim, return_sequences=False)(encoded)

        # Decoder
        decoded = layers.RepeatVector(self.sequence_length)(encoded)
        decoded = layers.LSTM(self.encoding_dim, return_sequences=True)(decoded)
        decoded = layers.LSTM(self.encoding_dim * 2, return_sequences=True)(decoded)
        outputs = layers.TimeDistributed(layers.Dense(n_features))(decoded)

        # Model
        model = Model(inputs=inputs, outputs=outputs)
        model.compile(optimizer='adam', loss='mse')

        return model

    def fit(self, feature_vectors: List[FeatureVector]):
        """Train autoencoder on normal sequences."""
        # Extract features
        X = self._extract_features_from_vectors(feature_vectors)

        if len(X) < self.sequence_length:
            raise ValueError(f"Not enough data. Need at least {self.sequence_length} points.")

        # Create sequences
        sequences = self._create_sequences(X)

        # Scale data
        n_samples, n_timesteps, n_features = sequences.shape
        sequences_2d = sequences.reshape(-1, n_features)
        sequences_scaled = self.scaler.fit_transform(sequences_2d)
        sequences = sequences_scaled.reshape(n_samples, n_timesteps, n_features)

        # Build and train model
        self.model = self.build_model(n_features)
        self.model.fit(
            sequences, sequences,
            epochs=self.epochs,
            batch_size=self.batch_size,
            verbose=0,
            validation_split=0.1
        )

        # Calculate threshold from training data
        reconstructions = self.model.predict(sequences, verbose=0)
        mse = np.mean(np.power(sequences - reconstructions, 2), axis=(1, 2))
        self.threshold = np.percentile(mse, self.threshold_percentile)

    def predict(self, feature_vectors: List[FeatureVector]) -> AnomalyScore:
        """Detect anomaly in sequence."""
        if self.model is None:
            raise ValueError("Model not trained. Call fit() first.")

        # Extract features
        X = self._extract_features_from_vectors(feature_vectors)

        # Take last sequence_length points
        if len(X) < self.sequence_length:
            # Pad with zeros if not enough data
            padding = np.zeros((self.sequence_length - len(X), X.shape[1]))
            X = np.vstack([padding, X])
        else:
            X = X[-self.sequence_length:]

        # Reshape to sequence
        sequence = X.reshape(1, self.sequence_length, X.shape[1])

        # Scale
        sequence_2d = sequence.reshape(-1, X.shape[1])
        sequence_scaled = self.scaler.transform(sequence_2d)
        sequence = sequence_scaled.reshape(1, self.sequence_length, X.shape[1])

        # Reconstruct
        reconstruction = self.model.predict(sequence, verbose=0)

        # Calculate MSE
        mse = np.mean(np.power(sequence - reconstruction, 2))

        # Detect anomaly
        is_anomaly = mse > self.threshold
        score = min(1.0, mse / (self.threshold * 2)) if self.threshold > 0 else 0.0

        return AnomalyScore(
            detector_name="lstm_autoencoder",
            is_anomaly=is_anomaly,
            score=score,
            threshold=float(self.threshold),
            details={
                "reconstruction_error": float(mse),
                "sequence_length": self.sequence_length
            }
        )

    def _extract_features_from_vectors(self, feature_vectors: List[FeatureVector]) -> np.ndarray:
        """Extract feature matrix from feature vectors."""
        features = []
        for fv in feature_vectors:
            row = [
                fv.current_value,
                fv.rolling_1h_mean,
                fv.rolling_24h_mean,
                fv.rate_of_change,
                fv.zscore
            ]
            features.append(row)
        return np.array(features)

    def _create_sequences(self, data: np.ndarray) -> np.ndarray:
        """Create sliding window sequences."""
        sequences = []
        for i in range(len(data) - self.sequence_length + 1):
            sequences.append(data[i:i + self.sequence_length])
        return np.array(sequences)


class ProphetDetector:
    """
    Prophet-based anomaly detection.

    Uses Facebook's Prophet for time-series forecasting.
    Anomalies are points that deviate significantly from forecast.
    """

    def __init__(self, interval_width: float = 0.95, uncertainty_samples: int = 100):
        self.interval_width = interval_width
        self.uncertainty_samples = uncertainty_samples
        self.model = None

    def fit(self, ts_data: TimeSeriesData):
        """Train Prophet model on time-series data."""
        # Prepare data for Prophet
        df = pd.DataFrame({
            'ds': pd.to_datetime(ts_data.timestamps, unit='s'),
            'y': ts_data.values
        })

        # Train Prophet
        self.model = Prophet(
            interval_width=self.interval_width,
            uncertainty_samples=self.uncertainty_samples,
            daily_seasonality=True,
            weekly_seasonality=True,
            yearly_seasonality=False
        )
        self.model.fit(df)

    def predict(self, timestamp: float, actual_value: float) -> AnomalyScore:
        """Detect if value is anomalous compared to forecast."""
        if self.model is None:
            raise ValueError("Model not trained. Call fit() first.")

        # Create dataframe for prediction
        df = pd.DataFrame({
            'ds': [pd.to_datetime(timestamp, unit='s')]
        })

        # Get forecast
        forecast = self.model.predict(df)

        predicted = forecast['yhat'].values[0]
        lower = forecast['yhat_lower'].values[0]
        upper = forecast['yhat_upper'].values[0]

        # Check if actual value is outside prediction interval
        is_anomaly = actual_value < lower or actual_value > upper

        # Calculate normalized score
        if is_anomaly:
            if actual_value < lower:
                deviation = lower - actual_value
            else:
                deviation = actual_value - upper
            interval_width = upper - lower
            score = min(1.0, deviation / interval_width) if interval_width > 0 else 1.0
        else:
            score = 0.0

        return AnomalyScore(
            detector_name="prophet",
            is_anomaly=is_anomaly,
            score=score,
            threshold=0.0,
            details={
                "predicted": float(predicted),
                "lower_bound": float(lower),
                "upper_bound": float(upper),
                "actual": float(actual_value)
            }
        )


class EnsembleDetector:
    """
    Ensemble anomaly detector combining all methods.

    Uses voting and weighted averaging for robust detection.
    """

    def __init__(
        self,
        voting_threshold: float = 0.5,  # Fraction of detectors that must agree
        enable_statistical: bool = True,
        enable_isolation_forest: bool = True,
        enable_lstm: bool = True,
        enable_prophet: bool = True
    ):
        self.voting_threshold = voting_threshold
        self.enable_statistical = enable_statistical
        self.enable_isolation_forest = enable_isolation_forest
        self.enable_lstm = enable_lstm
        self.enable_prophet = enable_prophet

        # Detectors
        self.statistical = StatisticalDetector() if enable_statistical else None
        self.isolation_forest = IsolationForestDetector() if enable_isolation_forest else None
        self.lstm = LSTMAutoencoderDetector() if enable_lstm else None
        self.prophet = ProphetDetector() if enable_prophet else None

    def fit(
        self,
        ts_data: TimeSeriesData,
        feature_vectors: Optional[List[FeatureVector]] = None
    ):
        """Train all enabled detectors."""
        print(f"Training ensemble detector for {ts_data.metric_name}...")

        # Generate feature vectors if not provided
        if feature_vectors is None:
            print("  Generating feature vectors...")
            feature_vectors = build_feature_matrix(ts_data, window_size=50)

        # Train Statistical detector
        if self.statistical:
            print("  Training statistical detector...")
            self.statistical.fit(ts_data.values)

        # Train Isolation Forest
        if self.isolation_forest and len(feature_vectors) > 0:
            print("  Training isolation forest...")
            self.isolation_forest.fit(feature_vectors)

        # Train LSTM Autoencoder
        if self.lstm and len(feature_vectors) >= 24:
            print("  Training LSTM autoencoder...")
            self.lstm.fit(feature_vectors)

        # Train Prophet
        if self.prophet:
            print("  Training Prophet model...")
            self.prophet.fit(ts_data)

        print("  Ensemble training complete!")

    def predict(
        self,
        ts_data: TimeSeriesData,
        feature_vector: Optional[FeatureVector] = None,
        feature_history: Optional[List[FeatureVector]] = None
    ) -> AnomalyDetectionResult:
        """Detect anomalies using ensemble voting."""
        # Get latest values
        current_value = ts_data.values[-1]
        current_timestamp = datetime.fromtimestamp(ts_data.timestamps[-1])

        # Generate feature vector if not provided
        if feature_vector is None:
            feature_vector = build_feature_vector(ts_data, index=-1)

        # Collect results from each detector
        results = {}

        # Statistical
        if self.statistical:
            results['statistical'] = self.statistical.predict(current_value)

        # Isolation Forest
        if self.isolation_forest:
            results['isolation_forest'] = self.isolation_forest.predict(feature_vector)

        # LSTM (needs sequence)
        if self.lstm:
            if feature_history is None and len(ts_data.values) >= 24:
                feature_history = build_feature_matrix(ts_data, window_size=24)[-24:]
            if feature_history and len(feature_history) >= 24:
                results['lstm'] = self.lstm.predict(feature_history[-24:])

        # Prophet
        if self.prophet:
            results['prophet'] = self.prophet.predict(
                ts_data.timestamps[-1],
                current_value
            )

        # Ensemble voting
        votes = sum(1 for r in results.values() if r.is_anomaly)
        total_detectors = len(results)
        agreement_ratio = votes / total_detectors if total_detectors > 0 else 0

        ensemble_is_anomaly = agreement_ratio >= self.voting_threshold
        ensemble_score = np.mean([r.score for r in results.values()])
        ensemble_confidence = agreement_ratio

        return AnomalyDetectionResult(
            timestamp=current_timestamp,
            metric_name=ts_data.metric_name,
            resource_hash=ts_data.resource_hash,
            current_value=current_value,
            statistical_result=results.get('statistical'),
            isolation_forest_result=results.get('isolation_forest'),
            lstm_result=results.get('lstm'),
            prophet_result=results.get('prophet'),
            ensemble_is_anomaly=ensemble_is_anomaly,
            ensemble_score=ensemble_score,
            ensemble_confidence=ensemble_confidence,
            detectors_agreeing=votes
        )


def evaluate_detector(
    predictions: List[bool],
    labels: List[bool],
    scores: Optional[List[float]] = None
) -> EvaluationMetrics:
    """
    Evaluate detector performance against labeled data.

    Args:
        predictions: Binary predictions (True = anomaly)
        labels: True labels
        scores: Optional confidence scores for AUC-ROC

    Returns:
        EvaluationMetrics with performance metrics
    """
    predictions = np.array(predictions, dtype=int)
    labels = np.array(labels, dtype=int)

    # Calculate metrics
    tn, fp, fn, tp = confusion_matrix(labels, predictions).ravel()

    accuracy = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) > 0 else 0
    precision = precision_score(labels, predictions, zero_division=0)
    recall = recall_score(labels, predictions, zero_division=0)
    f1 = f1_score(labels, predictions, zero_division=0)

    # AUC-ROC if scores provided
    auc_roc = None
    if scores is not None:
        try:
            auc_roc = roc_auc_score(labels, scores)
        except:
            pass

    return EvaluationMetrics(
        accuracy=accuracy,
        precision=precision,
        recall=recall,
        f1_score=f1,
        true_positives=int(tp),
        false_positives=int(fp),
        true_negatives=int(tn),
        false_negatives=int(fn),
        auc_roc=auc_roc
    )


def save_detector(detector: Any, filepath: str):
    """Save trained detector to file."""
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'wb') as f:
        pickle.dump(detector, f)


def load_detector(filepath: str) -> Any:
    """Load trained detector from file."""
    with open(filepath, 'rb') as f:
        return pickle.load(f)
