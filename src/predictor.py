# ABOUTME: Multi-algorithm time-series forecasting system for capacity planning
# ABOUTME: Implements ARIMA, Prophet, LSTM, and growth models with confidence intervals

"""
Time-Series Forecasting for LogicMonitor Metrics

Implements multiple forecasting algorithms:
1. ARIMA - Classical statistical forecasting for univariate series
2. Prophet - Facebook's forecasting with trend and seasonality
3. LSTM - Deep learning for complex patterns
4. Growth Models - Capacity planning (linear, exponential, logistic)
5. Ensemble - Combines multiple forecasters

All models provide confidence intervals and accuracy metrics.
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import warnings

# Statistical models
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller
from statsmodels.tools.sm_exceptions import ConvergenceWarning

# Prophet
from prophet import Prophet

# Deep Learning
import tensorflow as tf
from keras import layers, Model

# Metrics
from sklearn.metrics import mean_absolute_error, mean_squared_error

# Feature engineering
from src.feature_engineering import TimeSeriesData

warnings.filterwarnings('ignore', category=ConvergenceWarning)
warnings.filterwarnings('ignore', category=FutureWarning)


@dataclass
class ForecastResult:
    """Forecast result with confidence intervals."""
    timestamps: np.ndarray  # Future timestamps
    predictions: np.ndarray  # Point predictions
    lower_bound: np.ndarray  # Lower confidence interval
    upper_bound: np.ndarray  # Upper confidence interval
    model_name: str
    confidence_level: float = 0.95
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AccuracyMetrics:
    """Model accuracy metrics."""
    mae: float  # Mean Absolute Error
    rmse: float  # Root Mean Squared Error
    mape: float  # Mean Absolute Percentage Error
    smape: float  # Symmetric MAPE
    r2_score: float  # R-squared
    forecast_horizon: int
    model_name: str


@dataclass
class CapacityForecast:
    """Capacity planning forecast."""
    current_utilization: float
    forecast_utilization: np.ndarray
    capacity_limit: float
    time_to_limit: Optional[float]  # Days until limit reached
    recommended_capacity: float  # Recommended capacity headroom
    growth_rate: float  # Per day
    model_type: str  # linear, exponential, logistic


class ARIMAPredictor:
    """
    ARIMA forecasting for univariate time series.

    Auto-selects order (p,d,q) based on AIC criterion.
    """

    def __init__(
        self,
        order: Optional[Tuple[int, int, int]] = None,
        seasonal_order: Optional[Tuple[int, int, int, int]] = None
    ):
        self.order = order
        self.seasonal_order = seasonal_order
        self.model = None
        self.fitted_model = None

    def auto_select_order(self, data: np.ndarray) -> Tuple[int, int, int]:
        """Automatically select ARIMA order using AIC."""
        # Check for constant or near-constant values
        if np.std(data) < 1e-6:
            # For constant data, use simple AR(1) model
            return (1, 0, 0)

        # Check stationarity
        d = 0
        try:
            adf_result = adfuller(data)
            if adf_result[1] > 0.05:  # Not stationary
                d = 1
        except:
            d = 0

        # Grid search for p and q (simplified)
        best_aic = np.inf
        best_order = (1, d, 1)

        for p in range(0, 4):
            for q in range(0, 4):
                try:
                    model = ARIMA(data, order=(p, d, q))
                    fitted = model.fit()
                    if fitted.aic < best_aic:
                        best_aic = fitted.aic
                        best_order = (p, d, q)
                except:
                    continue

        return best_order

    def fit(self, ts_data: TimeSeriesData):
        """Train ARIMA model on time series."""
        data = ts_data.values

        # Auto-select order if not specified
        if self.order is None:
            self.order = self.auto_select_order(data)

        # Fit model (only pass seasonal_order if specified)
        if self.seasonal_order is not None:
            self.model = ARIMA(data, order=self.order, seasonal_order=self.seasonal_order)
        else:
            self.model = ARIMA(data, order=self.order)
        self.fitted_model = self.model.fit()

    def predict(
        self,
        steps: int,
        last_timestamp: float,
        interval_seconds: float = 3600.0,
        confidence_level: float = 0.95
    ) -> ForecastResult:
        """Generate forecast with confidence intervals."""
        if self.fitted_model is None:
            raise ValueError("Model not trained. Call fit() first.")

        # Generate forecast with confidence intervals
        forecast_obj = self.fitted_model.get_forecast(steps=steps)
        forecast_summary = forecast_obj.summary_frame(alpha=1-confidence_level)

        # Extract predictions and confidence intervals
        forecast = forecast_summary['mean'].values
        lower = forecast_summary.iloc[:, 2].values  # Lower CI column
        upper = forecast_summary.iloc[:, 3].values  # Upper CI column

        # Generate timestamps
        timestamps = np.array([
            last_timestamp + (i + 1) * interval_seconds
            for i in range(steps)
        ])

        return ForecastResult(
            timestamps=timestamps,
            predictions=forecast,  # Already numpy array
            lower_bound=lower,
            upper_bound=upper,
            model_name="ARIMA",
            confidence_level=confidence_level,
            metadata={"order": self.order, "aic": self.fitted_model.aic}
        )


class ProphetPredictor:
    """
    Prophet forecasting with trend and seasonality.

    Handles daily, weekly, yearly seasonality automatically.
    """

    def __init__(
        self,
        growth: str = 'linear',
        yearly_seasonality: bool = False,
        weekly_seasonality: bool = True,
        daily_seasonality: bool = True,
        interval_width: float = 0.95
    ):
        self.growth = growth
        self.yearly_seasonality = yearly_seasonality
        self.weekly_seasonality = weekly_seasonality
        self.daily_seasonality = daily_seasonality
        self.interval_width = interval_width
        self.model = None

    def fit(self, ts_data: TimeSeriesData):
        """Train Prophet model."""
        # Prepare data
        df = pd.DataFrame({
            'ds': pd.to_datetime(ts_data.timestamps, unit='s'),
            'y': ts_data.values
        })

        # Create and fit model
        self.model = Prophet(
            growth=self.growth,
            yearly_seasonality=self.yearly_seasonality,
            weekly_seasonality=self.weekly_seasonality,
            daily_seasonality=self.daily_seasonality,
            interval_width=self.interval_width
        )
        self.model.fit(df)

    def predict(
        self,
        steps: int,
        last_timestamp: float,
        interval_seconds: float = 3600.0
    ) -> ForecastResult:
        """Generate forecast with confidence intervals."""
        if self.model is None:
            raise ValueError("Model not trained. Call fit() first.")

        # Generate future timestamps
        last_dt = pd.to_datetime(last_timestamp, unit='s')
        future_dates = pd.date_range(
            start=last_dt + timedelta(seconds=interval_seconds),
            periods=steps,
            freq=f'{int(interval_seconds)}s'
        )

        future_df = pd.DataFrame({'ds': future_dates})

        # Generate forecast
        forecast = self.model.predict(future_df)

        timestamps = future_dates.astype(np.int64) // 10**9  # Convert to Unix timestamp

        return ForecastResult(
            timestamps=timestamps.values,
            predictions=forecast['yhat'].values,
            lower_bound=forecast['yhat_lower'].values,
            upper_bound=forecast['yhat_upper'].values,
            model_name="Prophet",
            confidence_level=self.interval_width,
            metadata={
                "growth": self.growth,
                "trend": forecast['trend'].values.tolist()[:5]  # Sample
            }
        )


class LSTMPredictor:
    """
    LSTM neural network for complex pattern forecasting.

    Uses sequence-to-sequence architecture.
    """

    def __init__(
        self,
        lookback_window: int = 24,
        lstm_units: int = 50,
        epochs: int = 50,
        batch_size: int = 32,
        dropout: float = 0.2
    ):
        self.lookback_window = lookback_window
        self.lstm_units = lstm_units
        self.epochs = epochs
        self.batch_size = batch_size
        self.dropout = dropout
        self.model = None
        self.mean = None
        self.std = None

    def build_model(self) -> Model:
        """Build LSTM architecture."""
        model = tf.keras.Sequential([
            layers.LSTM(self.lstm_units, return_sequences=True, input_shape=(self.lookback_window, 1)),
            layers.Dropout(self.dropout),
            layers.LSTM(self.lstm_units // 2, return_sequences=False),
            layers.Dropout(self.dropout),
            layers.Dense(25),
            layers.Dense(1)
        ])

        model.compile(optimizer='adam', loss='mse', metrics=['mae'])
        return model

    def create_sequences(self, data: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Create training sequences."""
        X, y = [], []

        for i in range(len(data) - self.lookback_window):
            X.append(data[i:i + self.lookback_window])
            y.append(data[i + self.lookback_window])

        return np.array(X), np.array(y)

    def fit(self, ts_data: TimeSeriesData):
        """Train LSTM model."""
        data = ts_data.values.copy()

        # Normalize
        self.mean = np.mean(data)
        self.std = np.std(data)
        data_normalized = (data - self.mean) / (self.std + 1e-8)

        # Create sequences
        X, y = self.create_sequences(data_normalized)

        if len(X) == 0:
            raise ValueError(f"Not enough data. Need at least {self.lookback_window + 1} points.")

        # Reshape for LSTM
        X = X.reshape(X.shape[0], X.shape[1], 1)

        # Build and train
        self.model = self.build_model()
        self.model.fit(
            X, y,
            epochs=self.epochs,
            batch_size=self.batch_size,
            validation_split=0.1,
            verbose=0
        )

    def predict(
        self,
        steps: int,
        ts_data: TimeSeriesData,
        interval_seconds: float = 3600.0,
        confidence_level: float = 0.95
    ) -> ForecastResult:
        """Generate multi-step forecast."""
        if self.model is None:
            raise ValueError("Model not trained. Call fit() first.")

        data = ts_data.values.copy()
        last_timestamp = ts_data.timestamps[-1]

        # Normalize
        data_normalized = (data - self.mean) / (self.std + 1e-8)

        # Take last lookback_window points as seed
        current_sequence = data_normalized[-self.lookback_window:].copy()

        predictions = []
        for _ in range(steps):
            # Reshape for prediction
            X = current_sequence.reshape(1, self.lookback_window, 1)

            # Predict next point
            pred_normalized = self.model.predict(X, verbose=0)[0, 0]

            # Denormalize
            pred = pred_normalized * self.std + self.mean
            predictions.append(pred)

            # Update sequence (shift and append)
            current_sequence = np.append(current_sequence[1:], pred_normalized)

        predictions = np.array(predictions)

        # Estimate confidence intervals (simplified using training error)
        # In practice, use monte carlo dropout or ensembles
        training_error = self.std * 0.1  # Simplified
        lower_bound = predictions - 1.96 * training_error
        upper_bound = predictions + 1.96 * training_error

        # Generate timestamps
        timestamps = np.array([
            last_timestamp + (i + 1) * interval_seconds
            for i in range(steps)
        ])

        return ForecastResult(
            timestamps=timestamps,
            predictions=predictions,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            model_name="LSTM",
            confidence_level=confidence_level,
            metadata={
                "lookback_window": self.lookback_window,
                "lstm_units": self.lstm_units
            }
        )


class GrowthModelPredictor:
    """
    Growth models for capacity planning.

    Supports linear, exponential, and logistic growth.
    """

    def __init__(self, model_type: str = 'linear', capacity_limit: Optional[float] = None):
        """
        Initialize growth model.

        Args:
            model_type: 'linear', 'exponential', or 'logistic'
            capacity_limit: Max capacity (required for logistic model)
        """
        self.model_type = model_type
        self.capacity_limit = capacity_limit
        self.params = {}

    def fit(self, ts_data: TimeSeriesData):
        """Fit growth model to data."""
        t = np.arange(len(ts_data.values))
        y = ts_data.values

        if self.model_type == 'linear':
            # y = a*t + b
            coeffs = np.polyfit(t, y, 1)
            self.params = {'slope': coeffs[0], 'intercept': coeffs[1]}

        elif self.model_type == 'exponential':
            # y = a * exp(b*t)
            # log(y) = log(a) + b*t
            log_y = np.log(np.maximum(y, 1e-10))  # Avoid log(0)
            coeffs = np.polyfit(t, log_y, 1)
            self.params = {'a': np.exp(coeffs[1]), 'b': coeffs[0]}

        elif self.model_type == 'logistic':
            # y = L / (1 + exp(-k(t-t0)))
            if self.capacity_limit is None:
                self.capacity_limit = np.max(y) * 1.5  # Estimate

            # Simplified fitting
            L = self.capacity_limit
            y_normalized = y / L
            # Avoid division by zero
            y_normalized = np.clip(y_normalized, 0.01, 0.99)
            log_odds = np.log(y_normalized / (1 - y_normalized))

            coeffs = np.polyfit(t, log_odds, 1)
            self.params = {'L': L, 'k': coeffs[0], 't0': -coeffs[1] / coeffs[0]}

    def predict(
        self,
        steps: int,
        ts_data: TimeSeriesData,
        interval_seconds: float = 3600.0
    ) -> ForecastResult:
        """Generate growth forecast."""
        n = len(ts_data.values)
        t_future = np.arange(n, n + steps)

        if self.model_type == 'linear':
            predictions = self.params['slope'] * t_future + self.params['intercept']

        elif self.model_type == 'exponential':
            predictions = self.params['a'] * np.exp(self.params['b'] * t_future)

        elif self.model_type == 'logistic':
            L = self.params['L']
            k = self.params['k']
            t0 = self.params['t0']
            predictions = L / (1 + np.exp(-k * (t_future - t0)))

        # Simple confidence intervals based on residuals
        residuals = ts_data.values - self._predict_historical(len(ts_data.values))
        std_residual = np.std(residuals)

        lower_bound = predictions - 1.96 * std_residual
        upper_bound = predictions + 1.96 * std_residual

        last_timestamp = ts_data.timestamps[-1]
        timestamps = np.array([
            last_timestamp + (i + 1) * interval_seconds
            for i in range(steps)
        ])

        return ForecastResult(
            timestamps=timestamps,
            predictions=predictions,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            model_name=f"Growth_{self.model_type}",
            confidence_level=0.95,
            metadata=self.params
        )

    def _predict_historical(self, n: int) -> np.ndarray:
        """Predict on historical data for residual calculation."""
        t = np.arange(n)

        if self.model_type == 'linear':
            return self.params['slope'] * t + self.params['intercept']
        elif self.model_type == 'exponential':
            return self.params['a'] * np.exp(self.params['b'] * t)
        elif self.model_type == 'logistic':
            L = self.params['L']
            k = self.params['k']
            t0 = self.params['t0']
            return L / (1 + np.exp(-k * (t - t0)))

    def capacity_forecast(
        self,
        ts_data: TimeSeriesData,
        capacity_limit: float,
        forecast_days: int = 30
    ) -> CapacityForecast:
        """Generate capacity planning forecast."""
        current_utilization = ts_data.values[-1]

        # Forecast for specified days (assume hourly data)
        steps = forecast_days * 24
        forecast = self.predict(steps, ts_data, interval_seconds=3600.0)

        # Calculate time to limit
        time_to_limit = None
        for i, pred in enumerate(forecast.predictions):
            if pred >= capacity_limit:
                # Use (i + 1) to ensure time_to_limit > 0
                time_to_limit = (i + 1) / 24.0  # Convert to days
                break

        # Calculate growth rate
        if self.model_type == 'linear':
            growth_rate = self.params['slope'] * 24  # Per day
        elif self.model_type == 'exponential':
            growth_rate = self.params['b'] * current_utilization * 24
        else:
            growth_rate = 0.0  # Varies with logistic

        # Recommended capacity (add 20% headroom)
        max_forecast = np.max(forecast.predictions)
        recommended_capacity = max_forecast * 1.2

        return CapacityForecast(
            current_utilization=current_utilization,
            forecast_utilization=forecast.predictions,
            capacity_limit=capacity_limit,
            time_to_limit=time_to_limit,
            recommended_capacity=recommended_capacity,
            growth_rate=growth_rate,
            model_type=self.model_type
        )


class EnsemblePredictor:
    """
    Ensemble forecasting combining multiple models.

    Uses weighted average based on historical accuracy.
    """

    def __init__(
        self,
        enable_arima: bool = True,
        enable_prophet: bool = True,
        enable_lstm: bool = True,
        enable_growth: bool = False
    ):
        self.enable_arima = enable_arima
        self.enable_prophet = enable_prophet
        self.enable_lstm = enable_lstm
        self.enable_growth = enable_growth

        self.arima = ARIMAPredictor() if enable_arima else None
        self.prophet = ProphetPredictor() if enable_prophet else None
        self.lstm = LSTMPredictor(epochs=30) if enable_lstm else None
        self.growth = GrowthModelPredictor() if enable_growth else None

        self.weights = {}

    def fit(self, ts_data: TimeSeriesData):
        """Train all enabled models."""
        print(f"Training ensemble predictor for {ts_data.metric_name}...")

        if self.arima:
            print("  Training ARIMA...")
            try:
                self.arima.fit(ts_data)
                self.weights['ARIMA'] = 1.0
            except Exception as e:
                print(f"  ARIMA training failed: {e}")
                self.arima = None

        if self.prophet:
            print("  Training Prophet...")
            try:
                self.prophet.fit(ts_data)
                self.weights['Prophet'] = 1.0
            except Exception as e:
                print(f"  Prophet training failed: {e}")
                self.prophet = None

        if self.lstm:
            print("  Training LSTM...")
            try:
                self.lstm.fit(ts_data)
                self.weights['LSTM'] = 1.0
            except Exception as e:
                print(f"  LSTM training failed: {e}")
                self.lstm = None

        if self.growth:
            print("  Training Growth Model...")
            try:
                self.growth.fit(ts_data)
                self.weights['Growth'] = 1.0
            except Exception as e:
                print(f"  Growth model training failed: {e}")
                self.growth = None

        # Normalize weights
        total = sum(self.weights.values())
        if total > 0:
            self.weights = {k: v/total for k, v in self.weights.items()}

        print(f"  Ensemble training complete! Weights: {self.weights}")

    def predict(
        self,
        steps: int,
        ts_data: TimeSeriesData,
        interval_seconds: float = 3600.0
    ) -> ForecastResult:
        """Generate ensemble forecast."""
        forecasts = []

        # Collect forecasts from each model
        if self.arima:
            try:
                forecast = self.arima.predict(steps, ts_data.timestamps[-1], interval_seconds)
                forecasts.append((forecast, self.weights.get('ARIMA', 0)))
            except:
                pass

        if self.prophet:
            try:
                forecast = self.prophet.predict(steps, ts_data.timestamps[-1], interval_seconds)
                forecasts.append((forecast, self.weights.get('Prophet', 0)))
            except:
                pass

        if self.lstm:
            try:
                forecast = self.lstm.predict(steps, ts_data, interval_seconds)
                forecasts.append((forecast, self.weights.get('LSTM', 0)))
            except:
                pass

        if self.growth:
            try:
                forecast = self.growth.predict(steps, ts_data, interval_seconds)
                forecasts.append((forecast, self.weights.get('Growth', 0)))
            except:
                pass

        if not forecasts:
            raise ValueError("No models produced forecasts")

        # Weighted average
        timestamps = forecasts[0][0].timestamps
        predictions = np.zeros(steps)
        lower_bounds = np.zeros(steps)
        upper_bounds = np.zeros(steps)

        for forecast, weight in forecasts:
            predictions += forecast.predictions * weight
            lower_bounds += forecast.lower_bound * weight
            upper_bounds += forecast.upper_bound * weight

        return ForecastResult(
            timestamps=timestamps,
            predictions=predictions,
            lower_bound=lower_bounds,
            upper_bound=upper_bounds,
            model_name="Ensemble",
            confidence_level=0.95,
            metadata={"weights": self.weights, "n_models": len(forecasts)}
        )


def calculate_accuracy_metrics(
    actual: np.ndarray,
    predicted: np.ndarray,
    model_name: str
) -> AccuracyMetrics:
    """
    Calculate forecast accuracy metrics.

    Args:
        actual: Actual values
        predicted: Predicted values
        model_name: Name of the model

    Returns:
        AccuracyMetrics with all metrics
    """
    # Mean Absolute Error
    mae = mean_absolute_error(actual, predicted)

    # Root Mean Squared Error
    rmse = np.sqrt(mean_squared_error(actual, predicted))

    # Mean Absolute Percentage Error
    mape = np.mean(np.abs((actual - predicted) / (actual + 1e-10))) * 100

    # Symmetric MAPE
    smape = np.mean(2.0 * np.abs(actual - predicted) / (np.abs(actual) + np.abs(predicted) + 1e-10)) * 100

    # R-squared
    ss_res = np.sum((actual - predicted) ** 2)
    ss_tot = np.sum((actual - np.mean(actual)) ** 2)
    r2 = 1 - (ss_res / (ss_tot + 1e-10))

    return AccuracyMetrics(
        mae=float(mae),
        rmse=float(rmse),
        mape=float(mape),
        smape=float(smape),
        r2_score=float(r2),
        forecast_horizon=len(actual),
        model_name=model_name
    )


def train_test_split_timeseries(
    ts_data: TimeSeriesData,
    test_size: int = 24
) -> Tuple[TimeSeriesData, TimeSeriesData]:
    """
    Split time series into train and test sets.

    Args:
        ts_data: Time series data
        test_size: Number of points for test set

    Returns:
        Tuple of (train_data, test_data)

    Raises:
        ValueError: If test_size is invalid
    """
    if test_size <= 0:
        raise ValueError(f"test_size must be positive, got {test_size}")
    if test_size >= len(ts_data.values):
        raise ValueError(f"test_size ({test_size}) must be less than data length ({len(ts_data.values)})")

    split_idx = len(ts_data.values) - test_size

    train_data = TimeSeriesData(
        timestamps=ts_data.timestamps[:split_idx],
        values=ts_data.values[:split_idx],
        metric_name=ts_data.metric_name,
        resource_hash=ts_data.resource_hash,
        unit=ts_data.unit
    )

    test_data = TimeSeriesData(
        timestamps=ts_data.timestamps[split_idx:],
        values=ts_data.values[split_idx:],
        metric_name=ts_data.metric_name,
        resource_hash=ts_data.resource_hash,
        unit=ts_data.unit
    )

    return train_data, test_data
