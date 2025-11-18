# Sample OTLP File - Status and Recommendations

## Current Status

 **We have:** `tests/fixtures/sample_otlp.json`
- Basic OTLP format example
- Contains: gauge metric only
- Shows: resource attributes, scope metadata, single data point

## Current Sample Contents

```json
{
  "resourceMetrics": [{
    "resource": {
      "attributes": [
        {"key": "service.name", "value": {"stringValue": "web-server"}},
        {"key": "host.name", "value": {"stringValue": "server01"}}
      ]
    },
    "scopeMetrics": [{
      "scope": {"name": "CPU_Usage", "version": "1.0"},
      "metrics": [{
        "name": "cpu.usage",
        "unit": "percent",
        "gauge": {
          "dataPoints": [{
            "timeUnixNano": 1699123456000000000,
            "asDouble": 45.2
          }]
        }
      }]
    }]
  }]
}
```

## Recommendation: Expand Sample File

The current sample is minimal. Consider creating a comprehensive example that includes:

### Missing Metric Types

1. **Sum Metrics** (counters)
   ```json
   "sum": {
     "dataPoints": [{
       "timeUnixNano": 1699123456000000000,
       "asInt": 1234,
       "isMonotonic": true
     }]
   }
   ```

2. **Histogram Metrics** (distributions)
   ```json
   "histogram": {
     "dataPoints": [{
       "timeUnixNano": 1699123456000000000,
       "count": 100,
       "sum": 5000,
       "bucketCounts": [10, 20, 30, 40],
       "explicitBounds": [1.0, 2.0, 5.0, 10.0]
     }]
   }
   ```

3. **Summary Metrics** (percentiles)
   ```json
   "summary": {
     "dataPoints": [{
       "timeUnixNano": 1699123456000000000,
       "count": 100,
       "sum": 5000,
       "quantileValues": [
         {"quantile": 0.5, "value": 45.0},
         {"quantile": 0.95, "value": 98.0},
         {"quantile": 0.99, "value": 123.0}
       ]
     }]
   }
   ```

### Missing Data Point Types

- **Integer values** (`asInt` instead of `asDouble`)
- **Multiple data points** per metric
- **Attributes on data points**
- **Exemplars** (trace sampling)

### Missing Resource Types

- Multiple resources in single payload
- Different attribute combinations:
  - `service.name`, `service.version`, `service.namespace`
  - `host.name`, `host.id`, `host.type`
  - `cloud.provider`, `cloud.region`, `cloud.account.id`

### Missing Edge Cases

- Empty metrics arrays
- Missing optional fields (unit, description, version)
- Multiple scopes per resource
- Multiple metrics per scope

## Proposed File Structure

Create multiple sample files for different scenarios:

```
tests/fixtures/
├── sample_otlp.json              # Current basic example (keep)
├── sample_otlp_comprehensive.json # All metric types
├── sample_otlp_logicmonitor.json  # Real LM data structure
├── sample_otlp_multi_resource.json # Multiple resources
└── sample_otlp_edge_cases.json    # Edge cases and errors
```

## Benefits of Comprehensive Samples

1. **Better Testing**
   - Test all metric type parsers
   - Verify edge case handling
   - Validate real-world data structures

2. **Documentation**
   - Show developers what data looks like
   - Provide examples for API testing
   - Reference for OTLP spec compliance

3. **Integration Testing**
   - Use for end-to-end tests
   - Validate complete pipeline
   - Test query endpoints with realistic data

4. **Development**
   - Quick manual testing via curl
   - Reproduce production issues locally
   - Validate parser changes

## Action Items

**Priority: Medium** (Can be done anytime, helpful but not blocking)

- [ ] Create `sample_otlp_comprehensive.json` with all metric types
- [ ] Create `sample_otlp_logicmonitor.json` based on real LM OTLP output
- [ ] Add samples to test suite for parser validation
- [ ] Document OTLP structure in main README

## References

- [OTLP Specification](https://opentelemetry.io/docs/specs/otlp/)
- [Metrics Data Model](https://opentelemetry.io/docs/specs/otel/metrics/data-model/)
- LogicMonitor OTLP export documentation (if available)

---

**Status:** Nice-to-have improvement
**Impact:** Improves testing and documentation
**Effort:** ~30-60 minutes to create comprehensive examples
**Created:** 2025-11-14
