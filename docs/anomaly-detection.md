# Anomaly Detection

Section 22 is explicit: **do not claim machine learning that isn't
implemented.** CyberStream's anomaly detection (`shared/scoring.py`,
`BaselineStore`) is a rolling-window z-score against a per-entity
baseline — nothing more, nothing dressed up as more.

## How it works
`BaselineStore.evaluate(key, observed)`:
1. Looks up (or creates) a `Baseline` for `key` (e.g. a hostname).
2. If fewer than 5 samples exist, returns `None` — there isn't enough
   history to say anything is "abnormal" yet.
3. Otherwise computes `mean` and population `stddev` of up to the last
   200 samples, then `z = (observed - mean) / stddev`.
4. If `|z| >= 3` and the observation is *above* the mean (a spike, not a
   lull), returns an anomaly record with the observed value, baseline
   mean/stddev, z-score, and percent deviation.
5. Regardless of outcome, the new observation is added to the baseline
   (so today's spike becomes part of tomorrow's baseline once it's no
   longer novel — Section 23's "baseline system").

## Why z-score, and not something fancier?
Z-score against a rolling mean/stddev is fully explainable — an analyst
can see exactly why an alert fired (`observed=850, baseline_mean=100,
z=8.2`) with no model internals to audit. Section 22's own worked example
(100 events/min baseline, 850 observed, +750% deviation) is precisely
this calculation. A future ML extension (Section 50) could replace or
augment this, but the first version intentionally does not.

## Baselines maintained
Per Section 23: host, user, source IP, destination, and event type are
all valid baseline keys — `BaselineStore` is generic over the string key
you pass it; the streaming job and local runner currently key by
hostname for event-volume anomalies, but the same mechanism applies to
any of the other entities.
