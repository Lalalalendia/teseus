# Theseus Survivor Lab report

- Request: `example-real-gap`
- Result: `result-74a846797ae3731ef64fea6b`
- Classification: **real_test_gap**
- Confidence: `0.93`

## Why it survived

Execution is valid and the mutant changes reachable behavior, but the current tests do not detect it.

## Evidence

- `mutant:209e93bc414d`
- `selection:77bad1fd0c22`
- `context:b44881e3b978`

## Findings

### Reachable behavior changed without a kill

The mutant changes an observed behavior, yet no selected test failed on it.

Severity: `info`; evidence: `mutant:209e93bc414d`, `selection:77bad1fd0c22`, `context:b44881e3b978`

## Possible equivalent behavior

- not established

## Missing observations

- None recorded

## Suggested inputs and assertions

### Exercise the comparison boundary

The mutation changes a comparison outcome. Use values immediately below, at, and immediately above the boundary.

Inputs:
- 2
- 3
- 4
Assertions:
- assert the exact branch or returned value
- assert the boundary and neighboring cases separately

## Candidate test proposal

### `test_survivor_boundary_input_m_example_001`

The mutation changes a comparison outcome. Use values immediately below, at, and immediately above the boundary.

Arrangement:
- 2
- 3
- 4
Action:
- The branch and returned behavior must change at the exact comparison boundary.
Assertions:
- assert the exact branch or returned value
- assert the boundary and neighboring cases separately

## Validation plan

### Original
- Candidate passes on original: `pytest <candidate-nodeid>`

### Mutant
- Candidate fails on target mutant: `pytest <candidate-nodeid>`

### Regression
- Related tests remain green: `pytest <related-tests>`

### Stability
- Repeat original candidate: `pytest <candidate-nodeid> --count=<n>`
- Check workspace independence: `review candidate fixture paths`
- Check assertion stability: `review candidate assertions`

## Warnings

- None
