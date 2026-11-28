# TUFLOW STEPPED rainfall to RORB direct-depth STM

| Field | Value |
| --- | --- |
| Status | Public producer CSV and installed candidate codec checked on Windows. Paired native reference and released codec pending. |
| Owner | #118 maintainer |
| Updated | 2026-11-28 |
| Next review | 2026-12-05 |
| Delivery | [Existing PR #118](https://github.com/Chain-Frost/ryan-tools/pull/118), branch `fix/rorb-stm-rainfall-and-library-integration`. Public conversion evidence uses reconciled implementation `97122284`. |

## Public producer data

Used the original [PyTuflow ARR 60-minute export](https://github.com/TUFLOW-Support/PyTuflow/blob/f2b1ec8e80d6ddf02fdd49c409eda5a3fca2687f/docs/assets/tables/SEQ_CatchmentA_RF_01p60m.csv)
and its [preburst/climate companion](https://github.com/TUFLOW-Support/PyTuflow/blob/f2b1ec8e80d6ddf02fdd49c409eda5a3fca2687f/docs/assets/tables/SEQ_CatchmentA_CC_RF_01p60m.csv).
Both were read unedited in disposable local folders on Windows 10 / Python 3.14.6.

- The single-file CLI selected TP07. The critical-storm table selected `SEQ_CatchmentA,01p,060m,TP07` with input
  template `{model}_RF_{aep}{duration_minutes}m.csv` and column template `{tp}`. Both used model title `Public`.
  Both produced byte-identical `Storm_SEQ_CatchmentA_01p_60m_tp07.stm` files. Dry runs created no output directories.
- Compared all 12 interval depths against the source column under the existing ten-significant-digit writer policy.
  The zero-time placeholder and the extra dry row at 1.0833333333333333 h were omitted. The retained five-minute
  intervals end at 1 h. The STM declares timestep 0.08333333333 h and horizon 200 increments. The first and last
  depths are 11.63423749 and 8.431672118 mm. Source total 105.0021434385885957 mm, written total 105.002143431 mm.
  The largest absolute change from formatting one depth was 0.000000004083378 mm.
- On the 2.5-hour series in the companion file, automatic mode rejected the mismatch with the 60-minute filename.
  Explicitly dropping the verified extra terminal zero retained all 30 intervals, including all 18 dry TP07 bins.
  The TP07_2050_SSP2 series retained 30 intervals totalling 127.0319071127 mm.
- Repeating both modes refused existing outputs and preserved their bytes. A batch with a valid row followed by
  an invalid TP produced no output. An output template escaping the root was also refused.
- The independent public [TUFLOW M06 tutorial](https://wiki.tuflow.com/Tutorial_M06) has a different input contract:
  its rainfall station file uses a plain `time` header and prints the ten-minute clock as 0.17, 0.33, 0.50, etc.
  The existing converter rejects its header. The successful tests above used original PyTuflow exports.

## Verification and outstanding work

The converter, orchestration and script-inventory tests passed locally (58 passed, six skipped).
The reconciled branch needed the two new repository-only paths added to the per-file inventory ledger.
The PR holds hosted results for the final commit. No script installation or MCP selection changed.

[PyRORB #252](https://github.com/Chain-Frost/pyrorb/pull/252) at `fbe0e3c9` passed all 24 direct-STM tests in
an isolated Python 3.14.6 wheel installation outside the checkout. The installed codec read and round-tripped the
public outputs above. For the 12-bin example, canonical text matched after normalizing Windows line endings.

The installed RORBwin 6.52 executable is available. A matched native reference for these public rainfall inputs was
unavailable, so peak flow, peak time, recession and adequacy of the 200-increment horizon await a paired native run.
Retain the existing writer until a qualified released wheel contains the direct-depth API, then repeat producer and
installed-wrapper checks on the existing branch. [Consumer tracking #346](https://github.com/Chain-Frost/ryan-tools/issues/346)
