# Rainfall loss screening: BoM / ARR 2026

**Owner:** hydrology implementation
**Started:** 2026-11-16
**Updated:** 2026-11-17
**Status:** Draft implementation; reference-engine, genuine-data, and installed-wheel acceptance OPEN.
**Next review:** 2026-11-23

## Scope and ownership

- Parent [#317](https://github.com/Chain-Frost/ryan-tools/issues/317)
- Implementation [draft PR #318](https://github.com/Chain-Frost/ryan-tools/pull/318)
- Library: `ryan_library/functions/hydrology/rainfall_loss_screening.py`
- Copyable wrapper: `ryan-scripts/hydrology-python/screen_rainfall_losses.py`
- Use existing PyTUFLOW ARR 2026 API/engine/temporal patterns for live and saved Data Hub data.
- Use existing `runoff_routing.bom_ifd` for BoM 2016-methodology CSV (in-development optional import; not a new mandatory cross-dependency).
- No model engine execution, routed runoff equivalence or automatic input pruning.

## Current behaviour

- IL/CL sequential rainfall-excess kernel and IL + runoff coefficient, synthetic tests.
- Preburst treatment: ARR recommended *burst* IL or *complete storm* with explicit preburst pattern and full storm IL. For BoM bursts a manually supplied preburst depth may be subtracted from a specified storm IL once, but overflow fails.
- AEP selection: all, editable common set, or explicit exact source labels; optional duration filtering.
- Console table with selected TP counts and zero/positive/unresolved verdicts; optional exclusive-create CSV export.
- User-provided AEP/duration IL/CL or IL+coefficient JSON overrides.
- Read-only sources; optional Data Hub and temporal pattern downloads.
- ARR Data Hub JSON and independently saved point/areal TP increments CSVs override their remote sources.
- Strict `OFFLINE=True` / `--offline` rejects missing local JSON/point CSV and any advertised but missing areal TP CSV, without network fallback; for BoM it requires local patterns or explicit no-pattern screening.
- Saved Data Hub JSON works without coordinates; source attribution and catchment matching remain the operator's responsibility.

## Source evidence and blockers

- Uploaded user BoM export: `depths_hd4_temp_all_design.csv`; coordinates **-23.08632, 119.2755**; issued **27 October 2026**; 29 durations x 18 probability columns. The actual file is an acceptance input, not a public committed fixture.
- ARR Data Hub documents 2026 JSON point API with `RecIFD`, storm losses, preburst ratios and temporal patterns. Published `pytuflow>=1.2` is not itself proof of the presence of the 2026 `pytuflow.arr` implementation. Test an ARR-capable installed wheel.
- Synthetic kernel 16 focused tests previously passed on Python 3.13.5. New wrapper tests and changes require exact-head GitHub CI, strict Pyright and Python 3.14/WIN installed-wheel acceptance.
- Live network fetch from coordinates in this work environment was not possible. Avoid claiming that the site-specific ARR 2026 response or TP ZIP has been downloaded or qualified.
- Verify BoM parser from its actual package against the uploaded file and compare sample cells/source coordinates/hashes.
- Obtain authentic Data Hub 2026 JSON/point TP and areal TP for the site; check source/probability units, preburst ratio choice, and selected IL/CL.
- Native RORBwin 6.52 / TUFLOW loss semantics, IL crossing timestep, temporal-pattern completeness and runoff pruning parity remain **unproven**.

## 17 November 2026: ARF policy cautions and source PB ratio

- Verified Tony Ladson's ARR interpolation correction against PyTUFLOW:
  Eq. 2.4.3 for 12–24-hour durations and Eq. 2.4.4 for area
  interpolation between 1 and 10 km². No ARF equation was replaced.
- Added grouped warnings for >50%-AEP unity fallback (including
  63.2%), under-30-minute extrapolation, unsupported area/duration
  ranges and zero ARF potentially floored from a negative result.
  Console ARF factors are flagged with `*`; CSV retains per-event
  caution codes without changing DROP/KEEP semantics.
- The selected ARR preburst ratio is read using PyTUFLOW's existing
  ratio selection/interpolation rather than inferred from applied
  preburst depth. Baseline and climate-change rows may share the
  ratio while reporting different applied PB depths. BoM's manual
  preburst ratio remains explicitly inferred, with CSV provenance.
- Focused regression tests added. Native RORBwin/TUFLOW acceptance
  remains unassessed; do not use policy-flagged DROP events as
  automatic exclusions without engineering review.
- **Validation pending:** exact-head Windows Python 3.14 focused tests,
  Ruff, modified-file strict Pyright and required installed-wrapper
  checks. Preserve draft PR #318; no merge or release authorized.

## 17 November 2026: post-Codex corrections and approved VS Code setting removal

- Codex on PR #318 `6c16518` identified two valid screening gaps:
  applying full-storm user IL after a negligible preburst was omitted
  could yield a false DROP, and hybrid BoM downloads ignored a supplied
  local areal TP CSV. Both paths now have targeted regression tests.
- Complete-storm override conversion subtracts only the omitted original
  preburst depth from full-storm IL when the source ratio is available,
  below the PyTUFLOW 1% omission threshold and within IL.
  Unknown/unsafe conversions are UNRESOLVED. The main-burst-only
  override contract remains unchanged.
- The owner explicitly required `python-envs.defaultEnvManager` to remain
  deleted from the TUFLOW VS Code settings template. The template is
  valid strict JSON without the field, and the project-setup test now
  verifies its absence, not the previous presence.
- The prior review finding was acknowledged and marked resolved by
  the owner, but exact-head tests and re-review must be checked before
  a merge.

## Acceptance and next actions

1. Check draft PR #318 at final head: Ruff, strict Pyright, focused tests, wheel/installed wrapper, bundle manifest and `--help`. The 2026-11-16 head previously failed hosted tests because the installed-script evidence ledger lacked the new selected wrapper; the ledger is updated in the same PR.
2. Verify offline/no-network acceptance with saved ARR JSON and both TP CSVs; test missing point/areal sources fail closed without accessing live URLs.
3. Verify installed copied wrapper, sample BoM file, and live ARR 2026 request at **-23.08632, 119.2755** on a network-enabled Windows Python 3.14 environment.
4. Compare preburst/IL/CL/event sequence against an accepted native reference before using the tool to suppress production design simulations.
5. Keep the PR in draft and do not merge or trigger a release until explicit acceptance.

`docs/RAIN_LOSS_SCREENING.md` owns user-facing options and limitations; do not copy its full manual here.
