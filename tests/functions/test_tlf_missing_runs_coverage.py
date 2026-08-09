"""Coverage tests for tlf_missing_runs.py."""

import pandas as pd

from ryan_library.functions.tlf_missing_runs import summarize_for_cli, EXPECTED_TPS


class TestTLFMissingRunsCoverage:
    def test_summarize_no_sets(self):
        # Trigger `result.no_sets` logic
        # We need a DataFrame where no single (AEP, Duration) group has all TPs
        df = pd.DataFrame(
            {"trim_run_code": ["EXG"], "AEP": ["1%"], "Duration": ["1h"], "TP": ["TP01"]}  # Only 1 TP out of 10
        )
        text, table = summarize_for_cli(df)
        assert "No complete (AEP, Duration) sets found" in text
        assert not table.empty
        assert table["section"].iloc[0] == "notice"

    def test_summarize_rollups_and_missing_tps(self):
        # We need to craft a DataFrame where we *do* have at least one complete set
        # so `result.no_sets` is False, but also we have:
        # 1. An AEP missing all durations
        # 2. A duration missing all AEPs
        # 3. A cell missing all TPs
        # 4. A cell missing >= 6 TPs
        # 5. A cell missing < 6 TPs

        rows = []

        # Complete set (to bypass no_sets)
        for tp in EXPECTED_TPS:
            rows.append({"trim_run_code": "EXG", "AEP": "1%", "Duration": "1h", "TP": tp})

        # A cell missing < 6 TPs (missing TP10 only)
        for tp in list(EXPECTED_TPS)[:-1]:
            rows.append({"trim_run_code": "EXG", "AEP": "1%", "Duration": "2h", "TP": tp})

        # A cell missing >= 6 TPs (missing TP05 to TP10)
        for tp in list(EXPECTED_TPS)[:4]:
            rows.append({"trim_run_code": "EXG", "AEP": "2%", "Duration": "1h", "TP": tp})

        df = pd.DataFrame(rows)

        # Now artificially add to aeplist and durlist without adding data,
        # Wait, `groupby` over `work` only uses present AEPs and Durations.
        # If an AEP has no rows for any duration, it won't appear in `work`.
        # But wait!
        # If we just create a cell with no TPs? We can't, `drop_duplicates` and `isin(EXPECTED_TPS)`
        # means a row must have a valid TP to exist. So a cell missing all TPs means there's no data for that (AEP, Duration).
        # But `aeplist` and `durlist` are just the cross product of all AEPs and Durations present for that trim_run_code!
        # Because we have AEPs [1%, 2%] and Durations [1h, 2h], the cross product is:
        # (1%, 1h) -> complete
        # (1%, 2h) -> missing < 6
        # (2%, 1h) -> missing >= 6
        # (2%, 2h) -> completely missing! No rows in `df`.

        text, table = summarize_for_cli(df)

        assert "AEP 2%, Duration 2h: missing all TP" in text
        assert "missing 6 TP (not listed)" in text
        assert "missing TP10" in text
