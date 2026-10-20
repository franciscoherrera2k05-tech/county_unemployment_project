"""Unit test: the config file loads and the state table is complete.

In: config/settings.yaml and the static state table. Out: pass/fail.
"""
from src.utils.helpers import load_config, state_fips_list


def test_config_and_states():
    cfg = load_config()
    assert cfg["target"]["threshold_pp"] == 0.5
    assert len(state_fips_list("all")) == 51          # 50 states + DC
    assert state_fips_list(["WV", "VA", "OH"]) == ["39", "51", "54"]
