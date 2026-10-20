from src.utils.config import load_config, resolve_states
from src.utils.states import STATE_FIPS, state_fips_for


def test_config_defaults():
    cfg = load_config()
    assert cfg["project"]["database"] == "unemployment"
    assert cfg["target"]["horizon_months"] == 3
    assert cfg["target"]["threshold_pp"] == 0.5
    assert str(cfg["data"]["panel_start"]) == "2015-01-01"


def test_state_table():
    assert len(STATE_FIPS) == 51
    assert all(len(k) == 2 for k in STATE_FIPS)
    assert state_fips_for(["WV", "VA", "OH"]) == ["39", "51", "54"]
    assert resolve_states("all") is None
    assert resolve_states(["wv"]) == ["WV"]
