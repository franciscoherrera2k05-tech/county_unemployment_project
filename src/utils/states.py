"""Static state FIPS table (50 states + DC = 51 rows). Used for ref.county.state_abbr."""
STATE_FIPS: dict[str, str] = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO", "09": "CT",
    "10": "DE", "11": "DC", "12": "FL", "13": "GA", "15": "HI", "16": "ID", "17": "IL",
    "18": "IN", "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME", "24": "MD",
    "25": "MA", "26": "MI", "27": "MN", "28": "MS", "29": "MO", "30": "MT", "31": "NE",
    "32": "NV", "33": "NH", "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC", "46": "SD",
    "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA", "54": "WV",
    "55": "WI", "56": "WY",
}
ABBR_TO_FIPS: dict[str, str] = {v: k for k, v in STATE_FIPS.items()}
assert len(STATE_FIPS) == 51


def state_fips_for(abbrs: list[str] | None) -> list[str]:
    if abbrs is None:
        return sorted(STATE_FIPS)
    bad = [a for a in abbrs if a not in ABBR_TO_FIPS]
    if bad:
        raise ValueError(f"Unknown state abbreviations: {bad}")
    return sorted(ABBR_TO_FIPS[a] for a in abbrs)


def values_sql() -> str:
    """SQL VALUES list for the state table."""
    return ", ".join(f"('{f}', '{a}')" for f, a in sorted(STATE_FIPS.items()))
