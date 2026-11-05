"""CODING_SYSTEM -> organization name and maakunta (filters/filter_minimal.get_coding_map)."""

import pandas as pd
import pytest

from kanta import config
from kanta.engine import reference_data
from kanta.engine.filters.filter_minimal import coding_system_org_code, get_coding_map

# a few rows in the format scripts/sote/data/coding_system_map.py writes (only the columns the engine reads
# matter; the rest are left empty)
TABLE_COLS = ["CODING_SYSTEM", "CNT", "engine_org", "code", "how", "sote_id", "org_name", "unit_name", "sector",
              "municipality", "maakunta", "hva", "region_reason"]
TABLE = [
    # CODING_SYSTEM, CNT, code, org_name, maakunta
    ("NA", "100", "", "", ""),
    ("1.2.246.537.6.3.2006", "90", "", "", ""),
    ("1.2.246.10.32213166.6.3.2024", "50", "32213166", "Pohjois-Savon hyvinvointialue", "Pohjois-Savo"),
    ("1.2.246.10.32213166.6.3.2025", "60", "32213166", "Pohjois-Savon hyvinvointialue", "Pohjois-Savo"),
    ("1.2.246.10.23925196.6.3.2016", "40", "23925196", "Fimlab Laboratoriot Oy", ""),
    ("1.2.246.537.10.3575029.12.2.10.1102012566.6.17", "30", "2012566", "HELSINGIN KAUPUNKI", "Uusimaa"),
    ("1.2.246.537.10.3575029.12.2.10.29872365.6.17", "20", "CGI:29872365", "", ""),
    ("1.2.246.10.1322394.10.20.6.3.2018", "10", "1322394", "Euran kunta", "Satakunta"),
]


@pytest.fixture(autouse=True)
def coding_table(tmp_path, monkeypatch):
    path = tmp_path / "coding_system_map.tsv"
    rows = [{"CODING_SYSTEM": cs, "CNT": n, "code": code, "org_name": name, "maakunta": mk}
            for cs, n, code, name, mk in TABLE]
    pd.DataFrame(rows, columns=TABLE_COLS).fillna("").to_csv(path, sep="\t", index=False)
    monkeypatch.setattr(config, "CODING_SYSTEM_MAP_FILE", path)
    monkeypatch.setattr(reference_data, "_CACHE_DIR", None)
    reference_data.get_coding_system_map.cache_clear()
    yield
    reference_data.get_coding_system_map.cache_clear()


@pytest.mark.parametrize("coding_system, code", [
    ("1.2.246.10.32213166.6.3.2024", "32213166"),
    ("1.2.246.537.10.15675350.6.3.2019", "15675350"),
    ("1.2.246.10.0132239.10.201022", "1322394"),  # old form: check digit added, leading zero dropped
    ("1.2.246.537.10.3575029.12.2.10.1102012566.6.17", "2012566"),  # CGI customer "11" + "02012566"
    ("1.2.246.537.10.3575029.12.2.10.1119025988.6.17", "19025988"),
    ("1.2.246.537.10.3575029.12.2.10.29872365.6.17", None),  # CGI customer that doesn't decode
    ("1.2.246.537.6.3.2006", None),
    ("NA", None),
])
def test_org_code(coding_system, code):
    assert coding_system_org_code(coding_system) == code


def test_get_coding_map():
    cases = {
        "1.2.246.10.32213166.6.3.2024": ("Pohjois-Savon hyvinvointialue", "Pohjois-Savo"),  # exact OID
        "1.2.246.10.32213166.6.3.2031": ("Pohjois-Savon hyvinvointialue", "Pohjois-Savo"),  # unseen year: by code
        "1.2.246.10.23925196.6.3.2016": ("Fimlab Laboratoriot Oy", "NA"),  # private: no maakunta
        "1.2.246.537.10.3575029.12.2.10.1102012566.6.17": ("HELSINGIN KAUPUNKI", "Uusimaa"),
        "1.2.246.10.0132239.10.201022": ("Euran kunta", "Satakunta"),  # old form, by code
        "1.2.246.537.10.3575029.12.2.10.29872365.6.17": ("NA", "NA"),  # listed, but no organization
        "1.2.246.10.99999999.6.3.2024": ("NA", "NA"),  # unknown organization
        "1.2.246.537.6.3.2006": ("NA", "NA"),
        "NA": ("NA", "NA"),
    }
    df = pd.DataFrame({"CODING_SYSTEM": pd.Series([*cases, None], dtype="string")})
    out = get_coding_map(df)
    assert out["CODING_SYSTEM"].tolist()[:-1] == list(cases)  # OID kept as is
    got = list(zip(out["CODING_SYSTEM_MAP"], out["CODING_SYSTEM_MAAKUNTA"]))
    assert got == [*cases.values(), ("NA", "NA")]


def test_get_coding_map_empty():
    out = get_coding_map(pd.DataFrame({"CODING_SYSTEM": pd.Series([], dtype="string")}))
    assert {"CODING_SYSTEM_MAP", "CODING_SYSTEM_MAAKUNTA"} <= set(out.columns) and out.empty


def test_coding_system_map_release_columns():
    """The organization name goes to the release (CODING_SYSTEM_ORG); the maakunta stays engine-only."""
    assert config.OUTPUT_COLUMNS["CODING_SYSTEM_MAP"][:2] == (True, "CODING_SYSTEM_ORG")
    assert config.OUTPUT_COLUMNS["CODING_SYSTEM_MAAKUNTA"][0] is False
