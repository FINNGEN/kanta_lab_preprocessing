"""Reference limits from text: the key, the verdict, the table loader and the engine fill (filters/ref_range.py)."""

import pandas as pd
import pyarrow as pa
import pytest

from kanta import config
from kanta.engine import reference_data
from kanta.engine.filters import ref_range
from kanta.engine.reference_data import approve, ref_key

TABLE_COLS = ["test_name", "ref_text", "status", "flags", "lower", "upper", "use", "lower_override", "upper_override", "NOTES"]
TABLE = [
    # test_name, ref_text, status, flags, lower, upper, use, lower_override, upper_override
    ("b-hb", "117-155", "range", "", "117.0", "155.0", "", "", ""),
    ("p-crp", "<4", "upper", "", "", "4.0", "", "", ""),
    ("p-tt-inr", "2-3/Varfariinihoito", "skip_therapeutic", "", "2.0", "3.0", "", "", ""),
    ("p-pct", "<0.05", "upper", "value_off", "", "0.05", "yes", "", ""),
    ("du-prot", "<0.1/kaikki", "upper", "value_off", "", "0.1", "", "", ""),
    ("p-probnp", "<125(kts.ohjek)", "unparsed", "unparsed", "", "", "", "", "125"),
    ("fp-kol-hdl", "tavoite: miehet >1,naiset >1.2", "unparsed", "unparsed", "", "", "no", "", ""),
    ("b-hb", "nainen", "skip_label", "", "", "", "", "", ""),
    ("b-leuk", "3.4-8.2", "range", "", "3.4", "8.2", "", "", ""),
    ("b-trom", "150-360", "range", "", "150.0", "360.0", "no", "", ""),
]


def table(rows=TABLE) -> pd.DataFrame:
    return pd.DataFrame([r + ("",) for r in rows], columns=TABLE_COLS)


@pytest.fixture
def limits_file(tmp_path, monkeypatch):
    """Point the engine at a synthetic curated table, with no pickle cache."""

    def write(rows=TABLE):
        path = tmp_path / "ref_text_limits.tsv"
        table(rows).to_csv(path, sep="\t", index=False)
        monkeypatch.setattr(config, "REF_TEXT_LIMITS_FILE", path)
        reference_data.get_text_limits.cache_clear()
        return path

    monkeypatch.setattr(reference_data, "_CACHE_DIR", None)
    yield write
    reference_data.get_text_limits.cache_clear()


def chunk(rows) -> pd.DataFrame:
    """Engine-like chunk: Arrow-backed strings, "NA" for missing (REFERENCE_RANGE_TEXT may also be null)."""
    cols = ["TEST_NAME_ABBREVIATION", "REFERENCE_RANGE_TEXT", "REFERENCE_RANGE_GROUP",
            "REFERENCE_RANGE_LOWER_VALUE", "REFERENCE_RANGE_UPPER_VALUE"]
    df = pd.DataFrame(rows, columns=cols)
    return df.astype(pd.ArrowDtype(pa.string()))


def test_ref_key():
    assert ref_key("117-155") == "117-155"
    assert ref_key("  <3\t ") == "<3"
    assert ref_key("<3\n") == "<3"
    assert ref_key('"<3"') == "<3"
    assert ref_key("<3\r\n/kaikki") == "<3 /kaikki"
    assert ref_key("0,5-1\t\t2") == "0,5-1 2"
    assert ref_key("a  b") == "a  b"  # inner spaces kept
    for empty in (None, pd.NA, float("nan"), "", " ", "NA", '"NA"'):
        assert ref_key(empty) is None


def test_approve():
    d = approve(table()).set_index(["test_name", "ref_text"])
    verdict = d[["approved", "reason"]].apply(tuple, axis=1).to_dict()
    assert verdict == {
        ("b-hb", "117-155"): ("yes", "parsed"),
        ("p-crp", "<4"): ("yes", "parsed"),
        ("p-tt-inr", "2-3/Varfariinihoito"): ("no", "skip_therapeutic"),
        ("p-pct", "<0.05"): ("yes", "use=yes"),
        ("du-prot", "<0.1/kaikki"): ("no", "value_off"),
        ("p-probnp", "<125(kts.ohjek)"): ("yes", "override"),
        ("fp-kol-hdl", "tavoite: miehet >1,naiset >1.2"): ("no", "use=no"),
        ("b-hb", "nainen"): ("no", "skip_label"),
        ("b-leuk", "3.4-8.2"): ("yes", "parsed"),
        ("b-trom", "150-360"): ("no", "use=no"),
    }
    assert d.loc[("p-probnp", "<125(kts.ohjek)"), "final_upper"] == 125
    assert pd.isna(d.loc[("p-crp", "<4"), "final_lower"])
    assert pd.isna(d.loc[("du-prot", "<0.1/kaikki"), "final_upper"])  # final limits only on approved pairs


def test_approve_use_yes_without_limits():
    rows = [("p-gluk", "4.0", "unparsed", "unparsed", "", "", "yes", "", "")]
    d = approve(table(rows))
    assert d.loc[0, "approved"] == "no" and d.loc[0, "reason"] == "use=yes, no limits"


def test_override_replaces_one_side():
    rows = [("x", "10-20", "range", "", "10.0", "20.0", "", "", "25")]
    d = approve(table(rows))
    assert (d.loc[0, "final_lower"], d.loc[0, "final_upper"]) == (10, 25)


def test_inject(limits_file):
    limits_file()
    df = chunk([
        ("b-hb", "117-155", "NA", "NA", "NA"),               # 0 range from text
        ("b-hb", '"117-155"\n', "NA", "NA", "NA"),           # 1 same key after ref_key
        ("b-hb", "117-155", "NA", "120", "NA"),              # 2 lab-sent limit: never touched
        ("p-crp", "<4", "NA", "NA", "NA"),                   # 3 one-sided
        ("p-tt-inr", "2-3/Varfariinihoito", "NA", "NA", "NA"),  # 4 not approved
        ("p-pct", "<0.05", "NA", "NA", "NA"),                # 5 whitelisted
        ("du-prot", "<0.1/kaikki", "NA", "NA", "NA"),        # 6 failed check, undecided
        ("p-probnp", "<125(kts.ohjek)", "NA", "NA", "NA"),   # 7 override
        ("b-leuk", None, "3.4-8.2", "NA", "NA"),             # 8 no text: the group is the key
        ("b-leuk", "4-9", "3.4-8.2", "NA", "NA"),            # 9 a text not in the table: the group is not used
        ("e-x", "117-155", "NA", "NA", "NA"),                # 10 other test, same text
        ("b-hb", "NA", "NA", "NA", "NA"),                    # 11 nothing
        ("b-trom", "150-360", "NA", "NA", "NA"),             # 12 blacklisted
    ])
    out = ref_range.inject_text_limits(df)
    got = list(zip(out["REFERENCE_RANGE_LOWER_VALUE"], out["REFERENCE_RANGE_UPPER_VALUE"], out["REFERENCE_RANGE_SOURCE"]))
    assert got == [
        ("117", "155", "text"),
        ("117", "155", "text"),
        ("120", "NA", "num"),
        ("NA", "4", "text"),
        ("NA", "NA", "NA"),
        ("NA", "0.05", "text"),
        ("NA", "NA", "NA"),
        ("NA", "125", "text"),
        ("3.4", "8.2", "text"),
        ("NA", "NA", "NA"),
        ("NA", "NA", "NA"),
        ("NA", "NA", "NA"),
        ("NA", "NA", "NA"),
    ]
    assert out["REFERENCE_RANGE_TEXT"].iloc[1] == '"117-155"\n'  # the text itself is kept as it was


def test_inject_empty_chunk(limits_file):
    limits_file()
    out = ref_range.inject_text_limits(chunk([]))
    assert len(out) == 0 and "REFERENCE_RANGE_SOURCE" in out


@pytest.mark.parametrize("rows, message", [
    (TABLE + [("b-hb", "117-155", "range", "", "1.0", "2.0", "", "", "")], "duplicated"),
    ([("b-hb", "117-155 ", "range", "", "117.0", "155.0", "", "", "")], "not ref_key"),
    ([("b-hb", "117-155", "range", "", "117.0", "155.0", "", "200", "")], "final_lower > final_upper"),
])
def test_table_checks(limits_file, rows, message):
    limits_file(rows)
    with pytest.raises(ValueError, match=message):
        reference_data.get_text_limits()


def read(path) -> pd.DataFrame:
    return pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False, quoting=3, escapechar="\\")


def test_verdict_written_back(limits_file):
    path = limits_file()  # a table without a verdict: it is added, before the manual columns
    reference_data.get_text_limits()
    cols = read(path).columns.tolist()
    assert cols[cols.index("use") - 4: cols.index("use")] == ["approved", "reason", "final_lower", "final_upper"]

    synced = path.read_text()  # in sync: loading again leaves the file as it is
    reference_data.get_text_limits.cache_clear()
    reference_data.get_text_limits()
    assert path.read_text() == synced

    d = read(path)  # a hand edit of `use` leaves the stored verdict stale: the next load fixes the file
    d.loc[(d.test_name == "b-hb") & (d.ref_text == "117-155"), "use"] = "no"
    d.to_csv(path, sep="\t", index=False, quoting=3, escapechar="\\")
    reference_data.get_text_limits.cache_clear()
    assert ("b-hb", "117-155") not in reference_data.get_text_limits().index
    row = read(path).set_index(["test_name", "ref_text"]).loc[("b-hb", "117-155")]
    assert (row["approved"], row["reason"], row["final_lower"]) == ("no", "use=no", "")


def test_verdict_read_only(limits_file, caplog):
    path = limits_file()
    path.parent.chmod(0o555)
    try:
        assert ("b-hb", "117-155") in reference_data.get_text_limits().index
    finally:
        path.parent.chmod(0o755)
    assert "can't be rewritten" in caplog.text
