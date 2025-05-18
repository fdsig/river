import datetime as _dt
import json as _json
from urllib.parse import quote_plus as _q
import requests
import pandas as pd


_API_ROOT = "https://timeseries.sepa.org.uk/KiWIS/KiWIS"
_DEFAULT_RETURN_FIELDS = "Timestamp,Value,Quality Code"


def _first_hit(url: str) -> dict:
    """Fetch a SEPA JSON endpoint and return the first record as a dict.

    The SEPA KiWIS JSON endpoints return data in a two-dimensional list form:

        [["col1", "col2"],               # header row (field names)
         ["val1", "val2"],               # first record
         ...]

    where the *first* sub-list contains the column names and each subsequent
    sub-list is a data row.  This helper converts the first data row into a
    dictionary keyed by the column names so that callers can use the field
    names directly (e.g. ``record["station_no"]``).
    """
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()

    # Basic validation – we expect at least a header row and one data row.
    if (
        not data
        or not isinstance(data, list)
        or len(data) < 2
        or not isinstance(data[0], list)
    ):
        raise LookupError("No matching records returned by SEPA.")

    header, first_row = data[0], data[1]
    # Guard against malformed rows with different lengths.
    if len(first_row) != len(header):
        raise LookupError("Malformed response from SEPA (row/header mismatch).")

    return dict(zip(header, first_row))


def get_river_level(
    name: str,
    date: str | _dt.date,
    *,
    parameter: str = "SG",
    resolution: str = "15m.Cmd",
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """
    Return a DataFrame of river-level values (m) for `name` on `date`
    using the public SEPA time-series API.

    Parameters
    ----------
    name : str
        Gauge/station name *or* river name (wildcards allowed, e.g. "Tay*").
    date : str | datetime.date
        Day of interest (YYYY-MM-DD).
    parameter : str, default "SG"
        SEPA parameter code – "SG" is river *Stage/level*.
    resolution : str, default "15m.Cmd"
        Time-series short-name (15-minute raw data).
    session : requests.Session | None
        Optional requests session (keeps the TCP connection open).

    Returns
    -------
    pd.DataFrame
        Timestamp-indexed DataFrame with columns: Value, Quality Code.
    """
    if isinstance(date, _dt.date) and not isinstance(date, _dt.datetime):
        date = date.isoformat()
    if "T" in date:
        raise ValueError("Pass a *date* (YYYY-MM-DD), not a full timestamp.")

    # ---------- 1. find the station number ---------------------------------
    sess = session or requests
    # Try station_name first, then river_name if no hit
    base_query = (
        f"{_API_ROOT}?service=kisters&type=queryServices&datasource=0"
        f"&request=getStationList&object_type=General&format=json"
        f"&returnfields=station_no,station_name,river_name"
    )
    qry_station = f"{base_query}&station_name={_q(name)}"
    try:
        station = _first_hit(qry_station)
    except LookupError:
        qry_station = f"{base_query}&river_name={_q(name)}"
        station = _first_hit(qry_station)

    sta_no = station["station_no"]

    # ---------- 2. build the ts_path for level data -------------------------
    ts_path = f"1/{sta_no}/{parameter}/{resolution}"

    # ---------- 3. fetch values for that date -------------------------------
    from_arg = date
    to_arg = f"{date}T23:59:59"

    qry_values = (
        f"{_API_ROOT}?service=kisters&type=queryServices&datasource=0"
        f"&request=getTimeseriesValues&ts_path={_q(ts_path)}"
        f"&from={from_arg}&to={to_arg}"
        f"&returnfields={_q(_DEFAULT_RETURN_FIELDS)}&format=json"
    )

    resp = sess.get(qry_values, timeout=60)
    resp.raise_for_status()
    raw = resp.json()

    # The timeseries endpoint returns a list with a single record that contains
    # the column names (comma-separated) and the data rows (list of lists).
    if (
        not raw
        or not isinstance(raw, list)
        or not isinstance(raw[0], dict)
        or "data" not in raw[0]
    ):
        raise LookupError(f"Malformed response fetching values for {name} on {date}.")

    rec = raw[0]
    cols = [c.strip() for c in rec["columns"].split(",")]
    df = pd.DataFrame(rec["data"], columns=cols)

    if df.empty:
        raise LookupError(f"No level values found for {name} on {date}.")

    df["Timestamp"] = pd.to_datetime(df["Timestamp"], utc=True)
    df.set_index("Timestamp", inplace=True)
    return df


# --------------------------- example usage ----------------------------------
if __name__ == "__main__":
    # River Tay at Pitnacree, 1 January 2024
    df_tay = get_river_level("Pitnacree", "2024-01-01")
    print(df_tay.head())
