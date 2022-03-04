from dataclasses import dataclass
import pandas
import numpy as N
import re
from pathlib import Path
from IPython import embed

# Fancy printing
from rich import print
from typing import List
import sparrow


@sparrow.task(name="import-earthchem")
def import_earthchem():
    """
    This task imports files dumped from the EarthChem portal.
    """
    data_dir = Path(sparrow.settings.DATA_DIR)
    files = data_dir.glob("*.txt")
    for file in files:
        # Read a thousand rows at a time
        chunks = pandas.read_table(file, sep="\t", chunksize=1000)
        for df in chunks:
            df = combine_repeated_columns(df)
            # Call the import_sample function for each row
            df.apply(import_sample, axis=1)


def import_sample(row):
    """
    This function imports a single sample from the EarthChem dump file.
    """
    data = {}
    for col_id, val in row.iteritems():
        if pandas.isnull(val) or col_id.endswith(" UNIT") or col_id.endswith(" METH"):
            continue
        uid = col_id + " UNIT"
        mid = col_id + " METH"
        if col_id + " UNIT" in row.index and col_id + " METH" in row.index:
            val = Value(val, col_id, str(row.loc[uid]), str(row.loc[mid]))
            print(f"{col_id}: {val.value} {val.unit} ({val.method})")
        elif col_id.endswith("AGE"):
            v1 = float(val)
            if v1 < 0:
                # The EarthChem portal uses negative ages to indicate an age in years before present.
                val = Value(v1 * -1, col_id, "year", "UNKNOWN")
            else:
                val = Value(v1, col_id, "Ma", "UNKNOWN")
        else:
            print(f"{col_id}: {val}")
        data[col_id] = val

    print(data)
    # Now, we restructure the data into its final nested form

    # Get reference data
    ref = data["REFERENCE"]
    year = re.search(r", (\d{4})$", ref)

    if year is None:
        raise Exception(f"Could not parse reference {ref}")
    authors = ref[: year.start()].strip()
    year = int(year.group(1))

    sample = {
        "name": data["SAMPLE ID"],
        "location": {
            "type": "Point",
            "coordinates": [
                float(data["LONGITUDE"]),
                float(data["LATITUDE"]),
            ],
        },
        "publication": {
            "author": authors,
            "year": year,
        },
    }

    if precision := data.get("LOC PREC"):
        sample["location_precision"] = meters_per_degree(
            float(data["LATITUDE"])
        ) * float(precision)

    sample["sessions"] = []

    print(sample)

    print()


## Utility functions


@dataclass
class Value:
    """
    A simple class to hold a value and its unit.
    """

    value: float
    parameter: str
    unit: str
    method: str


def combine_repeated_columns(df):
    """Take the first value of duplicate columns and drop the rest."""
    # Clean column names by removing trailing digits and whitespace
    for column_name in df.columns:
        # Strip pandas-created suffixes for repeated columns
        cleaned_column_name = re.sub(r"\.\d+$", "", column_name).strip()
        if cleaned_column_name == column_name:
            # No need to change anything
            continue

        if cleaned_column_name not in df.columns:
            # We need to improve our column name
            df.rename(columns={column_name: cleaned_column_name}, inplace=True)
            continue

        df.loc[:, cleaned_column_name].combine_first(df.loc[:, column_name])
        df.drop(columns=[column_name], inplace=True)
    return df


def duplicate_indexes(df, col_name) -> List[int]:
    ix = list(df.columns).index(col_name)
    for ix_1, column_name_1 in enumerate(df.columns[ix + 1 :]):
        if column_name_1 == col_name:
            yield ix_1 + ix


def meters_per_degree(lat: float) -> float:
    """
    Return approximate number of meters per degree of latitude.
    https://stackoverflow.com/questions/639695/how-to-convert-latitude-or-longitude-to-meters
    We need this function because Sparrow stores measurement precision in meters, not degrees.
    """
    cos_lat = N.cos(N.radians(lat))
    return 111132.92 - 559.82 * cos_lat ** 2 + 1.175 * cos_lat ** 4
