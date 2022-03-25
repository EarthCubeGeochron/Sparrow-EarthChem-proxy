from dataclasses import dataclass
import pandas
import numpy as N
import re
from pathlib import Path
from IPython import embed
from decimal import Decimal

# Fancy printing
from rich import print
from typing import List, Optional
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
        # Check to see whether we have a defined unit and method
        if uid in row.index and mid in row.index:
            unit = row.loc[uid]
            if pandas.isnull(unit) and "_" in col_id:
                # We have a composited column name and no unit, which probably means a ratio
                unit = "ratio"
            val = Value(val, col_id, str(unit), str(row.loc[mid]))
            print(f"{col_id}: {val.value} {val.unit} ({val.method})")
        elif col_id.endswith("AGE"):
            v1 = float(val)
            if v1 < 0:
                # The EarthChem portal uses negative ages to indicate an age in years before present.
                val = Value(v1 * -1, col_id, "year", "UNKNOWN")
            else:
                val = Value(v1, col_id, "Ma", "UNKNOWN")

        data[col_id] = val

    post_process_ages(data)

    print(data)
    # Now, we restructure the data into its final nested form

    # Get reference data
    ref = data["REFERENCE"]
    year = re.search(r", (\d{4})$", ref)

    if year is None:
        raise Exception(f"Could not parse reference {ref}")
    authors = ref[: year.start()].strip()
    year = int(year.group(1))
    latitude = float(data["LATITUDE"])

    sample = {
        "name": data["SAMPLE ID"],
        "location": {
            "type": "Point",
            "coordinates": [
                float(data["LONGITUDE"]),
                latitude,
            ],
        },
        "publication": {
            "author": authors,
            "year": year,
        },
        "attribute": list(get_attributes(data)),
        "material": {
            "id": data["ROCK NAME"],
            "member_of": data["MATERIAL"],
        },
    }

    if precision := data.get("LOC PREC"):
        sample["location_precision"] = meters_per_degree(latitude) * float(precision)

    sample["sessions"] = get_sessions(data)

    print(sample)

    print()

    # Actually load the data into the database
    db = sparrow.get_database()
    db.load_data("sample", sample)


def get_attributes(data):
    for col_name in ["MATERIAL", "TYPE", "COMPOSITION", "ROCK NAME", "SOURCE"]:
        val = data.get(col_name)
        if val is None:
            continue
        yield {
            "name": col_name,
            "value": val,
        }


def get_sessions(data):
    params = [v for v in data.values() if isinstance(v, Value)]
    # It might be nice to split these into different analyses here for tracking purposes.
    datum_list = [v.to_datum() for v in params]

    return [{"analysis": [{"datum": datum_list}]}]


## Utility functions


@dataclass
class Value:
    """
    A simple class to hold a value and its unit. This could maybe be
    eventually replaced with a Pydantic model.
    """

    value: float
    parameter: str
    unit: str
    method: str
    error: Optional[float] = None

    def to_datum(self):
        return {
            "value": self.value,
            "error": self.error,
            "type": {
                "parameter": self.parameter,
                "unit": self.unit,
                "method": self.method,
            },
        }


def post_process_ages(data):
    age = data.get("AGE")
    min_age = data.get("MIN AGE")
    max_age = data.get("MAX AGE")
    if any(x is None for x in [age, min_age, max_age]):
        return
    # If we have symmetric min and and max ages, we assume that these represent
    # a Gaussian error bound on the age.
    if age.value - min_age.value == max_age.value - age.value:
        del data["MIN AGE"]
        del data["MAX AGE"]
        data["AGE"].error = age.value - min_age.value


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
