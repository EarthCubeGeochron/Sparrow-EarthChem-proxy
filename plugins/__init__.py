from dataclasses import dataclass
import pandas
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


@dataclass
class Value:
    """
    A simple class to hold a value and its unit.
    """

    value: float
    unit: str
    method: str


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
            val = Value(val, str(row.loc[uid]), str(row.loc[mid]))
            print(f"{col_id}: {val.value} {val.unit} ({val.method})")
        else:
            print(f"{col_id}: {val}")
        data[col_id] = val

    print(data)
    print()


## Utility functions


def combine_repeated_columns(df):
    """Take the first value of duplicate columns and drop the rest."""
    # Clean column names by removing trailing digits and whitespace
    df.columns = [re.sub(r"\.\d+$", "", col_id).strip() for col_id in df.columns]

    columns_to_keep = list(range(len(df.columns)))
    has_duplicates = set()

    for ix, column_name in enumerate(df.columns):
        if column_name in has_duplicates:
            continue
        for duplicate_index in duplicate_indexes(df, column_name):
            if column_name == "ZN METH":
                embed()
            df.iloc[:, ix].combine_first(df.iloc[:, duplicate_index])
            # Continue removing things from our "keep" list if they are duplicates
            columns_to_keep.remove(duplicate_index)
            has_duplicates.add(column_name)

    print(f"Removed duplicates for {len(has_duplicates)} columns")
    print(has_duplicates)

    res = df.iloc[:, columns_to_keep]

    # Check that we have removed all duplicates
    assert len(res.columns) == len(set(res.columns))

    return res


def duplicate_indexes(df, col_name) -> List[int]:
    ix = list(df.columns).index(col_name)
    for ix_1, column_name_1 in enumerate(df.columns[ix + 1 :]):
        if column_name_1 == col_name:
            yield ix_1 + ix
