from pathlib import Path
from sparrow.plugins import SparrowPlugin

import sparrow
from sparrow.import_helpers import BaseImporter
from click import echo, secho
from sparrow.config import SPARROW_DATA_DIR


class EarthChemImporter(BaseImporter):
    """
    A Sparrow importer for EarthChem data.
    """

    authority = "ALC"
    name = "earthchem-data"

    def import_data(self):
        data_dir = Path(SPARROW_DATA_DIR)
        # echo(data_dir)


@sparrow.task(name="import-earthchem")
def import_earthchem():
    """
    Import EarthChem data
    """
    echo("Starting import task")
    importer = EarthChemImporter()
    importer.import_data()
