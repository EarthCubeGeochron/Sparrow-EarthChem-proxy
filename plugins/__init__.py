from pathlib import Path
from pandas import read_table
from IPython import embed
import sparrow


@sparrow.task(name="import-earthchem")
def import_earthchem():
    """
    Import EarthChem data
    """
    data_dir = Path(sparrow.settings.DATA_DIR)
    files = data_dir.glob("*.txt")
    for file in files:
        df = read_table(file, sep="\t")
        ## The line below embeds a shell prompt in the notebook
        embed()
        ## Here we raise an error, so we don't get in an endless loop
        ## of shell embedding :)
        raise
