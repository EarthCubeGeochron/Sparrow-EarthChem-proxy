export COMPOSE_PROJECT_NAME=navdat
export SPARROW_DATA_DIR="$SPARROW_CONFIG_DIR/data"
export SPARROW_PLUGIN_DIR="$SPARROW_CONFIG_DIR/plugins"
export SPARROW_LAB_NAME="EarthChem"

# Disable sparrow worker subsystem
export SPARROW_TASK_WORKER=0
export SPARROW_VERSION=">=2.5,>=3.0.0a1"