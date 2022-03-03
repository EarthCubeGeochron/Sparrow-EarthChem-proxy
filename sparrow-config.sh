export COMPOSE_PROJECT_NAME=navdat
export SPARROW_DATA_DIR="$SPARROW_CONFIG_DIR/data"
export SPARROW_PLUGIN_DIR="$SPARROW_CONFIG_DIR/plugins"

# Disable sparrow worker subsystem
export SPARROW_TASK_WORKER=0