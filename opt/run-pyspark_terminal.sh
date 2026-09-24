#!/bin/sh

##############################################
# RUN BASH TERMINAL
##############################################
if [ -z "$BASH_VERSION" ]; then
    exec bash "$0" "$@"
fi


##############################################
# LOOKING FOR ENVIRONMENT VARIABLES DO NOT MODIFY THIS SECTION
##############################################

# SEARCHING FO ENVIRONMENT VARIABLES

CURRENT_SCRIPT_DIR_LINUX=$(dirname "$(realpath "$0")")
TESTING_DIR_LINUX=$(realpath "$CURRENT_SCRIPT_DIR_LINUX/../..")
MINERVA_WORKSPACE_DIR_LINUX=$(realpath "$CURRENT_SCRIPT_DIR_LINUX/..")
export MINERVA_UNIX_ENVIRONMENT_VARS="${MINERVA_WORKSPACE_DIR_LINUX}/opt/environment_vars.sh"

source "${MINERVA_UNIX_ENVIRONMENT_VARS}"

##############################################
# VIRTUAL ENVIRONMENT
##############################################

# Conda setup
$CONDA_LINUX init bash
eval "$($CONDA_LINUX shell.bash hook)"

# Activate virtual environment
conda activate $VENV_NAME

# ---------------------------------------------------------------------------
# Check if the virtual environment tar.gz file exists in HDFS
# ---------------------------------------------------------------------------
# hdfs dfs -rm -f -r -skipTrash "${MINERVA_VENV_TAR_GZ_HDFS}"
if hdfs dfs -test -e "${MINERVA_VENV_TAR_GZ_HDFS}"; then
    echo "Virtual environment already exists in HDFS: ${MINERVA_VENV_TAR_GZ_HDFS}"
else
    echo "Virtual environment does not exist in HDFS. Uploading: ${MINERVA_VENV_TAR_GZ_HDFS}"
    # Remove the existing tar.gz file if it exists
    rm -f "${MINERVA_VENV_TAR_GZ_LINUX}"
    conda-pack -n $VENV_NAME -o "${MINERVA_VENV_TAR_GZ_LINUX}"
    # Create the parent directory in HDFS if it doesn't exist
    hdfs dfs -mkdir -p "${MINERVA_VENV_TAR_GZ_PARENT_DIR_HDFS}"
    hdfs dfs -put -f "${MINERVA_VENV_TAR_GZ_LINUX}" "${MINERVA_VENV_TAR_GZ_PARENT_DIR_HDFS}"
    # Changing permissions to make it readable by all users
    hdfs dfs -chmod -R o+rx "${MINERVA_VENV_TAR_GZ_PARENT_DIR_HDFS}"
fi



if hdfs dfs -test -e "${MINERVA_GRAPHFRAMES_JAR_HDFS}"; then
    echo "Jar Files already exist in HDFS: ${MINERVA_GRAPHFRAMES_JAR_HDFS}"
else
    echo "Jar Files already does not exist in HDFS. Uploading: ${MINERVA_GRAPHFRAMES_JAR_HDFS}"
    # Create the parent directory in HDFS if it doesn't exist
    hdfs dfs -mkdir -p "${MINERVA_GRAPHFRAMES_JAR_PARENT_DIR_HDFS}"
    hdfs dfs -put -f "${MINERVA_GRAPHFRAMES_JAR_LINUX}" "${MINERVA_GRAPHFRAMES_JAR_PARENT_DIR_HDFS}"
    # Changing permissions to make it readable by all users
    hdfs dfs -chmod -R o+rx "${MINERVA_GRAPHFRAMES_JAR_PARENT_DIR_HDFS}"
fi



##############################################
# NOTEBOOK
##############################################
cd $MINERVA_WORKSPACE_DIR_LINUX
python
