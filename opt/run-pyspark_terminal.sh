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
APP_WORKSPACE_DIR_LINUX=$(realpath "$CURRENT_SCRIPT_DIR_LINUX/..")
export APP_UNIX_ENVIRONMENT_VARS="${APP_WORKSPACE_DIR_LINUX}/opt/environment_vars.sh"

source "${APP_UNIX_ENVIRONMENT_VARS}"

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
# hdfs dfs -rm -f -r -skipTrash "${PYSPARK_VENV_TAR_GZ_HDFS}"
if hdfs dfs -test -e "${PYSPARK_VENV_TAR_GZ_HDFS}"; then
    echo "Virtual environment already exists in HDFS: ${PYSPARK_VENV_TAR_GZ_HDFS}"
else
    echo "Virtual environment does not exist in HDFS. Uploading: ${PYSPARK_VENV_TAR_GZ_HDFS}"
    # Remove the existing tar.gz file if it exists
    rm -f "${VENV_TAR_GZ_LINUX}"
    conda-pack -n $VENV_NAME -o "${VENV_TAR_GZ_LINUX}"
    # Create the parent directory in HDFS if it doesn't exist
    hdfs dfs -mkdir -p "${VENV_TAR_GZ_PARENT_DIR_HDFS}"
    hdfs dfs -put -f "${VENV_TAR_GZ_LINUX}" "${VENV_TAR_GZ_PARENT_DIR_HDFS}"
    # Changing permissions to make it readable by all users
    hdfs dfs -chmod -R o+rx "${VENV_TAR_GZ_PARENT_DIR_HDFS}"
fi



if hdfs dfs -test -e "${PYSPARK_JARS_HDFS}"; then
    echo "Jar Files already exist in HDFS: ${PYSPARK_JARS_HDFS}"
else
    echo "Jar Files do not exist in HDFS. Uploading: ${PYSPARK_JARS_HDFS}"
    # Create the parent directory in HDFS if it doesn't exist
    hdfs dfs -mkdir -p "${EXTRA_JAR_PARENT_DIR_HDFS}"
    hdfs dfs -put -f "${EXTRA_JAR_LINUX}" "${EXTRA_JAR_PARENT_DIR_HDFS}"
    # Changing permissions to make it readable by all users
    hdfs dfs -chmod -R o+rx "${EXTRA_JAR_PARENT_DIR_HDFS}"
fi



##############################################
# PYSPARK TERMINAL
##############################################
cd $APP_WORKSPACE_DIR_LINUX
python
