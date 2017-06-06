SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export SIGN_KEYSTORE="$SCRIPT_DIR/example.jks"
export SIGN_ALIAS=example0
export SIGN_PASSWORD=example
export ALKITAB_PROPRIETARY_DIR="$SCRIPT_DIR/proprietary-dir"

export BUILD_PACKAGE_NAME=org.tbsbibles.bg
export BUILD_DIST=market
export FLAVOR=org_tbsbibles_bg
source ./ybuild.sh
