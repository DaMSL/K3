#!/bin/bash
# generate mosaic files and compile them

if [ "$#" -ne 1 ]; then
	echo "Input an argument"
else

#get directory of script
pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd -P`
popd > /dev/null

FILENAME=$(basename "$1")
K3NAME="${FILENAME%%.*}"".k3"
ROOTPATH="${SCRIPTPATH}""/../../../.."

# check for dbtoaster
if [ ! -f "${ROOTPATH}/K3-Mosaic/tests/dbtoaster_release" ]; then
	${ROOTPATH}/K3-Mosaic/build_opt.sh
	${ROOTPATH}/K3-Mosaic/build_utils.sh
fi

# create the necessary files
${ROOTPATH}/K3-Mosaic/tests/auto_test.py --no-interp --no-deletes -d -f $1

# compile
echo "${SCRIPTPATH}/../run/compile.sh -fstage cexclude=Decl-FT,Decl-FE temp/${K3NAME}"
${SCRIPTPATH}/../run/compile.sh --fstage cexclude=Decl-FT,Decl-FE temp/${K3NAME}

fi
