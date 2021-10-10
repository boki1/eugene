#!/bin/bash

function usage() {
	echo "Build script for Eugene \
		===========================

		Options:

		--run 	| -r 				Run the project if built successfully
		--help 	| -h 				Print help
		--test 	| -t 				Run tests if built successfully
		--doc 	| -d 				Build and open Doxygen documentation if project was built successfully
	"
}

pushd . > /dev/null
SOURCE_DIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
SOURCE_DIR="$SOURCE_DIR/../"
cd "$SOURCE_DIR"
mkdir -p build/bin
cd build/bin
cmake ../.. && make

options=$(getopt -l "help,run,doc,test" -o "hrdt" -a -- "$@")

# set --:
# If no arguments follow this option, then the positional parameters are unset. Otherwise, the positional parameters
# are set to the arguments, even if some of them begin with a ‘-’.
eval set -- "$options"

while true
do
case $1 in
-h|--help)
	usage
    exit 0
    ;;
-r|--run)
	echo "Running eugene ..."
    ;;
-t|--test)
	ctest
	;;
-d|--doc)
	cd "$SOURCE_DIR"
	mkdir -p build/doc
	doxygen Doxyfile
	echo "Docs genereated"
	;;
--)
    shift
	break;;
esac
shift
done

popd > /dev/null
