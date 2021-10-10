#!/bin/bash

pushd . > /dev/null
SOURCE_DIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
SOURCE_DIR="$SOURCE_DIR/../"
cd "$SOURCE_DIR"

rm -rf build/bin/*
rm -rf build/doc/*

popd > /dev/null
