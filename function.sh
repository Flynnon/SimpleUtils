#!/usr/bin/env bash

# 一些简单的函数定义

# print something to stderr
function echoerr() {
    cat <<<"$@" 1>&2
}

# judge a directory is exist.
# return 1 if it is exists else 0
function directory_exists() {
    local directory_path=$1
    if [ ! ${directory_path} ]; then
        echoerr "The function usage is wrong. Please input the directory's path."
        exit 2
    fi

    if [ -d "${directory_path}" ]; then
        return 1
    else
        return 0
    fi
}

# add a path to PYTHONPATH
# return 0 if success else exit
function add_python_path() {
    local need_add_path=$1

    if [ ! ${need_add_path} ]; then
        echoerr "Please input the path you want to add .."
        exit 2
    fi

    if [[ ${PYTHONPATH} != *"${need_add_path}"* ]]; then
        export PYTHONPATH="${PYTHONPATH}:${need_add_path}"
    fi
    echo "Now, the python path is ${PYTHONPATH}."
    return 0
}

# create a directory. if it exists, do nothing
function create_directory() {
    local directory_path=$1

    directory_exists ${directory_path}

    if [ $? -eq 0 ]; then
        mkdir ${directory_path}
        return 0
    fi
    return 1
}

# create or clear a directory
function create_or_clear_directory() {
    local directory_path=$1

    directory_exists ${directory_path}

    if [ $? -eq 0 ]; then
        mkdir ${directory_path}
    else
        rm -rf ${directory_path}/*
    fi
}