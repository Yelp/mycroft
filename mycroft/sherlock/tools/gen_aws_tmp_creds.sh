#!/bin/bash

set -e

STS_EXTRA_OPTS=''

usage() {
    cat << EOF
Usage: $0 -t MFA_token -z time_in_seconds
EOF
}

while getopts "t:z:" opt; do
    case $opt in
        t)
            MFA_TOKEN=$OPTARG
            ;;
        z)
            AUTH_TIMEOUT=$OPTARG
            ;;
        \?)
            usage
            exit 1
            ;;
        :)
            echo option requires an argument
            usage
            exit 1
    esac
done

if [ -z $MFA_TOKEN ]; then
    usage
    exit 1
fi

if [ -n $MFA_TOKEN ]; then
    if [ -z $AWS_MFA_SERIAL ]; then
        cat << EOF
You need to export AWS_MFA_SERIAL
See http://docs.aws.amazon.com/cli/latest/reference/sts/get-session-token.html about serial-number
EOF
        exit 1
    fi
    STS_EXTRA_OPTS="--serial-number $AWS_MFA_SERIAL --token-code $MFA_TOKEN"
fi

if [ "$AUTH_TIMEOUT" ]; then
    STS_EXTRA_OPTS="$STS_EXTRA_OPTS --duration-seconds $AUTH_TIMEOUT"
fi

aws sts get-session-token $STS_EXTRA_OPTS | sherlock/tools/sts_to_metadata_response.py
