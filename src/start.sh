#!/bin/sh
export _HANDLER=$1
if [ -n "$AWS_LAMBDA_RUNTIME_API" ]; then
    exec /usr/local/bin/python3 -m awslambdaric $_HANDLER
else
    exec python3 $_HANDLER
fi


