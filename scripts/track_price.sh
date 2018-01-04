#!/bin/bash

APP_DIR=${PWD}/..
LOGS_DIR=/tmp/logs
TODAY=`date '+%Y_%m_%d__%H_%M_%S'`
S3_BUCKET=s3://stock-analytics/logs

# Create log folder
mkdir -p ${LOGS_DIR}/${TODAY}

# Update financial data
${HOME}/.nvm/versions/node/v6.10.2/bin/node ${APP_DIR}/track_price.js > ${LOGS_DIR}/${TODAY}/track_price.log

# Copy log file to S3
aws s3 cp ${LOGS_DIR}/${TODAY}/track_price.log ${S3_BUCKET}/${TODAY}-track_price.log
