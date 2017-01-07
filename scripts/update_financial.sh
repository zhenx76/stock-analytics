#!/bin/bash

APP_DIR=${PWD}/..
LOGS_DIR=/tmp/logs
TODAY=`date '+%Y_%m_%d__%H_%M_%S'`
S3_BUCKET=s3://stock-analytics/logs

# Create log folder
mkdir -p ${LOGS_DIR}/${TODAY}

# Update financial data
node ${APP_DIR}/update_financials.js > ${LOGS_DIR}/${TODAY}/update_financials.log

# Copy log file to S3
aws s3 cp ${LOGS_DIR}/${TODAY}/update_financials.log ${S3_BUCKET}/${TODAY}-update_financials.log

# Update EPS table
node ${APP_DIR}/update_eps.js > ${LOGS_DIR}/${TODAY}/update_eps.log

# Copy log file to S3
aws s3 cp ${LOGS_DIR}/${TODAY}/update_eps.log ${S3_BUCKET}/${TODAY}-update_eps.log
