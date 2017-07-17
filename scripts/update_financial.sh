#!/bin/bash

APP_DIR=${PWD}/..
LOGS_DIR=/tmp/logs
TODAY=`date '+%Y_%m_%d__%H_%M_%S'`
S3_BUCKET=s3://stock-analytics/logs

# Create log folder
mkdir -p ${LOGS_DIR}/${TODAY}

# Update financial data for Nasdaq
${HOME}/.nvm/v0.10.22/bin/node ${APP_DIR}/update_financials.js -t nasdaq > ${LOGS_DIR}/${TODAY}/update_financials_nasdaq.log

# Copy log file to S3
aws s3 cp ${LOGS_DIR}/${TODAY}/update_financials_nasdaq.log ${S3_BUCKET}/${TODAY}-update_financials_nasdaq.log

# Update financial data for NYSE
${HOME}/.nvm/v0.10.22/bin/node ${APP_DIR}/update_financials.js -t nyse > ${LOGS_DIR}/${TODAY}/update_financials_nyse.log

# Copy log file to S3
aws s3 cp ${LOGS_DIR}/${TODAY}/update_financials_nyse.log ${S3_BUCKET}/${TODAY}-update_financials_nyse.log

# Update EPS table for Nasdaq
${HOME}/.nvm/v0.10.22/bin/node ${APP_DIR}/update_eps.js -t nasdaq > ${LOGS_DIR}/${TODAY}/update_eps_nasdaq.log

# Copy log file to S3
aws s3 cp ${LOGS_DIR}/${TODAY}/update_eps_nasdaq.log ${S3_BUCKET}/${TODAY}-update_eps_nasdaq.log

# Update EPS table for NYSE
${HOME}/.nvm/v0.10.22/bin/node ${APP_DIR}/update_eps.js -t nyse > ${LOGS_DIR}/${TODAY}/update_eps_nyse.log

# Copy log file to S3
aws s3 cp ${LOGS_DIR}/${TODAY}/update_eps_nyse.log ${S3_BUCKET}/${TODAY}-update_eps_nyse.log

# Cleanup tmp log folder
rm -r ${LOGS_DIR}/${TODAY}
