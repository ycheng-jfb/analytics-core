#!/bin/bash
# --------------------------------------------------------------------
# Sync Airflow DAGs from S3 bucket to local folder
# --------------------------------------------------------------------

# S3 source and local destination
S3_PATH="s3://jfb.airflow.db/dags/"
LOCAL_PATH="C:\\git\\airflow_jfb\\dags"

# Display start message
echo "============================================="
echo " Syncing DAGs from S3 → Local"
echo " Source: $S3_PATH"
echo " Destination: $LOCAL_PATH"
echo "============================================="

# Run sync
aws s3 sync "$S3_PATH" "$LOCAL_PATH"

# Completion message
if [ $? -eq 0 ]; then
  echo "✅ Sync completed successfully."
else
  echo "❌ Sync failed. Please check your AWS CLI configuration."
fi
