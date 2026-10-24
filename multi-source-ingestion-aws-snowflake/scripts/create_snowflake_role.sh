#!/usr/bin/env bash
# Creates the IAM role Snowflake assumes. Run AFTER sql/01_setup.sql (DESC INTEGRATION S3_INT).
#   BUCKET=... SF_IAM_USER_ARN=arn:aws:iam::...:user/... SF_EXTERNAL_ID=... ./scripts/create_snowflake_role.sh
set -euo pipefail
: "${BUCKET:?}" "${SF_IAM_USER_ARN:?}" "${SF_EXTERNAL_ID:?}"
ROLE_NAME="snowflake-s3-ingest-role"

mkdir -p build
sed -e "s#<STORAGE_AWS_IAM_USER_ARN>#${SF_IAM_USER_ARN}#" -e "s#<STORAGE_AWS_EXTERNAL_ID>#${SF_EXTERNAL_ID}#" \
  aws/trust_policy_snowflake_role.json > build/trust_policy.json
sed -e "s/<BUCKET>/${BUCKET}/g" aws/snowflake_role_policy.json > build/snowflake_role_policy.json

if aws iam get-role --role-name "${ROLE_NAME}" >/dev/null 2>&1; then
  aws iam update-assume-role-policy --role-name "${ROLE_NAME}" --policy-document file://build/trust_policy.json
else
  aws iam create-role --role-name "${ROLE_NAME}" --assume-role-policy-document file://build/trust_policy.json
fi
aws iam put-role-policy --role-name "${ROLE_NAME}" --policy-name s3-read-raw \
  --policy-document file://build/snowflake_role_policy.json
echo "Role ${ROLE_NAME} ready. The ARN must match STORAGE_AWS_ROLE_ARN in sql/01_setup.sql."
