#!/usr/bin/env bash
# Creates the S3 bucket (private, encrypted, lifecycle-managed), an SNS alert topic and the
# least-privilege Snowflake read role. Run from the repo root with AWS credentials configured.
#   BUCKET=my-ingest-bucket REGION=ap-south-1 ALERT_EMAIL=me@example.com ./scripts/setup_aws.sh
set -euo pipefail

: "${BUCKET:?set BUCKET}" "${REGION:=ap-south-1}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

echo ">> bucket s3://${BUCKET} in ${REGION}"
if [ "${REGION}" = "us-east-1" ]; then
  aws s3api create-bucket --bucket "${BUCKET}" --region "${REGION}"
else
  aws s3api create-bucket --bucket "${BUCKET}" --region "${REGION}" \
    --create-bucket-configuration "LocationConstraint=${REGION}"
fi
aws s3api put-public-access-block --bucket "${BUCKET}" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket "${BUCKET}" --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
aws s3api put-bucket-lifecycle-configuration --bucket "${BUCKET}" \
  --lifecycle-configuration file://aws/lifecycle.json

echo ">> SNS topic ingestion-alerts"
TOPIC_ARN="$(aws sns create-topic --name ingestion-alerts --region "${REGION}" --query TopicArn --output text)"
if [ -n "${ALERT_EMAIL:-}" ]; then
  aws sns subscribe --topic-arn "${TOPIC_ARN}" --protocol email --notification-endpoint "${ALERT_EMAIL}" --region "${REGION}"
  echo "   confirm the subscription e-mail AWS just sent you"
fi

echo ">> rendering policy templates into ./build"
mkdir -p build
for f in snowflake_role_policy pipeline_writer_policy; do
  sed -e "s/<BUCKET>/${BUCKET}/g" -e "s/<REGION>/${REGION}/g" -e "s/<ACCOUNT_ID>/${ACCOUNT_ID}/g" \
    "aws/${f}.json" > "build/${f}.json"
done

cat <<MSG

Done. Next steps:
  1. Attach build/pipeline_writer_policy.json to the IAM identity (role/user/SSO permission set) that runs the pipeline.
  2. Run sql/01_setup.sql in Snowflake, then ./scripts/create_snowflake_role.sh (needs the values from DESC INTEGRATION).
  3. Run sql/02..04, then apply the S3 event notification (README, step 6).
  SNS_TOPIC_ARN=${TOPIC_ARN}
MSG
