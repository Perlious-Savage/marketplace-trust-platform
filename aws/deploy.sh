#!/usr/bin/env bash
# Cloud path: Kinesis stream -> Lambda -> S3.   Usage: aws/deploy.sh [teardown]
set -euo pipefail
cd "$(dirname "$0")/.."
export AWS_REGION=${AWS_REGION:-me-central-1}
export AWS_DEFAULT_REGION=$AWS_REGION
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=mkt-trust-lake-$ACCOUNT-$AWS_REGION
STREAM=mkt-cdc ROLE=mkt-trust-lambda FN=mkt-trust-processor
KINESIS_POLICY=arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole

if [ "${1:-}" = teardown ]; then
  docker compose --profile aws stop debezium-server 2>/dev/null || true
  for id in $(aws lambda list-event-source-mappings --function-name $FN --query 'EventSourceMappings[].UUID' \
      --output text 2>/dev/null); do aws lambda delete-event-source-mapping --uuid "$id" >/dev/null; done
  aws lambda delete-function --function-name $FN 2>/dev/null || true
  aws iam detach-role-policy --role-name $ROLE --policy-arn $KINESIS_POLICY 2>/dev/null || true
  aws iam delete-role-policy --role-name $ROLE --policy-name s3-write 2>/dev/null || true
  aws iam delete-role --role-name $ROLE 2>/dev/null || true
  aws kinesis delete-stream --stream-name $STREAM 2>/dev/null || true
  aws s3 rb "s3://$BUCKET" --force 2>/dev/null || true
  docker compose exec -T postgres psql -U postgres -d marketplace \
    -c "SELECT pg_drop_replication_slot('dbz_aws')" 2>/dev/null || true  # an idle slot would pin WAL forever
  rm -f .env.aws
  echo "torn down: nothing billable left"
  exit 0
fi

aws s3 mb "s3://$BUCKET" 2>/dev/null || true
aws kinesis create-stream --stream-name $STREAM --shard-count 1 2>/dev/null || true
aws kinesis wait stream-exists --stream-name $STREAM
STREAM_ARN=$(aws kinesis describe-stream-summary --stream-name $STREAM \
  --query StreamDescriptionSummary.StreamARN --output text)

aws iam create-role --role-name $ROLE --assume-role-policy-document \
  '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
  >/dev/null 2>&1 || true
aws iam attach-role-policy --role-name $ROLE --policy-arn $KINESIS_POLICY
aws iam put-role-policy --role-name $ROLE --policy-name s3-write --policy-document \
  "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"s3:PutObject\",\"Resource\":\"arn:aws:s3:::$BUCKET/*\"}]}"
ROLE_ARN=$(aws iam get-role --role-name $ROLE --query Role.Arn --output text)

# package: handler + shared detectors + benchmark + Linux pyarrow wheel
rm -rf aws/build aws/lambda.zip && mkdir -p aws/build
python -m pip install -q --platform manylinux2014_x86_64 --only-binary=:all: --python-version 3.12 \
  --target aws/build pyarrow
cp aws/lambda_handler.py processor/detectors.py postgres/benchmark_prices.csv aws/build/
python -c "import shutil; shutil.make_archive('aws/lambda', 'zip', 'aws/build')"
aws s3 cp aws/lambda.zip "s3://$BUCKET/code/lambda.zip" --only-show-errors

if aws lambda get-function --function-name $FN >/dev/null 2>&1; then
  aws lambda update-function-code --function-name $FN --s3-bucket "$BUCKET" --s3-key code/lambda.zip >/dev/null
else
  for _ in 1 2 3 4 5 6; do  # a new IAM role takes a few seconds to become assumable
    aws lambda create-function --function-name $FN --runtime python3.12 --handler lambda_handler.handler \
      --role "$ROLE_ARN" --code "S3Bucket=$BUCKET,S3Key=code/lambda.zip" --memory-size 512 --timeout 60 \
      --environment "Variables={BUCKET=$BUCKET}" >/dev/null && break || sleep 10
  done
fi
aws lambda wait function-active-v2 --function-name $FN
aws lambda create-event-source-mapping --function-name $FN --event-source-arn "$STREAM_ARN" \
  --starting-position LATEST --batch-size 100 --maximum-batching-window-in-seconds 1 >/dev/null 2>&1 || true

# short-lived credentials for the local Debezium Server container
aws configure export-credentials --format env-no-export > .env.aws
echo "AWS_REGION=$AWS_REGION" >> .env.aws

echo "deployed. next:"
echo "  docker compose --profile aws up -d debezium-server"
echo "  aws s3 ls s3://$BUCKET/ --recursive"
echo "  aws logs tail /aws/lambda/$FN --since 5m"
echo "  aws/deploy.sh teardown"
