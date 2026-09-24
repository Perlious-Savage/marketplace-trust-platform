#!/usr/bin/env bash
# Cloud path.  Usage: aws/deploy.sh [http|kinesis|teardown]
#   http    (default, works on the AWS Free plan): Debezium Server --HTTPS--> Lambda function URL -> S3
#   kinesis (paid plan): Debezium Server -> Kinesis stream -> Lambda (event source mapping) -> S3
set -euo pipefail
export MSYS_NO_PATHCONV=1  # Git Bash on Windows: don't rewrite /aws/lambda/... into C:/...
cd "$(dirname "$0")/.."
MODE=${1:-http}
export AWS_REGION=${AWS_REGION:-$(aws configure get region || echo ap-south-1)}
export AWS_DEFAULT_REGION=$AWS_REGION
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=mkt-trust-lake-$ACCOUNT-$AWS_REGION
STREAM=mkt-cdc ROLE=mkt-trust-lambda FN=mkt-trust-processor
BASIC=arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
KINESIS=arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole

if [ "$MODE" = teardown ]; then
  docker compose --profile aws stop debezium-server 2>/dev/null || true
  for id in $(aws lambda list-event-source-mappings --function-name $FN --query 'EventSourceMappings[].UUID' \
      --output text 2>/dev/null); do aws lambda delete-event-source-mapping --uuid "$id" >/dev/null; done
  aws lambda delete-function --function-name $FN 2>/dev/null || true  # also removes its function URL
  aws logs delete-log-group --log-group-name /aws/lambda/$FN 2>/dev/null || true
  for p in $BASIC $KINESIS; do aws iam detach-role-policy --role-name $ROLE --policy-arn $p 2>/dev/null || true; done
  aws iam delete-role-policy --role-name $ROLE --policy-name s3-write 2>/dev/null || true
  aws iam delete-role --role-name $ROLE 2>/dev/null || true
  aws kinesis delete-stream --stream-name $STREAM 2>/dev/null || true
  aws s3 rb "s3://$BUCKET" --force 2>/dev/null || true
  docker compose exec -T postgres psql -U postgres -d marketplace \
    -c "SELECT pg_drop_replication_slot('dbz_aws')" 2>/dev/null || true  # an idle slot would pin WAL forever
  rm -f .env.aws
  echo "torn down: nothing left in AWS"
  exit 0
fi

aws s3 mb "s3://$BUCKET" 2>/dev/null || true
aws iam create-role --role-name $ROLE --assume-role-policy-document \
  '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
  >/dev/null 2>&1 || true
aws iam attach-role-policy --role-name $ROLE --policy-arn "$([ "$MODE" = kinesis ] && echo $KINESIS || echo $BASIC)"
aws iam put-role-policy --role-name $ROLE --policy-name s3-write --policy-document \
  "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"s3:PutObject\",\"Resource\":\"arn:aws:s3:::$BUCKET/*\"}]}"
ROLE_ARN=$(aws iam get-role --role-name $ROLE --query Role.Arn --output text)

# package: handler + shared detectors + benchmark + Linux pyarrow wheel
rm -rf aws/build aws/lambda.zip && mkdir -p aws/build
python -m pip install -q --platform manylinux2014_x86_64 --only-binary=:all: --python-version 3.12 \
  --target aws/build pyarrow
cp aws/lambda_handler.py processor/detectors.py postgres/benchmark_prices.csv aws/build/
python -c "import shutil; shutil.make_archive('aws/lambda', 'zip', 'aws/build')"
if [ "$(stat -c%s aws/lambda.zip)" -lt 50000000 ]; then  # direct upload: no S3 copy of the code
  CREATE_CODE=(--zip-file fileb://aws/lambda.zip); UPDATE_CODE=(--zip-file fileb://aws/lambda.zip)
else
  aws s3 cp aws/lambda.zip "s3://$BUCKET/code/lambda.zip" --only-show-errors
  CREATE_CODE=(--code "S3Bucket=$BUCKET,S3Key=code/lambda.zip"); UPDATE_CODE=(--s3-bucket "$BUCKET" --s3-key code/lambda.zip)
fi

TOKEN=$(python -c "import secrets; print(secrets.token_hex(16))")
ENVVARS="Variables={BUCKET=$BUCKET,TOKEN=$TOKEN}"
if aws lambda get-function --function-name $FN >/dev/null 2>&1; then
  aws lambda update-function-code --function-name $FN "${UPDATE_CODE[@]}" >/dev/null
  aws lambda wait function-updated-v2 --function-name $FN
  aws lambda update-function-configuration --function-name $FN --environment "$ENVVARS" >/dev/null
else
  for _ in 1 2 3 4 5 6; do  # a new IAM role takes a few seconds to become assumable
    aws lambda create-function --function-name $FN --runtime python3.12 --handler lambda_handler.handler \
      --role "$ROLE_ARN" "${CREATE_CODE[@]}" --memory-size 512 --timeout 30 --environment "$ENVVARS" \
      >/dev/null && break || sleep 10
  done
fi
aws lambda wait function-updated-v2 --function-name $FN

if [ "$MODE" = kinesis ]; then
  aws kinesis create-stream --stream-name $STREAM --shard-count 1 2>/dev/null || true
  aws kinesis wait stream-exists --stream-name $STREAM
  STREAM_ARN=$(aws kinesis describe-stream-summary --stream-name $STREAM \
    --query StreamDescriptionSummary.StreamARN --output text)
  aws lambda create-event-source-mapping --function-name $FN --event-source-arn "$STREAM_ARN" \
    --starting-position LATEST --batch-size 100 --maximum-batching-window-in-seconds 1 >/dev/null 2>&1 || true
  aws configure export-credentials --format env-no-export > .env.aws  # short-lived creds for Debezium Server
  printf 'AWS_REGION=%s\nDEBEZIUM_SINK_TYPE=kinesis\n' "$AWS_REGION" >> .env.aws
else
  aws lambda create-function-url-config --function-name $FN --auth-type NONE >/dev/null 2>&1 || true
  aws lambda add-permission --function-name $FN --statement-id url --action lambda:InvokeFunctionUrl \
    --principal '*' --function-url-auth-type NONE >/dev/null 2>&1 || true
  aws lambda add-permission --function-name $FN --statement-id url-invoke --action lambda:InvokeFunction \
    --principal '*' --invoked-via-function-url >/dev/null 2>&1 || true
  URL=$(aws lambda get-function-url-config --function-name $FN --query FunctionUrl --output text)
  printf 'DEBEZIUM_SINK_TYPE=http\nDEBEZIUM_SINK_HTTP_URL=%s?token=%s\n' "$URL" "$TOKEN" > .env.aws
fi

echo "deployed ($MODE). next:"
echo "  docker compose --profile aws up -d debezium-server"
echo "  aws s3 ls s3://$BUCKET/ --recursive | tail"
echo "  aws logs tail /aws/lambda/$FN --since 5m"
echo "  aws/deploy.sh teardown"
