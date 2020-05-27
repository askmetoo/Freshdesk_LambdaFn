#!/bin/bash

# variables
TEMPLATE_IN=freshdesk_sam_template.yaml
TEMPLATE_OUT=deploy_freshdesk_cf_template.yaml
#S3_BUCKET=hmlet-datalake-codefile-ap-southeast-1
S3_BUCKET=hmlet-datalake-codefiles-ap-southeast-1
STACK_NAME=hmlet-datalake-freshdesk-stack

# package (upload artifact to S3)
echo "SAM is now packaging..."
sam package --template-file $TEMPLATE_IN --output-template-file $TEMPLATE_OUT --s3-bucket $S3_BUCKET

# deploy (CloudFormation changesets)
echo "SAM is now deploying..."
sam deploy --template-file $TEMPLATE_OUT --stack-name $STACK_NAME --capabilities CAPABILITY_IAM
