## Folder structure for Freshdesk Lambda deployment using SAM CLI

This project contains source code and supporting files for a serverless application that you can deploy with the SAM CLI. It includes the following files and folders.

- Freshdesk_s3 : Code for the extracting freshdesk- Lambda function.
- lambda-layer-library : Python dependant library to be placed in lambda layer.
- tests - Unit tests for the application code. 
- freshdesk_sam_template.yaml - A template that defines the application's AWS resources.
- hmlet-freshdesk-sam-template-deploy.sh - Bash script to automate the SAM CLI Lambda deployment.

The application uses several AWS resources, including Lambda functions, SNS, SAM CLI, S3 and cloud watch. These resources are defined in the `freshdesk_sam_template.yaml` file in this project. You can update the template to add AWS resources through the same deployment process that updates your application code.

## Deploy the freshdesk application using SAM CLI and Cloud formation

The Serverless Application Model Command Line Interface (SAM CLI) is an extension of the AWS CLI that adds functionality for building and testing Lambda applications. 

Steps:
1. Add the python dependant libraries in the lambda-layer-library folder and zip the folder.
2. Create SAM yaml template file which inlcudes the AWS resources and configuration settings for Lambda, Lambda layer, SNS.
3. Place the python scripts for extraction of freshdesk data in the folder - Freshdesk_s3.
4. hmlet-freshdesk-sam-template-deploy.sh : Create the bash script SAM CLI commands that first converts the SAM CLI yaml template to cloud formation template and create a stack in cloud formation to create lambda function , lambda layer and cloud watch event.
5. Run the bash script - hmlet-freshdesk-sam-template-deploy.sh


Notes:
* **Stack Name**: The name of the stack to deploy to CloudFormation. This should be unique to your account and region, and a good starting point would be something matching your project name.
* **AWS Region**: The AWS region you want to deploy your app to.
* **Confirm changes before deploy**: If set to yes, any change sets will be shown to you before execution for manual review. If set to no, the AWS SAM CLI will automatically deploy application changes.
* **Allow SAM CLI IAM role creation**: Many AWS SAM templates, including this example, create AWS IAM roles required for the AWS Lambda function(s) included to access AWS services. By default, these are scoped down to minimum required permissions. To deploy an AWS CloudFormation stack which creates or modified IAM roles, the `CAPABILITY_IAM` value for `capabilities` must be provided. If permission isn't provided through this prompt, to deploy this example you must explicitly pass `--capabilities CAPABILITY_IAM` to the `sam deploy` command.


