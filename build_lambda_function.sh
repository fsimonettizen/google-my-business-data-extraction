#!/bin/bash

echo "AWS Lambda Google My Business Insights Data extraction update function"

echo ""
echo "organizing files..."

mkdir deploy
cp -r source/rsa deploy/
cp -r source/pyasn1_modules deploy/
cp -r source/psycopg2 deploy/
cp -r source/pyasn1 deploy/
cp -r source/oauth2client deploy/
cp -r source/httplib2 deploy/
cp -r source/idna deploy/
cp -r source/certifi deploy/
cp -r source/chardet deploy/
cp -r source/urllib3 deploy/
cp -r source/requests deploy/

cp source/*.py deploy/
cp source/client_secrets.json deploy/
cp source/output_client_secret.googleusercontent.com.json deploy/
cp source/credentials deploy/
cd deploy 

echo ""
echo "zipping files..."

zip -FSj source.zip *.py client_secrets.json output_client_secret.googleusercontent.com.json credentials
zip -r source.zip rsa pyasn1_modules psycopg2 pyasn1 oauth2client httplib2 psycopg2 idna certifi chardet urllib3 requests

echo ""
echo "uploading aws lambda function..."
# aws lambda update-function-code --function-name teste-rds --zip-file "fileb://source.zip" --debug --cli-connect-timeout 6000
aws lambda update-function-code --function-name gmb-extract --zip-file "fileb://source.zip" --debug --cli-connect-timeout 6000 --profile=default

cd ..
rm -rf deploy

echo ""
echo "Finish"