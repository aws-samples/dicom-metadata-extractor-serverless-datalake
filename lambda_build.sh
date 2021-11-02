#!/bin/bash
set -e
echo "Start Deployment Process"
if [ ! -f "samconfig.toml" ]; then
    sam build -t deploy/serverless.yml -m requirements.txt && sam deploy --capabilities CAPABILITY_NAMED_IAM --guided
else
    CURRENT_VERSION=$(awk -F '[ =]+' '$1 == "parameter_overrides" { print $27 }' samconfig.toml)
    CURRENT_VERSION=$(echo $CURRENT_VERSION | tr -d '[:punct:]')
    NEW_VERSION=$CURRENT_VERSION
    ((NEW_VERSION++))
    # Ensures to always rebuild image for code change for version updates regardless of templates changes
    echo $NEW_VERSION > src/VERSION        
    echo "Increment Version Number"
    echo "Updating VersionDescription=$CURRENT_VERSION to VersionDescription=$NEW_VERSION"
    sam build -t deploy/serverless.yml 
    sed -i .bak 's/VersionDescription=\\\"'"$CURRENT_VERSION"'\\\"/VersionDescription=\\\"'"$NEW_VERSION"'\\\"/' samconfig.toml
    sam deploy --no-confirm-changeset
fi