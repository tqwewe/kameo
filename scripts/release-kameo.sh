#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function for success message
success() {
  echo -e "${GREEN}OK${NC}"
}

# Function for error message and exit
error() {
  echo -e "${RED}ERROR${NC}"
  echo -e "${RED}$1${NC}"
  exit 1
}

# Function to execute a command with prompts and status
execute_step() {
  local message="$1"
  local command="$2"
  
  echo -e "${YELLOW}$message${NC}"
  read -p "Press Enter to continue... " 
  
  eval "$command"
  if [ $? -ne 0 ]; then
    error "Command failed: $command"
  else
    success
  fi
}

# Check if the version argument is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <version>"
  echo "Please provide the new version number as an argument."
  exit 1
fi

NEW_VERSION="$1"
MAJOR_MINOR_VERSION="${NEW_VERSION%.*}"
VERSION_LINK="${NEW_VERSION//./}"
BRANCH=$(git rev-parse --abbrev-ref HEAD)

if [ "$BRANCH" != "main" ]; then
  echo "Publish can only be used when checked out to \`main\` branch. You're currently checked out to \`$BRANCH\`."
  exit 1
fi

execute_step "Updating changelog" "git cliff --tag \"$NEW_VERSION\" --ignore-tags \"\w-\" --prepend ./CHANGELOG.md --unreleased"

execute_step "Updating Cargo.toml package version to $NEW_VERSION" "perl -i -pe \"s/^version = \\\".*\\\"/version = \\\"$NEW_VERSION\\\"/\" ./Cargo.toml"

execute_step "Updating README.md version to $MAJOR_MINOR_VERSION" "perl -i -pe 's/kameo = \"[^\"]*\"/kameo = \"$MAJOR_MINOR_VERSION\"/' README.md"

execute_step "Updating getting-started.mdx version to $MAJOR_MINOR_VERSION" "perl -i -pe 's/kameo = \"[^\"]*\"/kameo = \"$MAJOR_MINOR_VERSION\"/' ./docs/getting-started.mdx"

execute_step "Publishing kameo version $NEW_VERSION" "cargo publish -p kameo --allow-dirty"

execute_step "Creating bump git commit" "git add Cargo.toml CHANGELOG.md README.md docs/getting-started.mdx && git commit -m \"chore: bump kameo to version $NEW_VERSION\""

execute_step "Pushing changes to remote" "git push origin main"

execute_step "Creating git tag v$NEW_VERSION" "git tag -a \"v$NEW_VERSION\" -m \"\""

execute_step "Pushing tag to remote" "git push origin \"v$NEW_VERSION\""

execute_step "Creating CHANGELOG-Release.md for release notes" "git cliff --tag \"$NEW_VERSION\" --output ./CHANGELOG-Release.md --current"

# Extract the release date from the CHANGELOG-Release.md file
RELEASE_DATE=$(grep '^## \['"$NEW_VERSION"'\] - ' CHANGELOG-Release.md | sed -E 's/.* - ([0-9]{4}-[0-9]{2}-[0-9]{2})$/\1/')
if [ -z "$RELEASE_DATE" ]; then
  error "Failed to extract release date from changelog"
fi

execute_step "Processing release notes" "perl -i -ne 'print if /^### / .. eof' CHANGELOG-Release.md && perl -i -ne 'print unless /^\[.*\]:.*$/ .. eof' CHANGELOG-Release.md"

# Append the footer with the extracted release date
echo -e "---\n\nSee the full [CHANGELOG.md](https://github.com/tqwewe/kameo/blob/main/CHANGELOG.md#${VERSION_LINK}---${RELEASE_DATE})" >> CHANGELOG-Release.md

execute_step "Creating GitHub release with changelog" "gh release create \"v$NEW_VERSION\" -F CHANGELOG-Release.md"

# Clean up the temporary file
rm CHANGELOG-Release.md

echo -e "${GREEN}Release process completed successfully!${NC}"
