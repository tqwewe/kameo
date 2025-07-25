#!/usr/bin/env bash

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
BRANCH=$(git rev-parse --abbrev-ref HEAD)

if [ "$BRANCH" != "main" ]; then
  echo "Publish can only be used when checked out to \`main\` branch. You're currently checked out to \`$BRANCH\`."
  exit 1
fi

execute_step "Updating kameo_actors Cargo.toml version to $NEW_VERSION" "perl -i -pe \"s/^version = \\\".*\\\"/version = \\\"$NEW_VERSION\\\"/\" ./actors/Cargo.toml"

execute_step "Publishing kameo_actors version $NEW_VERSION" "cargo publish -p kameo_actors --allow-dirty"

execute_step "Creating bump git commit" "git add actors/Cargo.toml && git commit -m \"chore: bump kameo_actors to version $NEW_VERSION\""

execute_step "Pushing changes to remote" "git push origin main"

execute_step "Creating git tag actors-v$NEW_VERSION" "git tag -a \"actors-v$NEW_VERSION\" -m \"Release kameo_actors v$NEW_VERSION\""

execute_step "Pushing tag to remote" "git push origin \"actors-v$NEW_VERSION\""

echo -e "${GREEN}Actors release process completed successfully!${NC}"
