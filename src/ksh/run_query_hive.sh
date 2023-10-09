#!/bin/bash

# Function to find the config.json file
find_config_file() {
  local current_dir="$PWD"
  while [ "$current_dir" != "/" ]; do
    if [ -f "$current_dir/config/config.json" ]; then
      echo "$current_dir/config/config.json"
      return
    fi
    current_dir="$(dirname "$current_dir")"
  done
}

# Find the config.json file
config_file=$(find_config_file)
echo $config_file
# Check if the config.json file was found
if [ -z "$config_file" ]; then
  echo "Error: config.json file not found in or above the current directory."
  exit 1
fi

# Check if the correct number of arguments are provided
if [ $# -ne 1 ]; then
  echo "Usage: $0 <location>"
  exit 1
fi

# Assign the location argument to a variable
location="$1"

# Read the database name from the JSON file
database=$(jq -r '.database' "$config_file")

# Check if the JSON file contains a valid database name
if [ -z "$database" ]; then
  echo "Error: JSON file does not contain a valid 'database' field."
  exit 1
fi

# Run the Hive query
hive -f "$location" --database $database