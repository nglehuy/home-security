#!/bin/sh
# Runs "terraform init" and "terraform apply" in the current folder, with the
# extra arguments given to this script.
#
# Run logs of a public repository are public. So the script keeps the full
# Terraform output in a temporary file, and prints only the summary line, or
# only the error blocks if a step fails. In an error block, Terraform shows a
# sensitive value as "(sensitive value)". But the error text of a provider can
# contain any value.
set -u

log=$(mktemp)
trap 'rm -f "$log"' EXIT

if terraform init -input=false -no-color >"$log" 2>&1 &&
  terraform apply -input=false -auto-approve -no-color "$@" >>"$log" 2>&1; then
  grep -E '^Apply complete!' "$log"
  exit 0
fi

echo "Terraform failed. Only the error blocks follow."
sed -n '/^╷/,/^╵/p;/^Error/p' "$log"
exit 1
