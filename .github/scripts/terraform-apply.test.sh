#!/bin/sh
# Tests terraform-apply.sh with a fake terraform on the PATH.
# Run from the repository root: sh .github/scripts/terraform-apply.test.sh
set -u

script="$(cd "$(dirname "$0")" && pwd)/terraform-apply.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir "$work/bin"

# The fake terraform prints a secret-looking plan line, and then succeeds or
# fails, depending on FAKE_FAIL (init or apply). It records its arguments.
cat > "$work/bin/terraform" <<'FAKE'
#!/bin/sh
echo "$*" >> "$FAKE_CALLS"
case "$1" in
  init)
    echo "Initializing the backend..."
    [ "${FAKE_FAIL:-}" = "init" ] && { printf '╷\n│ Error: backend unreachable\n╵\n'; exit 1; }
    exit 0 ;;
  apply)
    echo '  + data = { "config.yml" = "rtsp://admin@192.168.1.20" }'
    if [ "${FAKE_FAIL:-}" = "apply" ]; then
      printf '╷\n│ Error: creating Deployment: forbidden\n│\n│   with module.frigate.kubernetes_deployment_v1.frigate\n╵\n'
      exit 1
    fi
    echo "Apply complete! Resources: 1 added, 0 changed, 0 destroyed."
    exit 0 ;;
esac
FAKE
chmod +x "$work/bin/terraform"
export PATH="$work/bin:$PATH"
export FAKE_CALLS="$work/calls"

failures=0
# check DESCRIPTION COMMAND...: runs the command and counts a failure.
check() {
  desc=$1
  shift
  if "$@"; then
    echo "ok   $desc"
  else
    echo "FAIL $desc"
    failures=$((failures + 1))
  fi
}
# check runs these two by name, which shellcheck cannot see.
# shellcheck disable=SC2329
has() { printf '%s\n' "$1" | grep -q -- "$2"; }
# shellcheck disable=SC2329
lacks() { ! printf '%s\n' "$1" | grep -q -- "$2"; }

# Success: only the summary line, no plan line, exit code 0, extra arguments passed on.
: > "$FAKE_CALLS"
out=$(FAKE_FAIL='' "$script" -var "a=b")
rc=$?
check "success exits with 0" test "$rc" -eq 0
check "success prints only the summary" test "$out" = "Apply complete! Resources: 1 added, 0 changed, 0 destroyed."
check "extra arguments reach terraform apply" grep -q -- '-var a=b' "$FAKE_CALLS"

# Apply fails: only the error block, never the plan line, exit code 1.
: > "$FAKE_CALLS"
out=$(FAKE_FAIL=apply "$script")
rc=$?
check "a failed apply exits with 1" test "$rc" -eq 1
check "a failed apply prints the error" has "$out" 'Error: creating Deployment: forbidden'
check "a failed apply hides the plan" lacks "$out" '192.168.1.20'

# Init fails: exit code 1, and apply never runs.
: > "$FAKE_CALLS"
FAKE_FAIL=init "$script" >/dev/null
rc=$?
check "a failed init exits with 1" test "$rc" -eq 1
check "a failed init skips apply" lacks "$(cat "$FAKE_CALLS")" '^apply'

exit "$failures"
