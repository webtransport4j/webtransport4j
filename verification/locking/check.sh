#!/usr/bin/env bash
# Shared local/CI event-loop safety gate. Run from any directory.
set -euo pipefail
cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"

# Keep evidence separate between runs; CI supplies RUNNER_TEMP for artifact upload.
AUDIT_DIR=${RUNNER_TEMP:-$(mktemp -d "${TMPDIR:-/tmp}/wt4j-lock-audit.XXXXXX")}
mkdir -p "$AUDIT_DIR"
echo "Event-loop safety evidence: $AUDIT_DIR"

# Tests regenerate certificates and configuration; Maven clean also removes
# test resources. Never let a validation hook mutate the developer's checkout.
# Copy current file contents (including pre-commit's staged-only working tree),
# not HEAD, and exclude ignored build outputs to avoid stale/IDE-written classes.
AUDIT_WORKTREE=$(mktemp -d "$AUDIT_DIR/worktree.XXXXXX")
git ls-files -z --cached --others --exclude-standard | \
    while IFS= read -r -d '' file; do
        if [ -e "$file" ] || [ -L "$file" ]; then
            printf '%s\0' "$file"
        fi
    done | rsync -a --from0 --files-from=- ./ "$AUDIT_WORKTREE/"
cd "$AUDIT_WORKTREE"

# Audits scan every class file: incremental builds can retain deleted/renamed
# classes, so clean both outputs before compiling the code we actually inspect.
mvn -B -ntp -f verification/locking/pom.xml clean package
mvn -B -ntp -Pbench -Dexec.skip=true -DskipTests clean test-compile
mvn -B -ntp -Pbench -Dexec.skip=true dependency:build-classpath "-Dmdep.outputFile=$AUDIT_DIR/lock-classpath"

java -jar verification/locking/target/lock-audit.jar target-maven/classes
if java -jar verification/locking/target/lock-audit.jar verification/locking/target/classes/io/github/webtransport4j/verification/fixtures > "$AUDIT_DIR/static-negative.log" 2>&1; then
    echo 'Unsafe fixture incorrectly passed' >&2
    exit 1
fi
grep -q 'unguarded' "$AUDIT_DIR/static-negative.log"
java "-javaagent:$PWD/verification/locking/target/lock-audit.jar=$AUDIT_DIR/negative-count" \
    -cp "verification/locking/target/lock-audit.jar:target-maven/classes:$(cat "$AUDIT_DIR/lock-classpath")" \
    io.github.webtransport4j.verification.fixtures.BlockingCallback
test "$(cat "$AUDIT_DIR/negative-count")" = 1

mvn -B -ntp -Pbench,verify-eventloop-locks -Dexec.skip=true \
    "-Dwt4j.lockagent=$PWD/verification/locking/target/lock-audit.jar" \
    "-Dwt4j.lockreport=$AUDIT_DIR/lock-count" \
    -Dtest=EventLoopThreadSafetyTest,ConcurrencyRaceRegressionTest,PublisherConcurrencyTest,MetricsConcurrencyTest,PublisherModelBasedTest,MailboxConcurrencyTest,MailboxModelBasedTest,FlowControlModelBasedTest,ConfigReloadConcurrencyTest,QuicConcurrencyIntegrationTest test
test "$(cat "$AUDIT_DIR/lock-count")" = 0
test "$(cat "$AUDIT_DIR/lock-count.classes")" -gt 0
echo 'Event-loop safety gate passed.'
