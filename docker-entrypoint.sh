#!/bin/sh
set -e

# Select GC flag based on JAVA_GC environment variable (default: ZGC)
GC="${JAVA_GC:-${WT4J_GC:-ZGC}}"
case "$(echo "$GC" | tr '[:lower:]' '[:upper:]')" in
  G1|G1GC)
    GC_FLAG="-XX:+UseG1GC"
    ;;
  PARALLEL|PARALLELGC)
    GC_FLAG="-XX:+UseParallelGC"
    ;;
  SERIAL|SERIALGC)
    GC_FLAG="-XX:+UseSerialGC"
    ;;
  SHENANDOAH|SHENANDOAHGC)
    GC_FLAG="-XX:+UseShenandoahGC"
    ;;
  ZGC|GENERATIONAL_ZGC|GENZGC|*)
    GC_FLAG="-XX:+UseZGC"
    ;;
esac

# If user provided an explicit GC flag in JAVA_OPTS, avoid duplicate flags
if echo "$JAVA_OPTS" | grep -q -- "-XX:+Use"; then
  GC_FLAG=""
fi

exec java $GC_FLAG $JAVA_OPTS -cp '/app/webtransport4j.jar:/app/lib/*' io.github.webtransport4j.example.ClusterNodeSample "$@"
