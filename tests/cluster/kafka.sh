#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
version=${1:-4.0.2}
case "$version" in
  4.0.2|4.1.2) ;;
  *) echo "unsupported Kafka cluster test version: $version" >&2; exit 2 ;;
esac
run_id="mq-v2-cluster-$(date +%s)-$$"
network="$run_id"
ports=(19092 19093 19094)
image="apache/kafka:$version"

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if (( status != 0 )); then
    for node in 1 2 3; do
      docker logs --tail 20 "$run_id-k$node" 2>/dev/null || true
    done
  fi
  for node in 1 2 3; do
    name="$run_id-k$node"
    if [[ "$(docker inspect --format '{{index .Config.Labels "mq.v2.cluster.run"}}' "$name" 2>/dev/null || true)" == "$run_id" ]]; then
      docker rm -fv "$name" >/dev/null 2>&1 || true
    fi
  done
  if [[ "$(docker network inspect --format '{{index .Labels "mq.v2.cluster.run"}}' "$network" 2>/dev/null || true)" == "$run_id" ]]; then
    docker network rm "$network" >/dev/null 2>&1 || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

docker network create --label "mq.v2.cluster.run=$run_id" "$network" >/dev/null
voters="1@$run_id-k1:9093,2@$run_id-k2:9093,3@$run_id-k3:9093"
for node in 1 2 3; do
  port=${ports[$((node-1))]}
  docker run -d --name "$run_id-k$node" --network "$network" \
    --label "mq.v2.cluster.run=$run_id" -p "127.0.0.1:$port:$port" \
    -e CLUSTER_ID=5L6g3nShT-eMCtK--X86sw \
    -e KAFKA_NODE_ID="$node" \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS="$voters" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
    -e KAFKA_LISTENERS="INTERNAL://:9092,EXTERNAL://:$port,CONTROLLER://:9093" \
    -e KAFKA_ADVERTISED_LISTENERS="INTERNAL://$run_id-k$node:9092,EXTERNAL://127.0.0.1:$port" \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3 \
    -e KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS=3 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2 \
    -e KAFKA_MIN_INSYNC_REPLICAS=2 \
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -e KAFKA_HEAP_OPTS='-Xms256m -Xmx512m' \
    "$image" >/dev/null
done

ready=false
for attempt in $(seq 1 90); do
  if docker exec "$run_id-k1" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$run_id-k1:9092" --list >/dev/null 2>&1 && \
     docker exec "$run_id-k2" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$run_id-k2:9092" --list >/dev/null 2>&1 && \
     docker exec "$run_id-k3" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$run_id-k3:9092" --list >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  echo "Kafka cluster did not become ready" >&2
  exit 1
fi

if [[ "${MQ_CLUSTER_BENCHMARK:-}" == 1 ]]; then
  topic="mq-v2-cluster-bench-$(date +%s)-$$"
  docker exec "$run_id-k1" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$run_id-k1:9092" --create --topic "$topic" \
    --partitions 1 --replication-factor 3 --config min.insync.replicas=2 >/dev/null
  replicated=false
  for attempt in $(seq 1 60); do
    description=$(docker exec "$run_id-k1" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "$run_id-k1:9092" --describe --topic "$topic" 2>/dev/null || true)
    if printf '%s\n' "$description" | awk '/Isr: [0-9]+,[0-9]+,[0-9]+/ {ready=1} END {exit !ready}'; then
      replicated=true
      break
    fi
    sleep 1
  done
  if [[ "$replicated" != true ]]; then
    echo 'Kafka benchmark topic did not reach three ISR' >&2
    exit 1
  fi
  echo "Kafka $version three-node confirmed publish benchmark"
  cd "$repo_root"
  MQ_TEST_KAFKA_BROKERS=127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094 \
  MQ_TEST_KAFKA_TOPIC="$topic" \
  go test ./adapters/kafka -run '^$' -bench '^BenchmarkConfirmedBatch$' \
    -benchtime="${MQ_CLUSTER_BENCHTIME:-2s}" -count="${MQ_CLUSTER_BENCH_COUNT:-1}" -benchmem
  exit
fi

echo "Kafka $version three-node leader failover test"
cd "$repo_root"
MQ_TEST_CLUSTER_RUN_ID="$run_id" \
MQ_TEST_KAFKA_CLUSTER_BROKERS=127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094 \
go test -race ./adapters/kafka -run '^TestClusterLeaderFailover$' -count=1 -v
