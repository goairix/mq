#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
version=${1:-7.2}
scenario=${2:-adapter}
case "$version" in
  7.2|8.0) ;;
  *) echo "unsupported Redis cluster test version: $version" >&2; exit 2 ;;
esac
case "$scenario" in
  adapter|delay|benchmark) ;;
  *) echo "unsupported Redis cluster test scenario: $scenario" >&2; exit 2 ;;
esac
run_id="mq-v2-cluster-$(date +%s)-$$"
network="$run_id"

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if (( status != 0 )); then
    for node in 1 2 3 4 5 6; do
      docker logs --tail 15 "$run_id-d$node" 2>/dev/null || true
    done
  fi
  for node in 1 2 3 4 5 6; do
    name="$run_id-d$node"
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
for node in 1 2 3 4 5 6; do
  port=$((7000+node))
  docker run -d --name "$run_id-d$node" --network "$network" \
    --label "mq.v2.cluster.run=$run_id" -p "127.0.0.1:$port:$port" \
    "redis:$version" redis-server \
    --port "$port" --bind 0.0.0.0 --protected-mode no \
    --cluster-enabled yes --cluster-config-file nodes.conf \
    --cluster-node-timeout 1000 \
    --appendonly yes --appendfsync always >/dev/null
done

for node in 1 2 3 4 5 6; do
  ready=false
  for attempt in $(seq 1 60); do
    if [[ "$(docker exec "$run_id-d$node" redis-cli -p "$((7000+node))" ping 2>/dev/null || true)" == *PONG* ]]; then
      ready=true
      break
    fi
    sleep 1
  done
  if [[ "$ready" != true ]]; then
    echo "Redis node $node did not become ready" >&2
    exit 1
  fi
done

docker exec "$run_id-d1" redis-cli --cluster create \
  "$run_id-d1:7001" "$run_id-d2:7002" "$run_id-d3:7003" \
  "$run_id-d4:7004" "$run_id-d5:7005" "$run_id-d6:7006" \
  --cluster-replicas 1 --cluster-yes >/dev/null

ready=false
for attempt in $(seq 1 90); do
  state=$(docker exec "$run_id-d1" redis-cli -p 7001 cluster info 2>/dev/null || true)
  if [[ "$state" == *cluster_state:ok* && "$state" == *cluster_known_nodes:6* ]]; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  echo "six-node Redis Cluster did not become ready" >&2
  exit 1
fi

export MQ_TEST_CLUSTER_RUN_ID="$run_id"
export MQ_TEST_REDIS_CLUSTER_ADDR='127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005,127.0.0.1:7006'
cd "$repo_root"
if [[ "$scenario" == benchmark ]]; then
  echo "Redis $version six-node confirmed publish benchmark"
  go test ./adapters/redis -run '^$' -bench '^BenchmarkConfirmedBatch$' \
    -benchtime="${MQ_CLUSTER_BENCHTIME:-2s}" -count="${MQ_CLUSTER_BENCH_COUNT:-1}" -benchmem
elif [[ "$scenario" == adapter ]]; then
  echo "Redis $version six-node adapter primary failover test"
  go test -race ./adapters/redis -run '^TestClusterPrimaryFailover$' -count=1 -v
else
  echo "Redis $version six-node delay primary failover test"
  go test -race ./delay/redis -run '^TestClusterDelayedPrimaryFailover$' -count=1 -v
fi
