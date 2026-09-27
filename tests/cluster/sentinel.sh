#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
version=${1:-7.2}
scenario=${2:-adapter}
case "$version" in
  7.2|8.0) ;;
  *) echo "unsupported Redis Sentinel test version: $version" >&2; exit 2 ;;
esac
case "$scenario" in
  adapter|delay|benchmark) ;;
  *) echo "unsupported Redis Sentinel scenario: $scenario" >&2; exit 2 ;;
esac

run_id="mq-v2-cluster-$(date +%s)-$$"
network="$run_id"
master_name=mq-v2-master

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if (( status != 0 )); then
    for node in 1 2 3; do
      docker logs --tail 15 "$run_id-d$node" 2>/dev/null || true
      docker logs --tail 15 "$run_id-s$node" 2>/dev/null || true
    done
  fi
  for node in 1 2 3; do
    for kind in d s; do
      name="$run_id-$kind$node"
      if [[ "$(docker inspect --format '{{index .Config.Labels "mq.v2.cluster.run"}}' "$name" 2>/dev/null || true)" == "$run_id" ]]; then
        docker rm -fv "$name" >/dev/null 2>&1 || true
      fi
    done
  done
  if [[ "$(docker network inspect --format '{{index .Labels "mq.v2.cluster.run"}}' "$network" 2>/dev/null || true)" == "$run_id" ]]; then
    docker network rm "$network" >/dev/null 2>&1 || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

docker network create --label "mq.v2.cluster.run=$run_id" "$network" >/dev/null
for node in 1 2 3; do
  port=$((7100+node))
  replica_args=(--save '')
  if (( node > 1 )); then
    replica_args+=(--replicaof "$master_ip" 7101)
  fi
  docker run -d --name "$run_id-d$node" --network "$network" \
    --label "mq.v2.cluster.run=$run_id" -p "127.0.0.1:$port:$port" \
    "redis:$version" redis-server \
    --port "$port" --bind 0.0.0.0 --protected-mode no \
    --appendonly yes --appendfsync always "${replica_args[@]}" >/dev/null
  if (( node == 1 )); then
    master_ip=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$run_id-d1")
    if [[ -z "$master_ip" ]]; then
      echo 'could not inspect disposable Redis master IP' >&2
      exit 1
    fi
  fi
done

for node in 1 2 3; do
  port=$((7100+node))
  ready=false
  for attempt in $(seq 1 60); do
    if [[ "$(docker exec "$run_id-d$node" redis-cli -p "$port" ping 2>/dev/null || true)" == *PONG* ]]; then
      ready=true
      break
    fi
    sleep 1
  done
  if [[ "$ready" != true ]]; then
    echo "Redis Sentinel data node $node did not become ready" >&2
    exit 1
  fi
done

for node in 1 2 3; do
  port=$((27100+node))
  docker run -d --name "$run_id-s$node" --network "$network" \
    --label "mq.v2.cluster.run=$run_id" -p "127.0.0.1:$port:$port" \
    -e SENTINEL_PORT="$port" -e MASTER_IP="$master_ip" -e MASTER_NAME="$master_name" \
    "redis:$version" sh -c '
      printf "port %s\nbind 0.0.0.0\nprotected-mode no\ndir /tmp\nsentinel monitor %s %s 7101 2\nsentinel down-after-milliseconds %s 1500\nsentinel failover-timeout %s 10000\nsentinel parallel-syncs %s 1\n" \
        "$SENTINEL_PORT" "$MASTER_NAME" "$MASTER_IP" "$MASTER_NAME" "$MASTER_NAME" "$MASTER_NAME" > /tmp/sentinel.conf
      exec redis-server /tmp/sentinel.conf --sentinel
    ' >/dev/null
done

ready=false
for attempt in $(seq 1 90); do
  ready=true
  for node in 1 2 3; do
    port=$((27100+node))
    master=$(docker exec "$run_id-s$node" redis-cli --raw -p "$port" sentinel get-master-addr-by-name "$master_name" 2>/dev/null || true)
    master_info=$(docker exec "$run_id-s$node" redis-cli --raw -p "$port" sentinel master "$master_name" 2>/dev/null || true)
    quorum=$(docker exec "$run_id-s$node" redis-cli --raw -p "$port" sentinel ckquorum "$master_name" 2>/dev/null || true)
    if [[ "$master" != *$'\n7101' || "$master_info" != *$'\nnum-slaves\n2'* || "$quorum" != OK* ]]; then
      ready=false
      break
    fi
  done
  if [[ "$ready" == true ]]; then
    for node in 2 3; do
      port=$((7100+node))
      replica_info=$(docker exec "$run_id-d$node" redis-cli --raw -p "$port" info replication 2>/dev/null || true)
      if [[ "$replica_info" != *role:slave* || "$replica_info" != *master_link_status:up* ]]; then
        ready=false
        break
      fi
    done
  fi
  if [[ "$ready" == true ]]; then
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  echo 'three Redis Sentinels did not establish master, replicas and quorum' >&2
  exit 1
fi

export MQ_TEST_CLUSTER_RUN_ID="$run_id"
unset MQ_TEST_REDIS_ADDR MQ_TEST_REDIS_CLUSTER_ADDR
export MQ_TEST_REDIS_SENTINEL_ADDRS='127.0.0.1:27101,127.0.0.1:27102,127.0.0.1:27103'
export MQ_TEST_REDIS_SENTINEL_MASTER="$master_name"
cd "$repo_root"
case "$scenario" in
  adapter)
    echo "Redis $version Sentinel ordinary primary failover test"
    go test -race ./adapters/redis -run '^TestSentinelPrimaryFailover$' -count=1 -v
    ;;
  delay)
    echo "Redis $version Sentinel delayed primary failover test"
    go test -race ./delay/redis -run '^TestSentinelDelayedPrimaryFailover$' -count=1 -v
    ;;
  benchmark)
    echo "Redis $version Sentinel confirmed publish benchmark"
    go test ./adapters/redis -run '^$' -bench '^BenchmarkConfirmedBatch$' \
      -benchtime="${MQ_CLUSTER_BENCHTIME:-2s}" -count="${MQ_CLUSTER_BENCH_COUNT:-1}" -benchmem
    ;;
esac
