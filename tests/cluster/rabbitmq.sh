#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
run_id="mq-v2-cluster-$(date +%s)-$$"
network="$run_id"
if [[ -n "${MQ_TEST_RABBIT_IMAGE:-}" ]]; then
  image=$MQ_TEST_RABBIT_IMAGE
elif docker image inspect rabbitmq:3.13.3-management >/dev/null 2>&1; then
  image=rabbitmq:3.13.3-management
elif docker image inspect registry.i.huaxisy.com/library/rabbitmq:3.13.3-management >/dev/null 2>&1; then
  image=registry.i.huaxisy.com/library/rabbitmq:3.13.3-management
else
  image=rabbitmq:3.13.3-management
fi
cookie='mq-v2-disposable-three-node-quorum-cookie'
ports=(5672 5673 5674)

has_three_running_nodes() {
  local status=$1 running
  [[ "$status" == *Running\ Nodes* ]] || return 1
  running=$(printf '%s\n' "$status" | awk '/^Running Nodes$/ {inside=1; next} inside && NF==0 && started {exit} inside && NF>0 {started=1; print}')
  [[ "$running" == *rabbit@r1* && "$running" == *rabbit@r2* && "$running" == *rabbit@r3* ]]
}

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if (( status != 0 )); then
    for node in 1 2 3; do
      docker logs --tail 20 "$run_id-r$node" 2>/dev/null || true
    done
  fi
  for node in 1 2 3; do
    name="$run_id-r$node"
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
for node in 1 2 3; do
  port=${ports[$((node-1))]}
  args=(-p "127.0.0.1:$port:5672" -p "127.0.0.1:$((15671+node)):15672")
  docker run -d --name "$run_id-r$node" --hostname "r$node" \
    --network "$network" --network-alias "r$node" \
    --label "mq.v2.cluster.run=$run_id" "${args[@]}" \
    -e RABBITMQ_ERLANG_COOKIE="$cookie" \
    -e RABBITMQ_NODENAME="rabbit@r$node" \
    "$image" >/dev/null
done

for node in 1 2 3; do
  ready=false
  for attempt in $(seq 1 90); do
    # Starting rabbitmq-diagnostics before the server creates its cookie can
    # create a root-owned 0400 cookie and prevent the server from booting.
    if docker exec "$run_id-r$node" sh -c 'test "$(stat -c %U /var/lib/rabbitmq/.erlang.cookie 2>/dev/null)" = rabbitmq' >/dev/null 2>&1 && \
       docker exec "$run_id-r$node" rabbitmq-diagnostics -q ping >/dev/null 2>&1 && \
       docker exec "$run_id-r$node" rabbitmq-diagnostics -q check_port_listener 5672 >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 1
  done
  if [[ "$ready" != true ]]; then
    echo "RabbitMQ node $node did not become ready" >&2
    exit 1
  fi
done

for node in 2 3; do
  docker exec "$run_id-r$node" rabbitmqctl stop_app
  docker exec "$run_id-r$node" rabbitmqctl reset
  docker exec "$run_id-r$node" rabbitmqctl join_cluster rabbit@r1
  docker exec "$run_id-r$node" rabbitmqctl start_app
done
docker exec "$run_id-r1" rabbitmqctl enable_feature_flag stream_queue

cluster_ready=false
for attempt in $(seq 1 60); do
  status=$(docker exec "$run_id-r1" rabbitmqctl cluster_status 2>/dev/null || true)
  if has_three_running_nodes "$status"; then
    cluster_ready=true
    break
  fi
  sleep 1
done
if [[ "$cluster_ready" != true ]]; then
  echo "RabbitMQ cluster did not show all three members" >&2
  exit 1
fi

if [[ "${MQ_CLUSTER_BENCHMARK:-}" == 1 ]]; then
  echo 'RabbitMQ 3.13.3 three-node quorum confirmed publish benchmark'
  cd "$repo_root"
  MQ_TEST_RABBIT_URL=amqp://guest:guest@127.0.0.1:5672/ \
  go test ./adapters/rabbitmq -run '^$' -bench '^BenchmarkConfirmedBatch$' \
    -benchtime="${MQ_CLUSTER_BENCHTIME:-2s}" -count="${MQ_CLUSTER_BENCH_COUNT:-1}" -benchmem
  exit
fi

export MQ_TEST_CLUSTER_RUN_ID="$run_id"
export MQ_TEST_RABBIT_CLUSTER_URLS='amqp://guest:guest@127.0.0.1:5672/,amqp://guest:guest@127.0.0.1:5673/,amqp://guest:guest@127.0.0.1:5674/'
export MQ_TEST_RABBIT_CLUSTER_MGMT='http://127.0.0.1:15672'
export MQ_TEST_RABBIT_CLUSTER_MGMTS='http://127.0.0.1:15672,http://127.0.0.1:15673,http://127.0.0.1:15674'
cd "$repo_root"
echo 'RabbitMQ 3.13.3 three-node quorum leader failover test'
go test -race ./adapters/rabbitmq -run '^TestClusterQuorumLeaderFailover$' -count=1 -v

for node in 1 2 3; do
  if [[ "$(docker inspect --format '{{.State.Running}}' "$run_id-r$node")" != true ]]; then
    docker start "$run_id-r$node" >/dev/null
  fi
done
restored=false
for attempt in $(seq 1 90); do
  if docker exec "$run_id-r1" rabbitmq-diagnostics -q check_port_listener 5672 >/dev/null 2>&1 && \
     docker exec "$run_id-r2" rabbitmq-diagnostics -q check_port_listener 5672 >/dev/null 2>&1 && \
     docker exec "$run_id-r3" rabbitmq-diagnostics -q check_port_listener 5672 >/dev/null 2>&1; then
    status=$(docker exec "$run_id-r1" rabbitmqctl cluster_status 2>/dev/null || true)
    if has_three_running_nodes "$status"; then
      restored=true
      break
    fi
  fi
  sleep 1
done
if [[ "$restored" != true ]]; then
  echo 'RabbitMQ did not restore all three running cluster nodes' >&2
  exit 1
fi
echo 'RabbitMQ 3.13.3 three-node delayed quorum leader failover test'
go test -race ./delay/rabbitmq -run '^TestClusterDelayedQuorumLeaderFailover$' -count=1 -v
