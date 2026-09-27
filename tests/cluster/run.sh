#!/usr/bin/env bash
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
backend=${1:-all}
version=${2:-}

# The public selector controls the mode even if the caller has this internal
# child-runner switch in its environment.
if [[ "$backend" != bench ]]; then
  unset MQ_CLUSTER_BENCHMARK
fi

case "$backend" in
  bench)
    benchmark_backend=${2:-all}
    benchmark_version=${3:-}
    case "$benchmark_backend" in
      all)
        if [[ -n "$benchmark_version" ]]; then
          echo 'bench all does not accept a version argument' >&2
          exit 2
        fi
        for kafka_version in 4.0.2 4.1.2; do
          MQ_CLUSTER_BENCHMARK=1 bash "$here/kafka.sh" "$kafka_version"
        done
        MQ_CLUSTER_BENCHMARK=1 bash "$here/rabbitmq.sh"
        for redis_version in 7.2 8.0; do
          bash "$here/redis.sh" "$redis_version" benchmark
          bash "$here/sentinel.sh" "$redis_version" benchmark
        done
        ;;
      kafka)
        MQ_CLUSTER_BENCHMARK=1 bash "$here/kafka.sh" "${benchmark_version:-4.0.2}"
        ;;
      rabbitmq)
        if [[ -n "$benchmark_version" ]]; then
          echo 'bench rabbitmq does not accept a version argument' >&2
          exit 2
        fi
        MQ_CLUSTER_BENCHMARK=1 bash "$here/rabbitmq.sh"
        ;;
      redis)
        bash "$here/redis.sh" "${benchmark_version:-7.2}" benchmark
        ;;
      sentinel)
        bash "$here/sentinel.sh" "${benchmark_version:-7.2}" benchmark
        ;;
      *)
        echo 'usage: bash tests/cluster/run.sh bench [all|kafka|rabbitmq|redis|sentinel] [version]' >&2
        exit 2
        ;;
    esac
    ;;
  all)
    if [[ -n "$version" ]]; then
      echo 'all does not accept a version argument' >&2
      exit 2
    fi
    bash "$here/kafka.sh" 4.0.2
    bash "$here/kafka.sh" 4.1.2
    bash "$here/rabbitmq.sh"
    for redis_version in 7.2 8.0; do
      bash "$here/redis.sh" "$redis_version" adapter
      bash "$here/redis.sh" "$redis_version" delay
      bash "$here/sentinel.sh" "$redis_version" adapter
      bash "$here/sentinel.sh" "$redis_version" delay
    done
    ;;
  kafka)
    bash "$here/kafka.sh" "${version:-4.0.2}"
    ;;
  rabbitmq)
    if [[ -n "$version" ]]; then
      echo 'rabbitmq uses the pinned 3.13.3 image; no version argument is accepted' >&2
      exit 2
    fi
    bash "$here/rabbitmq.sh"
    ;;
  redis)
    for scenario in adapter delay; do
      bash "$here/redis.sh" "${version:-7.2}" "$scenario"
    done
    ;;
  sentinel)
    for scenario in adapter delay; do
      bash "$here/sentinel.sh" "${version:-7.2}" "$scenario"
    done
    ;;
  *)
    echo 'usage: bash tests/cluster/run.sh [all|kafka|rabbitmq|redis|sentinel|bench] [backend or version] [version]' >&2
    exit 2
    ;;
esac
