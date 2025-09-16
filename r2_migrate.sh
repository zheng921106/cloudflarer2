#!/usr/bin/env bash
set -euo pipefail

########################################
# 可配参数
########################################
SRC_REMOTE="${SRC_REMOTE:-r2push}"
DST_REMOTE="${DST_REMOTE:-r2dst}"
SRC_BUCKET="${SRC_BUCKET:-yasyadong001}"
DST_BUCKET="${DST_BUCKET:-yas003}"

# 每轮同步间隔（秒）：持续有新文件时建议 60~300
SYNC_INTERVAL="${SYNC_INTERVAL:-120}"

# 仅搬运“至少创建/修改 N 秒之前”的文件，避免搬到还在写入的半成品
MIN_AGE="${MIN_AGE:-60s}"

# 并发/速率控制（先保守，稳定后可逐步增大）
TRANSFERS="${TRANSFERS:-2}"
CHECKERS="${CHECKERS:-4}"
UPLOAD_CONC="${UPLOAD_CONC:-2}"
BW_LIMIT="${BW_LIMIT:-10M}"
TPS_LIMIT="${TPS_LIMIT:-8}"           # 每秒请求数上限，降低触发风控概率
CHUNK_SIZE="${CHUNK_SIZE:-32M}"
COPY_CUTOFF="${COPY_CUTOFF:-256M}"

# 退避重试设置
MAX_RETRIES="${MAX_RETRIES:-8}"
RETRIES_SLEEP_BASE="${RETRIES_SLEEP_BASE:-10}"   # 秒，指数退避基数
RETRIES_SLEEP_MAX="${RETRIES_SLEEP_MAX:-600}"    # 最大退避 10 分钟

# 校验频率：每隔 N 轮同步后做一次 rclone check
VERIFY_EVERY_N_ROUNDS="${VERIFY_EVERY_N_ROUNDS:-10}"

# 运行/日志目录
RUN_DIR="${RUN_DIR:-./run}"
LOG_DIR="${LOG_DIR:-./logs}"
mkdir -p "${RUN_DIR}" "${LOG_DIR}"
LOG_FILE="${LOG_FILE:-${LOG_DIR}/r2_migrate.$(date +%F).log}"
PID_FILE="${PID_FILE:-${RUN_DIR}/r2_migrate.pid}"
LOCK_FILE="${LOCK_FILE:-${RUN_DIR}/r2_migrate.lock}"

# 退出清理（仅前台/子进程使用）
cleanup() {
  echo "[$(date '+%F %T')] 收到退出信号，优雅退出。" | tee -a "$LOG_FILE"
  # 清理 lock：仅当我们持有锁文件描述符 9 时尝试删除（后台启动时用 flock 占用）
  # 这里不主动删 LOCK_FILE，本身由 flock 释放；但可安全移除 PID
  if [[ -f "$PID_FILE" ]] && [[ "$(cat "$PID_FILE" 2>/dev/null || true)" == "$$" ]]; then
    rm -f "$PID_FILE"
  fi
  exit 0
}
trap cleanup INT TERM

########################################
# 公共 rclone 参数
########################################
COMMON_FLAGS=(
  --fast-list
  --metadata
  --size-only
  --s3-upload-concurrency="${UPLOAD_CONC}"
  --transfers="${TRANSFERS}"
  --checkers="${CHECKERS}"
  --bwlimit="${BW_LIMIT}"
  --tpslimit="${TPS_LIMIT}"
  --s3-chunk-size="${CHUNK_SIZE}"
  --s3-copy-cutoff="${COPY_CUTOFF}"
  --retries="${MAX_RETRIES}"
  --low-level-retries=20
  --progress
)

# 仅同步“稳定文件”
AGE_FLAGS=( --min-age "${MIN_AGE}" )

########################################
# 单次同步
########################################
sync_once() {
  echo "[$(date '+%F %T')] 开始同步（min-age=${MIN_AGE}, interval=${SYNC_INTERVAL}s）..." | tee -a "$LOG_FILE"
  if rclone copy "${SRC_REMOTE}:${SRC_BUCKET}" "${DST_REMOTE}:${DST_BUCKET}" "${COMMON_FLAGS[@]}" "${AGE_FLAGS[@]}" 2>&1 | tee -a "$LOG_FILE"; then
    echo "[$(date '+%F %T')] 同步成功。" | tee -a "$LOG_FILE"
    return 0
  else
    echo "[$(date '+%F %T')] 同步失败（将触发退避重试）。" | tee -a "$LOG_FILE"
    return 1
  fi
}

########################################
# 指数退避
########################################
backoff_retry() {
  local attempt=1
  while (( attempt <= MAX_RETRIES )); do
    local sleep_sec=$(( RETRIES_SLEEP_BASE * (2 ** (attempt - 1)) ))
    (( sleep_sec > RETRIES_SLEEP_MAX )) && sleep_sec=${RETRIES_SLEEP_MAX}
    echo "[$(date '+%F %T')] 第 ${attempt}/${MAX_RETRIES} 次重试前等待 ${sleep_sec}s..." | tee -a "$LOG_FILE"
    sleep "${sleep_sec}"
    if sync_once; then
      return 0
    fi
    attempt=$(( attempt + 1 ))
  done
  echo "[$(date '+%F %T')] 达到最大重试次数，进入下一轮周期。" | tee -a "$LOG_FILE"
  return 1
}

########################################
# 定期一致性校验（不删除目标端）
########################################
verify_once() {
  echo "[$(date '+%F %T')] 开始一致性校验（one-way/size-only）..." | tee -a "$LOG_FILE"
  rclone check "${SRC_REMOTE}:${SRC_BUCKET}" "${DST_REMOTE}:${DST_BUCKET}" \
    --one-way --size-only --checkers=$((CHECKERS*2)) --progress 2>&1 | tee -a "$LOG_FILE" || true
  echo "[$(date '+%F %T')] 校验完成。" | tee -a "$LOG_FILE"
}

########################################
# 启动前的一次 Dry-run（可注释掉）
########################################
dry_run() {
  echo "[$(date '+%F %T')] Dry-run 预演..." | tee -a "$LOG_FILE"
  rclone copy "${SRC_REMOTE}:${SRC_BUCKET}" "${DST_REMOTE}:${DST_BUCKET}" --dry-run "${COMMON_FLAGS[@]}" "${AGE_FLAGS[@]}" 2>&1 | tee -a "$LOG_FILE" || true
  echo "[$(date '+%F %T')] Dry-run 完成。" | tee -a "$LOG_FILE"
}

########################################
# 主循环：持续同步新文件
########################################
main_loop() {
  dry_run
  local round=0
  # 将自身 PID 写入文件，便于 stop/status
  echo "$$" > "$PID_FILE"
  # 忽略 SIGHUP，防止会话挂断影响（即使不是 nohup，也更稳）
  trap '' HUP

  while true; do
    round=$((round + 1))
    echo "==================== Round ${round} ====================" | tee -a "$LOG_FILE"

    if ! sync_once; then
      backoff_retry || true
    fi

    # 周期性做一次一致性校验
    if (( round % VERIFY_EVERY_N_ROUNDS == 0 )); then
      verify_once
    fi

    echo "[$(date '+%F %T')] 休眠 ${SYNC_INTERVAL}s，等待下一轮..." | tee -a "$LOG_FILE"
    sleep "${SYNC_INTERVAL}"
  done
}

########################################
# 守护进程控制：start/stop/status/foreground
########################################
is_running() {
  local pid="$1"
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

cmd_start() {
  # 防重入：pid 存在且进程在，直接返回
  if [[ -f "$PID_FILE" ]]; then
    local oldpid
    oldpid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if is_running "${oldpid:-}"; then
      echo "已在后台运行中（PID=${oldpid}），无需重复启动。日志：${LOG_FILE}"
      exit 0
    else
      rm -f "$PID_FILE"
    fi
  fi

  # 使用 flock 保证单实例，nohup+setsid+disown 彻底脱离终端
  # shellcheck disable=SC2009
  {
    echo "[$(date '+%F %T')] 以后台模式启动。日志：${LOG_FILE}"
    :
  } | tee -a "$LOG_FILE"

  # 在一个子 bash 中持有锁并执行主循环
  nohup bash -c '
    exec 9>"'"$LOCK_FILE"'"
    if ! flock -n 9; then
      echo "['"$(date '+%F %T')"'] 已有实例持有锁：'"$LOCK_FILE"'" >> "'"$LOG_FILE"'"
      exit 1
    fi
    # 将子进程 PID 写入 PID_FILE
    echo "$$" > "'"$PID_FILE"'"
    # 运行主循环
    '"$(declare -f dry_run sync_once backoff_retry verify_once main_loop cleanup)"'
    trap cleanup INT TERM
    main_loop
  ' >>"$LOG_FILE" 2>&1 &

  disown || true
  sleep 0.2
  if [[ -f "$PID_FILE" ]]; then
    echo "启动成功，PID=$(cat "$PID_FILE"). 日志：$LOG_FILE"
  else
    echo "启动失败，请查看日志：$LOG_FILE"
    exit 1
  fi
}

cmd_foreground() {
  # 前台模式：拿锁，直接运行 main_loop
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "另一实例正在运行（锁：$LOCK_FILE）。" | tee -a "$LOG_FILE"
    exit 1
  fi
  echo "$$" > "$PID_FILE"
  echo "以前台模式运行（Ctrl+C 退出）。日志：$LOG_FILE" | tee -a "$LOG_FILE"
  main_loop
}

cmd_stop() {
  if [[ ! -f "$PID_FILE" ]]; then
    echo "未发现 PID 文件，可能未运行。"
    exit 0
  fi
  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -z "${pid:-}" ]]; then
    echo "PID 文件为空，清理后退出。"
    rm -f "$PID_FILE"
    exit 0
  fi
  if ! is_running "$pid"; then
    echo "进程 PID=$pid 不存在，清理 PID 文件。"
    rm -f "$PID_FILE"
    exit 0
  fi
  echo "发送 SIGTERM 给 PID=$pid，等待优雅退出..."
  kill -TERM "$pid" || true
  for i in {1..30}; do
    if ! is_running "$pid"; then
      echo "已停止。"
      rm -f "$PID_FILE"
      exit 0
    fi
    sleep 1
  done
  echo "超时未退出，发送 SIGKILL..."
  kill -KILL "$pid" || true
  rm -f "$PID_FILE"
  echo "已强制停止。"
}

cmd_status() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if is_running "${pid:-}"; then
      echo "运行中：PID=$pid，日志：$LOG_FILE"
      exit 0
    fi
  fi
  echo "未运行。"
  exit 1
}

########################################
# 入口
########################################
usage() {
  cat <<EOF
用法：
  $0 start         # 后台运行（默认）
  $0 stop          # 停止后台实例
  $0 status        # 查看运行状态
  $0 foreground    # 以前台方式运行（调试）

环境变量可覆盖：
  RUN_DIR, LOG_DIR, LOG_FILE, PID_FILE, LOCK_FILE,
  SYNC_INTERVAL, MIN_AGE, TRANSFERS, CHECKERS, UPLOAD_CONC,
  BW_LIMIT, TPS_LIMIT, CHUNK_SIZE, COPY_CUTOFF, MAX_RETRIES,
  RETRIES_SLEEP_BASE, RETRIES_SLEEP_MAX, VERIFY_EVERY_N_ROUNDS,
  SRC_REMOTE, DST_REMOTE, SRC_BUCKET, DST_BUCKET
EOF
}

SUBCMD="${1:-start}"
case "$SUBCMD" in
  start)       shift || true; cmd_start "$@";;
  stop)        shift || true; cmd_stop;;
  status)      shift || true; cmd_status;;
  foreground)  shift || true; cmd_foreground;;
  -h|--help|help) usage;;
  *) echo "未知子命令：$SUBCMD"; usage; exit 1;;
esac
