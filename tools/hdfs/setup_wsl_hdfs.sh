#!/usr/bin/env bash
# DTSMS — real single-node HDFS (Apache Hadoop) inside WSL2 Ubuntu.
#
# Run once from Windows, in the project folder (PowerShell):
#   wsl -d Ubuntu -u root -- bash tools/hdfs/setup_wsl_hdfs.sh
# Then start/stop it with tools/hdfs/hdfs.ps1 (start.ps1 does this automatically).
#
# What it does (safe to re-run, finished steps are skipped):
#   1. installs OpenJDK 11 (Hadoop 3.4's Java runtime)
#   2. downloads Apache Hadoop from downloads.apache.org and checks its published SHA-512
#   3. writes a pseudo-distributed config: NameNode hdfs://127.0.0.1:9000, WebHDFS on :9870,
#      one DataNode registered as 127.0.0.1 so Windows (Flask via WebHDFS, Spark via hdfs://) can reach it
#   4. formats the NameNode the first time and installs /usr/local/bin/dtsms-hdfs (start | stop | status)
set -euo pipefail

HADOOP_VERSION="${HADOOP_VERSION:-3.4.3}"
PREFIX=/opt/hadoop
DATA=/var/lib/dtsms-hdfs
RUN_USER="${RUN_USER:-$(getent passwd 1000 | cut -d: -f1)}"
MIRROR="https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}"
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

[ "$(id -u)" = 0 ] || { echo "Run as root:  wsl -d Ubuntu -u root -- bash $0" >&2; exit 1; }
if [ -z "$RUN_USER" ]; then                     # distro without a regular user yet: one just for the daemons
  RUN_USER=dtsms
  id "$RUN_USER" >/dev/null 2>&1 || useradd -m -s /bin/bash "$RUN_USER"
fi
echo "DTSMS HDFS setup: Hadoop ${HADOOP_VERSION}, daemons run as '${RUN_USER}'"

# A new Ubuntu runs its automatic updates on its first start and holds apt's lock meanwhile: wait for it
# (up to 15 minutes) instead of failing on "Could not get lock /var/lib/apt/lists/lock".
apt_wait() {
  local err; err="$(mktemp)"
  for i in $(seq 1 90); do
    if "$@" >/dev/null 2>"$err"; then rm -f "$err"; return 0; fi
    if grep -qi "lock" "$err"; then
      [ "$i" = 1 ] && echo "      Ubuntu is installing its own updates (normal on a new Ubuntu); waiting for them to finish…"
      sleep 10
    else
      cat "$err" >&2; rm -f "$err"; return 1
    fi
  done
  echo "apt was still locked after 15 minutes; run the setup again later" >&2; rm -f "$err"; return 1
}

# 1. Java 11
if [ ! -x "$JAVA_HOME/bin/java" ]; then
  echo "[1/4] Installing OpenJDK 11…"
  apt_wait apt-get update -qq
  apt_wait env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq openjdk-11-jdk-headless curl
else
  echo "[1/4] OpenJDK 11 already installed"
fi

# 2. Hadoop, verified against the published SHA-512
if [ ! -x "$PREFIX/bin/hdfs" ]; then
  echo "[2/4] Downloading hadoop-${HADOOP_VERSION}.tar.gz (~515 MB)…"
  tmp="$(mktemp -d)"
  curl -fSL --retry 3 --progress-bar -o "$tmp/hadoop.tgz" "$MIRROR/hadoop-${HADOOP_VERSION}.tar.gz"
  curl -fsSL -o "$tmp/hadoop.sha512" "$MIRROR/hadoop-${HADOOP_VERSION}.tar.gz.sha512"
  want="$(tr -d ' \r\n' < "$tmp/hadoop.sha512" | grep -oiE '[0-9a-f]{128}' | head -1 | tr 'A-F' 'a-f')"
  got="$(sha512sum "$tmp/hadoop.tgz" | cut -d' ' -f1)"
  if [ -z "$want" ] || [ "$want" != "$got" ]; then
    echo "SHA-512 mismatch — download rejected" >&2; rm -rf "$tmp"; exit 1
  fi
  echo "      SHA-512 verified"
  mkdir -p "$PREFIX"
  tar -xzf "$tmp/hadoop.tgz" -C "$PREFIX" --strip-components=1
  rm -rf "$tmp"
else
  echo "[2/4] Hadoop already installed at $PREFIX"
fi

# 3. Configuration
echo "[3/4] Writing configuration…"
mkdir -p "$DATA"/{name,data,tmp,logs,pids}
cat > "$PREFIX/etc/hadoop/core-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://127.0.0.1:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>${DATA}/tmp</value></property>
  <property><name>hadoop.http.staticuser.user</name><value>${RUN_USER}</value></property>
</configuration>
EOF
cat > "$PREFIX/etc/hadoop/hdfs-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://${DATA}/name</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://${DATA}/data</value></property>
  <property><name>dfs.permissions.enabled</name><value>false</value></property>
  <property><name>dfs.webhdfs.enabled</name><value>true</value></property>
  <!-- listen on all interfaces so Windows reaches the daemons through WSL's localhost forwarding -->
  <property><name>dfs.namenode.rpc-bind-host</name><value>0.0.0.0</value></property>
  <property><name>dfs.namenode.http-bind-host</name><value>0.0.0.0</value></property>
  <!-- the DataNode registers as 127.0.0.1, so WebHDFS redirects and Spark block reads work from Windows
       ("localhost" makes Windows clients try IPv6 first, which WSL does not forward: +2 s per request) -->
  <property><name>dfs.datanode.hostname</name><value>127.0.0.1</value></property>
  <property><name>dfs.namenode.datanode.registration.ip-hostname-check</name><value>false</value></property>
</configuration>
EOF
sed -i '/^# DTSMS$/,$d' "$PREFIX/etc/hadoop/hadoop-env.sh"
cat >> "$PREFIX/etc/hadoop/hadoop-env.sh" <<EOF
# DTSMS
export JAVA_HOME=${JAVA_HOME}
export HADOOP_LOG_DIR=${DATA}/logs
export HADOOP_PID_DIR=${DATA}/pids
EOF
chown -R "$RUN_USER":"$RUN_USER" "$DATA" "$PREFIX"

# 4. Format once, and a control command
if [ ! -d "$DATA/name/current" ]; then
  echo "[4/4] Formatting the NameNode…"
  su -s /bin/bash "$RUN_USER" -c "JAVA_HOME=$JAVA_HOME $PREFIX/bin/hdfs namenode -format -nonInteractive -clusterId dtsms" >/dev/null 2>&1
else
  echo "[4/4] NameNode already formatted"
fi
cat > /usr/local/bin/dtsms-hdfs <<EOF
#!/usr/bin/env bash
# DTSMS single-node HDFS:  dtsms-hdfs start | stop | status
# the daemons run as ${RUN_USER} (owner of the data folders), also when WSL's default user is root
if [ "\$(id -u)" = 0 ]; then exec su -s /bin/bash ${RUN_USER} -c "/usr/local/bin/dtsms-hdfs \${1:-status}"; fi
export JAVA_HOME=${JAVA_HOME} HADOOP_HOME=${PREFIX}
H=\$HADOOP_HOME/bin/hdfs
case "\${1:-status}" in
  start)  nohup \$H --daemon start namenode >/dev/null 2>&1
          nohup \$H --daemon start datanode >/dev/null 2>&1
          for i in \$(seq 1 60); do
            curl -sf "http://127.0.0.1:9870/webhdfs/v1/?op=GETFILESTATUS" >/dev/null && \$H dfsadmin -safemode get 2>/dev/null | grep -q OFF && break
            sleep 1
          done
          \$H dfs -mkdir -p /traffic/input /traffic/output 2>/dev/null; echo "HDFS running" ;;
  stop)   \$H --daemon stop datanode; \$H --daemon stop namenode; echo "HDFS stopped" ;;
  status) for d in namenode datanode; do
            if [ -f ${DATA}/pids/hadoop-\$(whoami)-\$d.pid ] && kill -0 "\$(cat ${DATA}/pids/hadoop-\$(whoami)-\$d.pid)" 2>/dev/null; then echo "\$d: running"; else echo "\$d: stopped"; fi
          done ;;
  *)      echo "usage: dtsms-hdfs start | stop | status"; exit 1 ;;
esac
EOF
chmod +x /usr/local/bin/dtsms-hdfs
echo "Done. Start it with:  wsl -d Ubuntu -u ${RUN_USER} -- dtsms-hdfs start   (or tools/hdfs/hdfs.ps1 start)"
