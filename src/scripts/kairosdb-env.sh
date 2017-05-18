# JVM uses only 1/4 of system memory by default
DEFAULT_MEM_JAVA_PERCENT=80

if [ -z "$MEM_JAVA_PERCENT" ]; then
    MEM_JAVA_PERCENT=$DEFAULT_MEM_JAVA_PERCENT
fi

MEM_TOTAL_KB=$(cat /proc/meminfo | grep MemTotal | awk '{print $2}')
MEM_JAVA_KB=$(($MEM_TOTAL_KB * $MEM_JAVA_PERCENT / 100))

JAVA_OPTS="$JAVA_OPTS -Xms${MEM_JAVA_KB}k -Xmx${MEM_JAVA_KB}k"
