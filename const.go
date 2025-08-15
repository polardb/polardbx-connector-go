package polardbx

const (
	basicInfoQuery            = "/* PolarDB-X-Driver HaManager */ select version(), @@cluster_id, @@port;"
	clusterLocalQuery         = "/* PolarDB-X-Driver HaManager */ select CURRENT_LEADER, ROLE from information_schema.alisql_cluster_local limit 1;"
	clusterGlobalQuery        = "/* PolarDB-X-Driver HaManager */ select ROLE, IP_PORT from information_schema.alisql_cluster_global;"
	checkLeaderTransferQuery  = "/* PolarDB-X-Driver HaManager */ show global status like 'consensus_in_leader_transfer';"
	setPingMode               = "/* PolarDB-X-Driver HaManager */ set session ping_mode='IS_LEADER,NOT_IN_LEADER_TRANSFER,NO_CLUSTER_CHANGED';"
	showMppQuery              = "/* PolarDB-X-HA-Driver HaManager */ show mpp;"
	recordDsnQuery            = "/* PolarDB-X-Driver HaManager */ call dbms_conn.comment_connection('%s');"
	clusterHealthQuery        = "/* PolarDB-X-Driver HaManager */ select a.Role, a.IP_PORT from information_schema.alisql_cluster_health a join information_schema.alisql_cluster_global b on a.IP_PORT=b.IP_PORT where a.APPLY_RUNNING='Yes' and a.APPLY_DELAY_SECONDS <= %d and b.ELECTION_WEIGHT > %d"
	setFollowerReadTrue       = "/* PolarDB-X-Driver HaManager */ set session enable_in_memory_follower_read = true;"
	setFollowerReadFalse      = "/* PolarDB-X-Driver HaManager */ set session enable_in_memory_follower_read = false;"
	setReadWeight             = "/* PolarDB-X-Driver HaManager */ set session FOLLOWER_READ_WEIGHT = 100;"
	enableConsistentReadTrue  = "/* PolarDB-X-Driver HaManager */ set session ENABLE_CONSISTENT_REPLICA_READ = true;"
	enableConsistentReadFalse = "/* PolarDB-X-Driver HaManager */ set session ENABLE_CONSISTENT_REPLICA_READ = false;"
)

const dateFormat = "2006-01-02 15:04:05 -0700"

const (
	reset   = "\033[0m"
	red     = "\033[31m"
	green   = "\033[32m"
	yellow  = "\033[33m"
	blue    = "\033[34m"
	magenta = "\033[35m"
	cyan    = "\033[36m"
)

// Dn cluster states
const (
	leaderAlive        = 0
	leaderTransferring = 1
	leaderTransferred  = 2
	leaderLost         = 3
)

// Cn cluster states
const (
	cnAlive = 0
	cnLost  = 1
)

const (
	mysqlNative  = "mysqlNative"
	leaderOnly   = "leaderOnly"
	slaveOnly    = "slaveOnly"
	mix          = "mix"
	mysqlWo      = "mysqlWo"
	leaderOnlyWo = "leaderOnlyWo"
	slaveOnlyWo  = "slaveOnlyWo"
	mixWo        = "mixWo"
)

const (
	random    = "random"
	leastConn = "least_connection"
)

const (
	w  = "W"
	r  = "R"
	cR = "CR"
)

const (
	doNothing           = -1
	readMaster          = 0
	readSlave           = 1
	readConsistentSlave = 2
)
