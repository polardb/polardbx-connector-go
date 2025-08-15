package polardbx

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
)

var (
	dnAddr         = flag.String("dnAddr", "127.0.0.1:3306", "Dn Test Address, multiple addresses separated by commas.")
	cnAddr         = flag.String("cnAddr", "127.0.0.1:3306", "Cn Test Address, multiple addresses separated by commas.")
	dnUser         = flag.String("dnUser", "root", "Dn Test Username.")
	cnUser         = flag.String("cnUser", "root", "Cn Test Address.")
	dnPasswd       = flag.String("dnPasswd", "admin", "Dn Test Password.")
	cnPasswd       = flag.String("cnPasswd", "admin", "Cn Test Password.")
	zoneName       = flag.String("zoneName", "", "Available Zone Name.")
	backupZoneName = flag.String("backupZoneName", "", "Backup Available Zone Name.")
	instanceName   = flag.String("instanceName", "", "Instance Name.")
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func TestMultiIpPort(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True", *dnUser, *dnPasswd, *dnAddr)
	err := queryByDnTemplate(dsn)
	if err != nil {
		log.Fatal(err)
	}
}

func TestClusterId(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&clusterID=1064", *dnUser, *dnPasswd, *dnAddr)
	err := queryByDnTemplate(dsn)
	if err != nil {
		log.Fatal(err)
	}
}

func TestHaTimeParams(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True", *dnUser, *dnPasswd, *dnAddr) +
		"&haCheckConnectTimeoutMillis=3000&haCheckSocketTimeoutMillis=3000&haCheckIntervalMillis=100" +
		"&checkLeaderTransferringIntervalMillis=100&leaderTransferringWaitTimeoutMillis=5000" +
		"&connectTimeout=5000"
	err := queryByDnTemplate(dsn)
	if err != nil {
		log.Fatal(err)
	}
}

func TestRecordJdbcUrl(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&recordJdbcUrl=true", *dnUser, *dnPasswd, *dnAddr)
	err := queryByDnTemplate(dsn)
	if err != nil {
		log.Fatal(err)
	}
}

func TestDnSlaveRead(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&slaveRead=true", *dnUser, *dnPasswd, *dnAddr)
	err := queryByDnTemplate(dsn)
	if err != nil {
		log.Fatal(err)
	}
}

func TestSlaveWeight(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&slaveRead=true&slaveWeightThreshold=10", *dnUser, *dnPasswd, *dnAddr)
	err := queryByDnTemplate(dsn)
	if err == nil {
		log.Fatal("TestSlaveWeight failed: should have ErrNoNodeFound, but not.")
	}
}

func TestApplyDelay(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&slaveRead=true&applyDelayThreshold=3", *dnUser, *dnPasswd, *dnAddr)
	err := queryByDnTemplate(dsn)
	if err != nil {
		log.Fatal(err)
	}
}

func TestDirectMode(t *testing.T) {
	addr := strings.Split(*dnAddr, ",")
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&directMode=true", *dnUser, *dnPasswd, strings.TrimSpace(addr[0]))
	err := queryByDnTemplate(dsn)
	if err != nil {
		log.Fatal(err)
	}
}

func TestLoadBalance1(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&slaveRead=true&loadBalanceAlgorithm=random", *dnUser, *dnPasswd, *dnAddr)
	err := queryByDnTemplate(dsn)
	if err != nil {
		log.Fatal(err)
	}
}

func TestLoadBalance2(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&slaveRead=true&loadBalanceAlgorithm=least_connection", *dnUser, *dnPasswd, *dnAddr)
	err := queryByDnTemplate(dsn)
	if err != nil {
		log.Fatal(err)
	}
}

func TestAllDnParams(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&clusterID=1064", *dnUser, *dnPasswd, *dnAddr) +
		"&haCheckConnectTimeoutMillis=3000&haCheckSocketTimeoutMillis=3000&haCheckIntervalMillis=100" +
		"&checkLeaderTransferringIntervalMillis=100&leaderTransferringWaitTimeoutMillis=5000&connectTimeout=5000" +
		"&slaveRead=true&slaveWeightThreshold=1&applyDelayThreshold=3&loadBalanceAlgorithm=least_connection"
	err := queryByDnTemplate(dsn)
	if err != nil {
		log.Fatal(err)
	}
}

func TestZoneName(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&zoneName=%s", *cnUser, *cnPasswd, *cnAddr, *zoneName)
	_, err := queryByCnTemplate(dsn, true)
	if err != nil {
		log.Fatal(err)
	}
}

func TestMinZoneNodes(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&zoneName=%s&minZoneNodes=1", *cnUser, *cnPasswd, *cnAddr, *zoneName)
	_, err := queryByCnTemplate(dsn, true)
	if err != nil {
		log.Fatal(err)
	}
	dsn = fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&zoneName=%s&minZoneNodes=2", *cnUser, *cnPasswd, *cnAddr, *zoneName)
	_, err = queryByCnTemplate(dsn, true)
	if !errors.Is(err, ErrNoNodeFound) && err != nil {
		log.Fatal(err)
	}
}

func TestBackupZoneName(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&", *cnUser, *cnPasswd, *cnAddr) +
		fmt.Sprintf("zoneName=%s&minZoneNodes=2&backupZoneName=%s", zoneName, backupZoneName)
	_, err := queryByCnTemplate(dsn, true)
	if err != nil {
		log.Fatal(err)
	}
}

func TestCnSlaveRead(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&slaveRead=false", *cnUser, *cnPasswd, *cnAddr)
	_, err := queryByCnTemplate(dsn, true)
	if err != nil {
		log.Fatal(err)
	}
}

func TestInstanceName(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&instanceName=%s", *cnUser, *cnPasswd, *cnAddr, *instanceName)
	_, err := queryByCnTemplate(dsn, true)
	if err != nil {
		log.Fatal(err)
	}
}

func TestMppRole(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&mppRole=W", *cnUser, *cnPasswd, *cnAddr)
	_, err := queryByCnTemplate(dsn, true)
	if err != nil {
		log.Fatal(err)
	}
}

func TestEnableFollowerRead0(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableFollowerRead=0", *cnUser, *cnPasswd, *cnAddr)
	db, err := queryByCnTemplate(dsn, false)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("show variables like '%enable_in_memory_follower_read%'")
	if err != nil {
		log.Fatal(err)
	}
	if rows.Next() {
		log.Fatal("enableFollowerRead should have false, but not.")
	}
}

func TestEnableFollowerRead1(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableFollowerRead=1", *cnUser, *cnPasswd, *cnAddr)
	db, err := queryByCnTemplate(dsn, false)
	if err != nil {
		log.Fatal(err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Printf("Failed to close the database: %v", err)
		}
	}(db)

	var name string
	var enableConsistentReplicaReadVar bool
	err = db.QueryRow("show variables like '%ENABLE_CONSISTENT_REPLICA_READ%'").Scan(&name, &enableConsistentReplicaReadVar)
	if err != nil {
		log.Fatal(err)
	}
	if enableConsistentReplicaReadVar != false {
		log.Fatal("ENABLE_CONSISTENT_REPLICA_READ should have false, but not.")
	}
}

func TestEnableFollowerRead2(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableFollowerRead=2", *cnUser, *cnPasswd, *cnAddr)
	db, err := queryByCnTemplate(dsn, false)
	if err != nil {
		log.Fatal(err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Printf("Failed to close the database: %v", err)
		}
	}(db)

	var name string
	var enableConsistentReplicaReadVar bool
	err = db.QueryRow("show variables like '%ENABLE_CONSISTENT_REPLICA_READ%'").Scan(&name, &enableConsistentReplicaReadVar)
	if err != nil {
		log.Fatal(err)
	}
	if enableConsistentReplicaReadVar != true {
		log.Fatal("ENABLE_CONSISTENT_REPLICA_READ should have true, but not.")
	}
}

func TestAllCnParams(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True", *cnUser, *cnPasswd, *cnAddr) +
		"&haCheckConnectTimeoutMillis=3000&haCheckSocketTimeoutMillis=3000&haCheckIntervalMillis=100" +
		"&checkLeaderTransferringIntervalMillis=100&leaderTransferringWaitTimeoutMillis=5000&connectTimeout=5000" +
		"&slaveRead=false&loadBalanceAlgorithm=least_connection" +
		fmt.Sprintf("&instanceName=%s&zoneName=%s&minZoneNodes=2&backupZoneName=%s", *instanceName, *zoneName, *backupZoneName) +
		"&mppRole=W"
	_, err := queryByCnTemplate(dsn, true)
	if err != nil {
		log.Fatal(err)
	}
	dsn = fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True", *cnUser, *cnPasswd, *cnAddr) +
		"&haCheckConnectTimeoutMillis=3000&haCheckSocketTimeoutMillis=3000&haCheckIntervalMillis=100" +
		"&checkLeaderTransferringIntervalMillis=100&leaderTransferringWaitTimeoutMillis=5000&connectTimeout=5000" +
		fmt.Sprintf("&slaveRead=true&loadBalanceAlgorithm=least_connection&instanceName=%s", *instanceName) +
		"&mppRole=R"
	_, err = queryByCnTemplate(dsn, true)
	if err != nil {
		log.Fatal(err)
	}
}

func queryByDnTemplate(dsn string) (err error) {
	db, err := sql.Open("polardbx", dsn)
	if err != nil {
		return
	}
	defer db.Close()

	var rs *sql.Rows
	rs, err = db.Query("select ROLE, CURRENT_LEADER from information_schema.alisql_cluster_local")
	if err != nil {
		return
	}

	columns, _ := rs.Columns()
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rs.Next() {
		err = rs.Scan(scanArgs...)
		if err != nil {
			return
		}

		rowData := make(map[string]string)
		for i, col := range values {
			rowData[columns[i]] = string(col)
		}

	}
	return
}

func queryByCnTemplate(dsn string, toClose bool) (db *sql.DB, err error) {
	db, err = sql.Open("polardbx", dsn)
	if err != nil {
		return
	}
	if toClose {
		defer db.Close()
	}

	var rs *sql.Rows
	rs, err = db.Query(showMppQuery)
	if err != nil {
		return
	}
	defer rs.Close()

	columns, _ := rs.Columns()
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rs.Next() {
		err = rs.Scan(scanArgs...)
		if err != nil {
			return
		}

		rowData := make(map[string]string)
		for i, col := range values {
			rowData[columns[i]] = string(col)
		}
	}
	return
}
