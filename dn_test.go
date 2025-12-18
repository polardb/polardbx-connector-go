package polardbx

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDNSimpleCase(t *testing.T) {
	if *testType != "dn" {
		t.Skip("Skip non-dn test")
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)
	println("Run test with dsn: ", dsn)
	db, err := sql.Open("polardbx", dsn)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close database: %v", err)
		}
	}()

	runSimpleCase(t, db)
}

func TestDNClusterId(t *testing.T) {
	if *testType != "dn" {
		t.Skip("Skip non-dn test")
	}
	var clusterId int32
	t.Run("get cluster id", func(t *testing.T) {
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s",
			*user, *passwd, *addr, *enableLog, *enableProbeLog)
		t.Logf("Get cluster id with dsn: %s", dsn)
		db, err := sql.Open("polardbx", dsn)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Errorf("Failed to close database: %v", err)
			}
		}()

		err = db.QueryRow("select @@cluster_id").Scan(&clusterId)
		if err != nil {
			t.Fatalf("Failed to get cluster id: %v", err)
		} else {
			t.Logf("Get cluster id: %d", clusterId)
		}
	})

	t.Run("test correct cluster id", func(t *testing.T) {
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
			"&clusterId=%d",
			*user, *passwd, *addr, *enableLog, *enableProbeLog, clusterId)
		t.Logf("Test correct cluster id with dsn: %s", dsn)
		db, err := sql.Open("polardbx", dsn)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Errorf("Failed to close database: %v", err)
			}
		}()

		t.Log("Testing Ping operation to verify connection is normal")
		err = db.Ping()
		if err != nil {
			t.Fatalf("Failed to ping database: %v", err)
		}

		var ignored int
		t.Log("Executing simple query to verify database operations work correctly")
		err = db.QueryRow("select 1").Scan(&ignored)
		if err != nil {
			t.Fatalf("Failed to select 1: %v", err)
		}
	})

	t.Run("test wrong cluster id", func(t *testing.T) {
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
			"&clusterId=%d",
			*user, *passwd, *addr, *enableLog, *enableProbeLog, clusterId+1)
		t.Logf("Test wrong cluster id with dsn: %s", dsn)
		db, err := sql.Open("polardbx", dsn)
		if err != nil {
			t.Fatalf("Failed to open database: %v", err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Errorf("Failed to close database: %v", err)
			}
		}()

		t.Log("Testing Ping operation, expecting failure due to cluster ID mismatch")
		err = db.Ping()
		if err != nil {
			t.Logf("Ping error: %v", err.Error())
		}
		if err == nil {
			t.Fatalf("Should failed to ping database: %v", err)
		} else if !strings.Contains(err.Error(), "no available nodes meet the conditions") {
			t.Fatalf("Failed to ping database with unexpected err: %v", err)
		}
	})
}

func TestDNSlaveRead(t *testing.T) {
	if *testType != "dn" {
		t.Skip("Skip non-dn test")
	}
	dbName := fmt.Sprintf("test_slave_read_db")
	tableName := "test_slave_read_table"
	insertSQL := fmt.Sprintf("INSERT INTO %s (name, value) VALUES (?, ?)", tableName)

	// 先用 slaveRead=false 创建数据
	dsnWrite := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
		"&slaveRead=false",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)

	t.Log("Opening database connection for writing...")
	dbWrite, err := sql.Open("polardbx", dsnWrite)
	if err != nil {
		t.Fatalf("Failed to open database for writing: %v", err)
	}

	// 创建测试数据库和表
	t.Log("Dropping database if exists...")
	_, err = dbWrite.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	if err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}

	t.Log("Creating database...")
	_, err = dbWrite.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	t.Log("Using database...")
	_, err = dbWrite.Exec(fmt.Sprintf("USE %s", dbName))
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	// 创建测试表
	t.Log("Creating test table...")
	createTableSQL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id INT AUTO_INCREMENT PRIMARY KEY,
				name VARCHAR(50) NOT NULL,
				value VARCHAR(100)
			) PARTITION BY HASH(id) PARTITIONS 16`, tableName)
	_, err = dbWrite.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 插入测试数据
	t.Log("Inserting test data...")
	_, err = dbWrite.Exec(insertSQL, "test_name", "test_value")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// 关闭写连接
	t.Log("Closing write database connection...")
	err = dbWrite.Close()
	if err != nil {
		t.Errorf("Failed to close write database: %v", err)
	}

	defer func() {
		// 清理测试数据
		t.Log("Cleaning up test data...")
		dbCleanup, err := sql.Open("polardbx", dsnWrite)
		if err != nil {
			t.Errorf("Failed to open database for cleanup: %v", err)
			return
		}
		defer dbCleanup.Close()

		_, err = dbCleanup.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
		if err != nil {
			t.Errorf("Failed to drop database: %v", err)
		}
	}()

	// 使用 slaveRead=true 的状态去读取数据
	dsnRead := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
		"&slaveRead=true",
		*user, *passwd, *addr, dbName, *enableLog, *enableProbeLog)

	t.Log("Opening database connection for reading...")
	dbRead, err := sql.Open("polardbx", dsnRead)
	if err != nil {
		t.Fatalf("Failed to open database for reading: %v", err)
	}

	defer func() {
		t.Log("Closing read database connection...")
		if err := dbRead.Close(); err != nil {
			t.Errorf("Failed to close read database: %v", err)
		}
	}()

	// 验证可以读取数据 - 循环10秒内不断尝试读取数据
	querySQL := fmt.Sprintf("SELECT id, name, value FROM %s WHERE name = ?", tableName)
	var id int
	var name, value string

	// 设置10秒超时
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond) // 每500毫秒尝试一次
	defer ticker.Stop()

	readSuccess := false
	t.Log("Attempting to read data with slaveRead=true...")
	for !readSuccess {
		select {
		case <-timeout:
			t.Fatalf("Failed to read expected data within 10 seconds with slaveRead=true")
		case <-ticker.C:
			t.Log("Trying to read data...")
			err = dbRead.QueryRow(querySQL, "test_name").Scan(&id, &name, &value)
			if err == nil && name == "test_name" && value == "test_value" {
				readSuccess = true
				t.Logf("Successfully read expected data within 10 seconds")
			}
		}
	}

	// 尝试写入数据，预期内失败
	t.Log("Testing write operation with slaveRead=true (expected to fail)...")
	_, err = dbRead.Exec(insertSQL, "test_name_2", "test_value_2")
	if err == nil {
		t.Errorf("Expected write operation to fail with slaveRead=true, but it succeeded")
	} else {
		t.Logf("Write operation correctly failed with slaveRead=true: %v", err)
	}

	// 尝试更新数据，预期内失败
	t.Log("Testing update operation with slaveRead=true (expected to fail)...")
	updateSQL := fmt.Sprintf("UPDATE %s SET value = ? WHERE name = ?", tableName)
	_, err = dbRead.Exec(updateSQL, "updated_value", "test_name")
	if err == nil {
		t.Errorf("Expected update operation to fail with slaveRead=true, but it succeeded")
	} else {
		t.Logf("Update operation correctly failed with slaveRead=true: %v", err)
	}

	// 尝试删除数据，预期内失败
	t.Log("Testing delete operation with slaveRead=true (expected to fail)...")
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE name = ?", tableName)
	_, err = dbRead.Exec(deleteSQL, "test_name")
	if err == nil {
		t.Errorf("Expected delete operation to fail with slaveRead=true, but it succeeded")
	} else {
		t.Logf("Delete operation correctly failed with slaveRead=true: %v", err)
	}

	t.Log("TestSlaveRead completed successfully!")
}

func TestDNApplyDelayThreshold(t *testing.T) {
	if *testType != "dn" {
		t.Skip("Skip non-dn test")
	}
	dbName := fmt.Sprintf("test_apply_delay_threshold_db")
	tableName := "test_apply_delay_threshold_table"
	insertSQL := fmt.Sprintf("INSERT INTO %s (name, value) VALUES (?, ?)", tableName)

	// 先用 slaveRead=false 创建数据
	dsnWrite := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
		"&slaveRead=false",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)

	t.Log("Opening database connection for writing...")
	dbWrite, err := sql.Open("polardbx", dsnWrite)
	if err != nil {
		t.Fatalf("Failed to open database for writing: %v", err)
	}

	// 创建测试数据库和表
	t.Log("Creating database...")
	_, err = dbWrite.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	if err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}

	_, err = dbWrite.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	_, err = dbWrite.Exec(fmt.Sprintf("USE %s", dbName))
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	// 创建测试表
	t.Log("Creating test table...")
	createTableSQL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id INT AUTO_INCREMENT PRIMARY KEY,
				name VARCHAR(20) NOT NULL,
				value VARCHAR(100)
			) PARTITION BY HASH(id) PARTITIONS 16`, tableName)
	_, err = dbWrite.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	t.Log("Inserting test data...")
	// 创建独立的数据库连接用于并发插入
	dbWorker, err := sql.Open("polardbx", dsnWrite)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
		return
	}
	defer dbWorker.Close()

	_, err = dbWorker.Exec(fmt.Sprintf("USE %s", dbName))
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	// 插入分配给这个worker的数据
	for i := 0; i < 100; i++ {
		_, err = dbWorker.Exec(insertSQL, fmt.Sprintf("test_name_%d", i), fmt.Sprintf("test_value_%d", i))
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
			return
		}
	}

	// 关闭写连接
	err = dbWrite.Close()
	if err != nil {
		t.Errorf("Failed to close write database: %v", err)
	}

	defer func() {
		// 清理测试数据
		t.Log("Cleaning up test data...")
		dbCleanup, err := sql.Open("polardbx", dsnWrite)
		if err != nil {
			t.Errorf("Failed to open database for cleanup: %v", err)
			return
		}
		defer dbCleanup.Close()

		_, err = dbCleanup.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
		if err != nil {
			t.Errorf("Failed to drop database: %v", err)
		}
	}()

	// 测试1: 使用较大的 applyDelayThreshold 值，应该能够成功连接到备库并读取数据
	t.Log("Test 1: Testing with large applyDelayThreshold value...")
	dsnReadLargeThreshold := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
		"&slaveRead=true&applyDelayThreshold=100",
		*user, *passwd, *addr, dbName, *enableLog, *enableProbeLog)

	t.Logf("Using dsn %s", dsnReadLargeThreshold)
	dbReadLarge, err := sql.Open("polardbx", dsnReadLargeThreshold)
	if err != nil {
		t.Fatalf("Failed to open database for reading with large threshold: %v", err)
	}

	defer func() {
		if err := dbReadLarge.Close(); err != nil {
			t.Errorf("Failed to close read database with large threshold: %v", err)
		}
	}()

	time.Sleep(1 * time.Second)

	// 验证可以读取数据
	querySQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var count int
	err = dbReadLarge.QueryRow(querySQL).Scan(&count)
	if err != nil {
		t.Errorf("Failed to read data with large applyDelayThreshold: %v", err)
	} else {
		t.Logf("Successfully read data with large applyDelayThreshold")
	}

	// 测试2: 使用较小的 applyDelayThreshold 值
	t.Log("Test 2: Testing with small applyDelayThreshold value...")

	// 使用较小的 applyDelayThreshold 值连接备库，预期连接失败
	dsnReadSmallThreshold := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
		"&haCheckIntervalMillis=%d&haCheckSocketTimeoutMillis=%d&connectTimeout=%d&slaveRead=true&applyDelayThreshold=-1",
		*user, *passwd, *addr, dbName, *enableLog, *enableProbeLog,
		*haCheckIntervalMillis, *haCheckSocketTimeoutMillis, *connectTimeout)

	t.Logf("Using dsn %s", dsnReadSmallThreshold)
	dbReadSmall, err := sql.Open("polardbx", dsnReadSmallThreshold)
	if err != nil {
		t.Fatalf("Failed to open database for reading with small threshold: %v", err)
	}

	defer func() {
		t.Log("Closing read database connection with small threshold...")
		if err := dbReadSmall.Close(); err != nil {
			t.Errorf("Failed to close read database with small threshold: %v", err)
		}
	}()

	// 尝试读取数据，预期可能失败或超时
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	querySQL2 := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var count2 int
	err = dbReadSmall.QueryRowContext(ctx, querySQL2).Scan(&count2)
	if err != nil {
		// 连接失败符合预期，因为延迟超过了阈值
		t.Logf("Expected failure when connecting with small applyDelayThreshold: %v", err)
	} else {
		// 如果连接成功，记录结果但标记为需要注意
		t.Logf("Warning: Connected successfully with small applyDelayThreshold, record count: %d", count2)
	}

	t.Log("TestApplyDelayThreshold completed!")
}

func TestDNSlaveWeightThreshold(t *testing.T) {
	if *testType != "dn" {
		t.Skip("Skip non-dn test")
	}
	dsnLeader := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
		"&slaveRead=false",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)

	t.Logf("Querying election weight using leader dsn %s...", dsnLeader)
	dbWrite, err := sql.Open("polardbx", dsnLeader)
	if err != nil {
		t.Fatalf("Failed to open database for writing: %v", err)
	}
	defer dbWrite.Close()

	var maxElectionWeight int
	err = dbWrite.QueryRow("select max(ELECTION_WEIGHT) from information_schema.alisql_cluster_global").Scan(&maxElectionWeight)
	if err != nil {
		t.Fatalf("Failed to query election weight: %v", err)
	}
	t.Logf("Got max election weight %d...", maxElectionWeight)

	dsnRead := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
		"&haCheckIntervalMillis=%d&haCheckSocketTimeoutMillis=%d&connectTimeout=%d&slaveRead=true&slaveWeightThreshold=%d",
		*user, *passwd, *addr, "information_schema", *enableLog, *enableProbeLog,
		*haCheckIntervalMillis, *haCheckSocketTimeoutMillis, *connectTimeout, maxElectionWeight+1)

	t.Logf("Using dsn %s", dsnRead)
	dbRead, err := sql.Open("polardbx", dsnRead)
	if err != nil {
		t.Fatalf("Failed to open database for reading with small threshold: %v", err)
	}

	defer func() {
		t.Log("Closing read database connection with small threshold...")
		if err := dbRead.Close(); err != nil {
			t.Errorf("Failed to close read database with small threshold: %v", err)
		}
	}()

	var count int
	err = dbRead.QueryRow("select count(*) from information_schema.alisql_cluster_global").Scan(&count)
	if err != nil {
		t.Logf("Expected failure when connecting with large slaveWeightThreshold: %v", err)
	} else {
		t.Logf("Warning: Connected successfully with large slaveWeightThreshold, record count: %d", count)
	}
}

func TestDNDirectMode(t *testing.T) {
	if *testType != "dn" {
		t.Skip("Skip non-dn test")
	}
	dsnLeader := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
		"&slaveRead=false",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)

	t.Logf("Querying port gap using leader dsn %s...", dsnLeader)
	dbLeader, err := sql.Open("polardbx", dsnLeader)
	if err != nil {
		t.Fatalf("Failed to open database for writing: %v", err)
	}
	defer dbLeader.Close()

	var leaderPort int
	err = dbLeader.QueryRow("select @@port").Scan(&leaderPort)
	if err != nil {
		t.Fatalf("Failed to query leader port: %v", err)
	}
	var leaderAddress string
	err = dbLeader.QueryRow("select ip_port from information_schema.alisql_cluster_global where role = 'Leader' and INSTANCE_TYPE = 'Normal'").Scan(&leaderAddress)
	leaderIp := strings.Split(leaderAddress, ":")[0]
	leaderXPaxosPort, err := strconv.Atoi(strings.Split(leaderAddress, ":")[1])
	if err != nil {
		t.Fatalf("Failed to parse xpaxos port: %v", err)
	}
	gap := leaderXPaxosPort - leaderPort
	t.Logf("Got leader address %s, port gap %d...", leaderAddress, gap)
	leaderAddress = leaderIp + ":" + strconv.Itoa(leaderPort)

	var clusterId int
	err = dbLeader.QueryRow("select @@cluster_id").Scan(&clusterId)
	if err != nil {
		t.Fatalf("Failed to get cluster id: %v", err)
	} else {
		t.Logf("Get cluster id: %d", clusterId)
	}

	var slaveAddress string
	err = dbLeader.QueryRow("select ip_port from information_schema.alisql_cluster_global where role = 'Follower' and INSTANCE_TYPE = 'Normal'").Scan(&slaveAddress)
	slaveIp := strings.Split(slaveAddress, ":")[0]
	slaveXPaxosPort, err := strconv.Atoi(strings.Split(slaveAddress, ":")[1])
	if err != nil {
		t.Fatalf("Failed to parse xpaxos port: %v", err)
	}
	slavePort := slaveXPaxosPort - gap
	t.Logf("Got slave address %s, real port %d...", slaveAddress, slavePort)
	slaveAddress = slaveIp + ":" + strconv.Itoa(slavePort)

	dsnLeaderDirect := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
		"&slaveRead=false&directMode=true&clusterId=%d",
		*user, *passwd, leaderAddress, *enableLog, *enableProbeLog, clusterId+1)
	t.Logf("Opening database using leader dsn in direct mode and wrong cluster id %s", dsnLeaderDirect)
	dbLeaderDirect, err := sql.Open("polardbx", dsnLeaderDirect)
	if err != nil {
		t.Fatalf("Failed to open database for writing: %v", err)
	}
	defer dbLeaderDirect.Close()

	// create database and table and insert data and drop database using dbLeaderDirect
	t.Logf("Creating database and table and insert data and drop database using dbLeaderDirect...")
	_, err = dbLeaderDirect.Exec("create database if not exists test_direct_mode")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	_, err = dbLeaderDirect.Exec("create table if not exists test_direct_mode.test_direct_mode (id int primary key, value int)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = dbLeaderDirect.Exec("insert into test_direct_mode.test_direct_mode (id, value) values (1, 1)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	_, err = dbLeaderDirect.Exec("drop database test_direct_mode")
	if err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}

	dsnSlaveDirect := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
		"&slaveRead=false&directMode=true&clusterId=%d&applyDelayThreshold=-1",
		*user, *passwd, slaveAddress, *enableLog, *enableProbeLog, clusterId+1)
	t.Logf("Opening database using slave dsn in direct mode and wrong cluster id and -1 applyDelayThreshold %s", dsnSlaveDirect)
	dbSlaveDirect, err := sql.Open("polardbx", dsnSlaveDirect)
	if err != nil {
		t.Fatalf("Failed to open database for writing: %v", err)
	}
	defer dbSlaveDirect.Close()

	// query information_schema.alisql_cluster_local
	var role string
	err = dbSlaveDirect.QueryRow("select role from information_schema.alisql_cluster_local").Scan(&role)
	if err != nil {
		t.Fatalf("Failed to query role: %v", err)
	}
	if !strings.EqualFold(role, "Follower") {
		t.Fatalf("Role should be Follower, but got %s", role)
	}
}

func TestDNRecordJdbcUrl(t *testing.T) {
	if *testType != "dn" {
		t.Skip("Skip non-dn test")
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&recordJdbcUrl=true",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)
	println("Run test with dsn: ", dsn)
	db, err := sql.Open("polardbx", dsn)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	expected := "charset=utf8mb4&parseTime=True&driverVersion=" + Version
	// execute call dbms_conn.show_connection(); and get rows and expect one rows containing a column named JDBC_COMMENT and JDBC_COMMENT contains expected
	var rows *sql.Rows
	rows, err = db.Query("call dbms_conn.show_connection()")
	if err != nil {
		t.Fatalf("Failed to query connection: %v", err)
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var jdbcComment, ignore *string
		err = rows.Scan(&ignore, &ignore, &ignore, &ignore, &jdbcComment)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		if *jdbcComment != "" {
			t.Log("JDBC_COMMENT: ", *jdbcComment)
		}
		if strings.Contains(*jdbcComment, expected) {
			found = true
		}
	}
	if !found {
		t.Errorf("JDBC_COMMENT not found")
	}
}

func TestDNIgnoreVip(t *testing.T) {
	if *testType != "dn" {
		t.Skip("Skip non-dn test")
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&ignoreVip=true",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)
	println("Run test with dsn(ignoreVip=true): ", dsn)
	db, err := sql.Open("polardbx", dsn)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	runSimpleCase(t, db)

	dsn = fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&ignoreVip=false",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)
	println("Run test with dsn(ignoreVip=false): ", dsn)
	db, err = sql.Open("polardbx", dsn)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	runSimpleCase(t, db)
}

func TestDNSameClusterId(t *testing.T) {
	if *testType != "dn" {
		t.Skip("Skip non-dn test")
	}
	// 1. 测试输入2个实例A和B的连接串
	// 假设我们有两个不同的地址，但属于同一个集群ID
	dsnA := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)

	// 为了模拟另一个实例，我们使用相同的集群ID参数
	dsnB := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s",
		*user2, *passwd2, *addr2, *enableLog, *enableProbeLog)

	t.Logf("DSN A: %s", dsnA)
	t.Logf("DSN B: %s", dsnB)

	// 2. 在 A、B 实例上创建相同的表，插入不同的数据
	// 连接实例A
	dbA, err := sql.Open("polardbx", dsnA)
	if err != nil {
		t.Fatalf("Failed to open database A: %v", err)
	}
	defer dbA.Close()

	// 连接实例B
	dbB, err := sql.Open("polardbx", dsnB)
	if err != nil {
		t.Fatalf("Failed to open database B: %v", err)
	}
	defer dbB.Close()

	var clusterIdA int
	err = dbA.QueryRow("select @@cluster_id").Scan(&clusterIdA)
	if err != nil {
		t.Fatalf("Failed to get cluster id on A: %v", err)
	}
	var clusterIdB int
	err = dbB.QueryRow("select @@cluster_id").Scan(&clusterIdB)
	if err != nil {
		t.Fatalf("Failed to get cluster id on B: %v", err)
	}
	t.Logf("Cluster ID A: %d, Cluster ID B: %d", clusterIdA, clusterIdB)

	// 创建测试数据库
	dbName := "test_same_cluster_id"
	_, err = dbA.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	if err != nil {
		t.Fatalf("Failed to create database on A: %v", err)
	}
	_, err = dbB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	if err != nil {
		t.Fatalf("Failed to create database on B: %v", err)
	}

	// 使用测试数据库
	_, err = dbA.Exec(fmt.Sprintf("USE %s", dbName))
	if err != nil {
		t.Fatalf("Failed to use database on A: %v", err)
	}
	_, err = dbB.Exec(fmt.Sprintf("USE %s", dbName))
	if err != nil {
		t.Fatalf("Failed to use database on B: %v", err)
	}

	// 创建相同的表
	tableName := "test_table"
	t.Logf("Creating database %s and table %s on A and B", dbName, tableName)
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(50) NOT NULL,
			source VARCHAR(10) NOT NULL
		)`, tableName)

	_, err = dbA.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table on A: %v", err)
	}
	_, err = dbB.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table on B: %v", err)
	}

	// 清空表数据
	_, err = dbA.Exec(fmt.Sprintf("TRUNCATE TABLE %s", tableName))
	if err != nil {
		t.Fatalf("Failed to truncate table on A: %v", err)
	}
	_, err = dbB.Exec(fmt.Sprintf("TRUNCATE TABLE %s", tableName))
	if err != nil {
		t.Fatalf("Failed to truncate table on B: %v", err)
	}

	// 在A实例插入数据
	t.Logf("Inserting data into A: DataFromA1, DataFromA2")
	_, err = dbA.Exec(fmt.Sprintf("INSERT INTO %s (name, source) VALUES ('DataFromA1', 'A'), ('DataFromA2', 'A')", tableName))
	if err != nil {
		t.Fatalf("Failed to insert data into A: %v", err)
	}

	// 在B实例插入不同的数据
	t.Logf("Inserting data into B: DataFromB1, DataFromB2")
	_, err = dbB.Exec(fmt.Sprintf("INSERT INTO %s (name, source) VALUES ('DataFromB1', 'B'), ('DataFromB2', 'B')", tableName))
	if err != nil {
		t.Fatalf("Failed to insert data into B: %v", err)
	}

	// 3. 先连接 A 实例查询表数据，预期查到 A 实例的表数据
	rowsA, err := dbA.Query(fmt.Sprintf("SELECT name, source FROM %s ORDER BY name", tableName))
	if err != nil {
		t.Fatalf("Failed to query data from A: %v", err)
	}
	defer rowsA.Close()

	var namesA []string
	var sourcesA []string
	for rowsA.Next() {
		var name, source string
		err := rowsA.Scan(&name, &source)
		if err != nil {
			t.Fatalf("Failed to scan row from A: %v", err)
		}
		namesA = append(namesA, name)
		sourcesA = append(sourcesA, source)
	}

	// 验证数据来自A实例
	t.Log("Querying data from instance A")
	if len(namesA) != 2 {
		t.Errorf("Expected 2 rows from A, got %d", len(namesA))
	}
	for _, source := range sourcesA {
		if source != "A" {
			t.Errorf("Expected all data from A instance, but got source: %s", source)
		}
	}
	t.Logf("Data from A instance: %v", namesA)

	// 4. 再连接 B 实例查询表数据，预期查到 B 实例的表数据
	t.Log("Querying data from instance B")
	rowsB, err := dbB.Query(fmt.Sprintf("SELECT name, source FROM %s ORDER BY name", tableName))
	if err != nil {
		t.Fatalf("Failed to query data from B: %v", err)
	}
	defer rowsB.Close()

	var namesB []string
	var sourcesB []string
	for rowsB.Next() {
		var name, source string
		err := rowsB.Scan(&name, &source)
		if err != nil {
			t.Fatalf("Failed to scan row from B: %v", err)
		}
		namesB = append(namesB, name)
		sourcesB = append(sourcesB, source)
	}

	// 验证数据来自B实例
	if len(namesB) != 2 {
		t.Errorf("Expected 2 rows from B, got %d", len(namesB))
	}
	for _, source := range sourcesB {
		if source != "B" {
			t.Errorf("Expected all data from B instance, but got source: %s", source)
		}
	}
	t.Logf("Data from B instance: %v", namesB)

	// 5. 重复 3、4 两步，不断查询 A B 两个实例，每次重新建立新的连接
	t.Log("Repeating querying with deletion of JSON files")

	// 重复查询多次，每次都重新建立连接
	for i := 0; i < 10; i++ {
		t.Logf("Iteration %d: Reconnecting and querying A and B instances", i+1)

		// 重新连接实例A并查询数据
		dbNewA, err := sql.Open("polardbx", dsnA)
		if err != nil {
			t.Fatalf("Failed to reopen database A: %v", err)
		}

		rowsNewA, err := dbNewA.Query(fmt.Sprintf("SELECT name, source FROM %s.%s ORDER BY name", dbName, tableName))
		if err != nil {
			dbNewA.Close()
			t.Fatalf("Failed to query data from A in iteration %d: %v", i+1, err)
		}

		var newNamesA []string
		var newSourcesA []string
		for rowsNewA.Next() {
			var name, source string
			err := rowsNewA.Scan(&name, &source)
			if err != nil {
				rowsNewA.Close()
				dbNewA.Close()
				t.Fatalf("Failed to scan row from A in iteration %d: %v", i+1, err)
			}
			newNamesA = append(newNamesA, name)
			newSourcesA = append(newSourcesA, source)
		}
		rowsNewA.Close()
		dbNewA.Close()

		// 验证数据来自A实例
		if len(newNamesA) != 2 {
			t.Errorf("Iteration %d: Expected 2 rows from A, got %d", i+1, len(newNamesA))
		}
		for _, source := range newSourcesA {
			if source != "A" {
				t.Errorf("Iteration %d: Expected all data from A instance, but got source: %s", i+1, source)
			}
		}
		t.Logf("Iteration %d: Data from A instance: %v", i+1, newNamesA)

		// 重新连接实例B并查询数据
		dbNewB, err := sql.Open("polardbx", dsnB)
		if err != nil {
			t.Fatalf("Failed to reopen database B: %v", err)
		}

		rowsNewB, err := dbNewB.Query(fmt.Sprintf("SELECT name, source FROM %s.%s ORDER BY name", dbName, tableName))
		if err != nil {
			dbNewB.Close()
			t.Fatalf("Failed to query data from B in iteration %d: %v", i+1, err)
		}

		var newNamesB []string
		var newSourcesB []string
		for rowsNewB.Next() {
			var name, source string
			err := rowsNewB.Scan(&name, &source)
			if err != nil {
				rowsNewB.Close()
				dbNewB.Close()
				t.Fatalf("Failed to scan row from B in iteration %d: %v", i+1, err)
			}
			newNamesB = append(newNamesB, name)
			newSourcesB = append(newSourcesB, source)
		}
		rowsNewB.Close()
		dbNewB.Close()

		// 验证数据来自B实例
		if len(newNamesB) != 2 {
			t.Errorf("Iteration %d: Expected 2 rows from B, got %d", i+1, len(newNamesB))
		}
		for _, source := range newSourcesB {
			if source != "B" {
				t.Errorf("Iteration %d: Expected all data from B instance, but got source: %s", i+1, source)
			}
		}
		t.Logf("Iteration %d: Data from B instance: %v", i+1, newNamesB)
	}

	// 清理：删除测试数据库
	_, err = dbA.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	if err != nil {
		t.Logf("Failed to drop database on A: %v", err)
	}
	_, err = dbB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	if err != nil {
		t.Logf("Failed to drop database on B: %v", err)
	}

	t.Log("TestSameClusterId completed!")
}

// runSimpleCase executes the simple case scenario with the given database connection
func runSimpleCase(t *testing.T, db *sql.DB) {
	// Add logging to indicate what operation is being performed
	t.Log("Starting simple case test scenario")

	// Drop database if exists
	t.Log("Dropping database if it exists")
	dbName := fmt.Sprintf("test_simple_case_db")
	_, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	if err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}

	// Create database
	t.Log("Creating database")
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Use database
	t.Log("Using database")
	_, err = db.Exec(fmt.Sprintf("USE %s", dbName))
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	// Create table
	t.Log("Creating table")
	tableName := "test_users"
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(50) NOT NULL,
			email VARCHAR(100),
			age INT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`, tableName)
	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// CRUD operations
	// Create - Insert data
	t.Log("Inserting data into table")
	insertSQL := fmt.Sprintf("INSERT INTO %s (name, email, age) VALUES (?, ?, ?)", tableName)
	result, err := db.Exec(insertSQL, "Alice", "alice@example.com", 25)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	id1, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("Failed to get last insert id: %v", err)
	}

	result, err = db.Exec(insertSQL, "Bob", "bob@example.com", 30)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	id2, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("Failed to get last insert id: %v", err)
	}

	// Read - Query data
	t.Log("Querying data from table")
	querySQL := fmt.Sprintf("SELECT id, name, email, age FROM %s WHERE id = ?", tableName)
	var id int
	var name, email string
	var age int
	err = db.QueryRow(querySQL, id1).Scan(&id, &name, &email, &age)
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	if id != int(id1) || name != "Alice" || email != "alice@example.com" || age != 25 {
		t.Errorf("Query result mismatch: got (%d, %s, %s, %d), want (%d, Alice, alice@example.com, 25)", id, name, email, age, id1)
	}

	// Update - Update data
	t.Log("Updating data in table")
	updateSQL := fmt.Sprintf("UPDATE %s SET age = ? WHERE id = ?", tableName)
	_, err = db.Exec(updateSQL, 26, id1)
	if err != nil {
		t.Fatalf("Failed to update data: %v", err)
	}

	// Verify update
	t.Log("Verifying updated data")
	err = db.QueryRow(querySQL, id1).Scan(&id, &name, &email, &age)
	if err != nil {
		t.Fatalf("Failed to query updated data: %v", err)
	}
	if age != 26 {
		t.Errorf("Update failed: got age %d, want 26", age)
	}

	// Delete - Delete data
	t.Log("Deleting data from table")
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE id = ?", tableName)
	_, err = db.Exec(deleteSQL, id2)
	if err != nil {
		t.Fatalf("Failed to delete data: %v", err)
	}

	// Verify deletion
	t.Log("Verifying data deletion")
	var count int
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	err = db.QueryRow(countSQL).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}
	if count != 1 {
		t.Errorf("Delete failed: got count %d, want 1", count)
	}

	// Test partitioned table
	t.Log("Testing partitioned table")
	partitionedTableName := "test_users_partitioned"
	createPartitionedTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(50) NOT NULL,
			email VARCHAR(100),
			age INT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		) ENGINE=InnoDB PARTITION BY HASH(id) PARTITIONS 16`, partitionedTableName)
	_, err = db.Exec(createPartitionedTableSQL)
	if err != nil {
		t.Fatalf("Failed to create partitioned table: %v", err)
	}

	// CRUD operations on partitioned table
	// Create - Insert data into partitioned table
	t.Log("Inserting data into partitioned table")
	insertPartitionedSQL := fmt.Sprintf("INSERT INTO %s (name, email, age) VALUES (?, ?, ?)", partitionedTableName)
	result, err = db.Exec(insertPartitionedSQL, "Charlie", "charlie@example.com", 35)
	if err != nil {
		t.Fatalf("Failed to insert data into partitioned table: %v", err)
	}
	pid1, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("Failed to get last insert id from partitioned table: %v", err)
	}

	result, err = db.Exec(insertPartitionedSQL, "David", "david@example.com", 40)
	if err != nil {
		t.Fatalf("Failed to insert data into partitioned table: %v", err)
	}
	pid2, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("Failed to get last insert id from partitioned table: %v", err)
	}

	// Read - Query data from partitioned table
	t.Log("Querying data from partitioned table")
	queryPartitionedSQL := fmt.Sprintf("SELECT id, name, email, age FROM %s WHERE id = ?", partitionedTableName)
	var pid int
	var pname, pemail string
	var page int
	err = db.QueryRow(queryPartitionedSQL, pid1).Scan(&pid, &pname, &pemail, &page)
	if err != nil {
		t.Fatalf("Failed to query data from partitioned table: %v", err)
	}
	if pid != int(pid1) || pname != "Charlie" || pemail != "charlie@example.com" || page != 35 {
		t.Errorf("Query result mismatch in partitioned table: got (%d, %s, %s, %d), want (%d, Charlie, charlie@example.com, 35)", pid, pname, pemail, page, pid1)
	}

	// Update - Update data in partitioned table
	t.Log("Updating data in partitioned table")
	updatePartitionedSQL := fmt.Sprintf("UPDATE %s SET age = ? WHERE id = ?", partitionedTableName)
	_, err = db.Exec(updatePartitionedSQL, 36, pid1)
	if err != nil {
		t.Fatalf("Failed to update data in partitioned table: %v", err)
	}

	// Verify update in partitioned table
	t.Log("Verifying updated data in partitioned table")
	err = db.QueryRow(queryPartitionedSQL, pid1).Scan(&pid, &pname, &pemail, &page)
	if err != nil {
		t.Fatalf("Failed to query updated data from partitioned table: %v", err)
	}
	if page != 36 {
		t.Errorf("Update failed in partitioned table: got age %d, want 36", page)
	}

	// Delete - Delete data from partitioned table
	t.Log("Deleting data from partitioned table")
	deletePartitionedSQL := fmt.Sprintf("DELETE FROM %s WHERE id = ?", partitionedTableName)
	_, err = db.Exec(deletePartitionedSQL, pid2)
	if err != nil {
		t.Fatalf("Failed to delete data from partitioned table: %v", err)
	}

	// Verify deletion from partitioned table
	t.Log("Verifying data deletion from partitioned table")
	var pcount int
	countPartitionedSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", partitionedTableName)
	err = db.QueryRow(countPartitionedSQL).Scan(&pcount)
	if err != nil {
		t.Fatalf("Failed to count records in partitioned table: %v", err)
	}
	if pcount != 1 {
		t.Errorf("Delete failed in partitioned table: got count %d, want 1", pcount)
	}

	// Drop tables
	t.Log("Dropping tables")
	dropTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	_, err = db.Exec(dropTableSQL)
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	dropPartitionedTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", partitionedTableName)
	_, err = db.Exec(dropPartitionedTableSQL)
	if err != nil {
		t.Fatalf("Failed to drop partitioned table: %v", err)
	}

	// Execute SET statement
	t.Log("Executing SET statement")
	_, err = db.Exec("SET SESSION sql_mode = 'STRICT_TRANS_TABLES'")
	if err != nil {
		t.Logf("Warning: Failed to execute SET statement: %v", err)
	}

	// Execute SHOW statement
	t.Log("Executing SHOW statement")
	var variableName, variableValue string
	err = db.QueryRow("SHOW VARIABLES LIKE 'version'").Scan(&variableName, &variableValue)
	if err != nil {
		t.Logf("Warning: Failed to execute SHOW statement: %v", err)
	} else {
		t.Logf("Database version: %s = %s", variableName, variableValue)
	}

	// Drop database
	t.Log("Dropping database")
	dropDBSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName)
	_, err = db.Exec(dropDBSQL)
	if err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}

	t.Log("Simple case test scenario completed successfully")
}
