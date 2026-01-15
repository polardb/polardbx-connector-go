package polardbx

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCNSimpleCase(t *testing.T) {
	if *testType != "cn" {
		t.Skip("Skip non-cn test")
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

	runCNSimpleCase(t, db)
}

func TestCNIgnoreVip(t *testing.T) {
	if *testType != "cn" {
		t.Skip("Skip non-cn test")
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&ignoreVip=true",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)
	println("Run test with dsn(ignoreVip=true): ", dsn)
	db, err := sql.Open("polardbx", dsn)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	runCNSimpleCase(t, db)

	dsn = fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&ignoreVip=false",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)
	println("Run test with dsn(ignoreVip=false): ", dsn)
	db, err = sql.Open("polardbx", dsn)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	runCNSimpleCase(t, db)
}

func TestCNRecordJdbcUrl(t *testing.T) {
	if *testType != "cn" {
		t.Skip("Skip non-cn test")
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
	// execute call polardbx.show_jdbc_url();; and get rows and expect one rows containing a column named JDBC_COMMENT and JDBC_COMMENT contains expected
	var rows *sql.Rows
	rows, err = db.Query("call polardbx.show_jdbc_url()")
	if err != nil {
		t.Fatalf("Failed to query connection: %v", err)
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var jdbcComment, ignore *string
		err = rows.Scan(&ignore, &ignore, &ignore, &ignore, &ignore, &jdbcComment)
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

func TestCNSlaveRead(t *testing.T) {
	if *testType != "cn" {
		t.Skip("Skip non-cn test")
	}
	dbName := fmt.Sprintf("test_slave_read_db")
	tableName := "test_slave_read_table"
	insertSQL := fmt.Sprintf("INSERT INTO %s (name, value) VALUES (?, ?)", tableName)

	// 先用 slaveRead=false 创建数据
	dsnWrite := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
		"&slaveRead=false",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)

	t.Log("Opening database connection for writing, dsn: ", dsnWrite)
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

	t.Log("Opening database connection for reading, dsn: ", dsnRead)
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

func TestCNEnableFollowerRead(t *testing.T) {
	if *testType != "cn" {
		t.Skip("Skip non-cn test")
	}
	dbName := fmt.Sprintf("test_enable_follower_read_db")

	// Test cases for different enableFollowerRead values
	testCases := []struct {
		name               string
		enableFollowerRead int32
		expectReadOnly     bool
	}{
		{"Default (-1) - Read Write", -1, false},
		{"Read Master (0) - Read Write", 0, false},
		{"Read Slave (1) - Read Only", 1, true},
		{"Read Consistent Slave (2) - Read Only", 2, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create DSN without enableFollowerRead parameter for initialization
			initDsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s",
				*user, *passwd, *addr, *enableLog, *enableProbeLog)

			// Initialize database and table for each test case
			t.Log("Opening initialization database connection with dsn: ", initDsn)
			initDb, err := sql.Open("polardbx", initDsn)
			if err != nil {
				t.Fatalf("Failed to open initialization database: %v", err)
			}

			defer func() {
				if err := initDb.Close(); err != nil {
					t.Errorf("Failed to close initialization database: %v", err)
				}
			}()

			// Drop database if exists
			t.Log("Dropping database if exists...")
			_, err = initDb.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
			if err != nil {
				t.Fatalf("Failed to drop database: %v", err)
			}

			// Create database
			t.Log("Creating database...")
			_, err = initDb.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
			if err != nil {
				t.Fatalf("Failed to create database: %v", err)
			}

			// Use database
			t.Log("Using database...")
			_, err = initDb.Exec(fmt.Sprintf("USE %s", dbName))
			if err != nil {
				t.Fatalf("Failed to use database: %v", err)
			}

			_, err = initDb.Exec("set enable_in_memory_follower_read=false")
			if err != nil {
				t.Fatalf("Failed to use database: %v", err)
			}

			// Close initialization database connection
			if err := initDb.Close(); err != nil {
				t.Errorf("Failed to close initialization database: %v", err)
			}

			// Create DSN with specific enableFollowerRead value for testing
			testDsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&enableFollowerRead=%d",
				*user, *passwd, *addr, dbName, *enableLog, *enableProbeLog, tc.enableFollowerRead)

			t.Log("Opening test database connection with dsn: ", testDsn)
			db, err := sql.Open("polardbx", testDsn)
			if err != nil {
				t.Fatalf("Failed to open test database: %v", err)
			}

			defer func() {
				if err := db.Close(); err != nil {
					t.Errorf("Failed to close test database: %v", err)
				}
			}()

			// Test show datasources command behavior based on enableFollowerRead value
			t.Logf("Testing show datasources behavior for enableFollowerRead=%d", tc.enableFollowerRead)

			// Use database first
			_, err = db.Exec(fmt.Sprintf("USE %s", dbName))
			if err != nil {
				t.Fatalf("Failed to use database: %v", err)
			}

			// Execute show datasources command
			rows, err := db.Query("SHOW DATASOURCES")
			if err != nil {
				t.Errorf("Failed to execute SHOW DATASOURCES: %v", err)
			} else {
				defer rows.Close()

				// Parse the results
				type datasource struct {
					id            string
					schema        string
					name          string
					group         string
					url           string
					user          string
					dsType        string
					init          string
					min           string
					max           string
					idleTimeout   string
					maxWait       string
					onFatalError  string
					maxWaitThread string
					activeCount   string
					poolingCount  string
					atom          string
					readWeight    string
					writeWeight   string
					storageInstId string
				}

				var datasources []datasource

				for rows.Next() {
					var ds datasource
					err := rows.Scan(
						&ds.id, &ds.schema, &ds.name, &ds.group, &ds.url,
						&ds.user, &ds.dsType, &ds.init, &ds.min, &ds.max,
						&ds.idleTimeout, &ds.maxWait, &ds.onFatalError, &ds.maxWaitThread,
						&ds.activeCount, &ds.poolingCount, &ds.atom, &ds.readWeight,
						&ds.writeWeight, &ds.storageInstId)
					if err != nil {
						t.Errorf("Failed to scan row: %v", err)
						break
					}
					datasources = append(datasources, ds)
				}

				if err = rows.Err(); err != nil {
					t.Errorf("Error iterating rows: %v", err)
				} else {
					// Log detailed information about the datasources
					t.Logf("Found %d datasource records", len(datasources))

					// Count different STORAGE_INST_IDs
					storageInstIds := make(map[string]int)

					// Populate the map with actual data
					for _, ds := range datasources {
						storageInstIds[ds.storageInstId]++
					}

					// Log summary information
					t.Logf("Number of different STORAGE_INST_IDs: %d", len(storageInstIds))

					// Log detailed information about each STORAGE_INST_ID
					for storageInstId, count := range storageInstIds {
						t.Logf("STORAGE_INST_ID '%s' appears %d times", storageInstId, count)
					}

					// Log detailed information about each datasource record
					for i, ds := range datasources {
						t.Logf("Datasource[%d]: STORAGE_INST_ID='%s', WRITE_WEIGHT='%s', READ_WEIGHT='%s'",
							i, ds.storageInstId, ds.writeWeight, ds.readWeight)
					}

					// Validate behavior based on enableFollowerRead value
					switch tc.enableFollowerRead {
					case -1, 0:
						// For -1 and 0, check that there are no duplicate STORAGE_INST_IDs
						hasDuplicates := false
						for _, count := range storageInstIds {
							if count > 1 {
								hasDuplicates = true
								break
							}
						}

						if hasDuplicates {
							t.Errorf("For enableFollowerRead=%d, expected no duplicate STORAGE_INST_IDs, but found duplicates", tc.enableFollowerRead)
						} else {
							t.Logf("For enableFollowerRead=%d, verified no duplicate STORAGE_INST_IDs", tc.enableFollowerRead)
						}

					case 1, 2:
						// For 1 and 2, check that there are duplicate STORAGE_INST_IDs (at least one appears more than once)
						hasDuplicates := false
						for _, count := range storageInstIds {
							if count > 1 {
								hasDuplicates = true
								break
							}
						}

						if !hasDuplicates {
							t.Errorf("For enableFollowerRead=%d, expected duplicate STORAGE_INST_IDs, but found none", tc.enableFollowerRead)
						} else {
							t.Logf("For enableFollowerRead=%d, verified duplicate STORAGE_INST_IDs exist", tc.enableFollowerRead)
						}
					}
				}
			}

			// Clean up test data after each test case
			t.Log("Cleaning up test data...")
			cleanupDsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s",
				*user, *passwd, *addr, *enableLog, *enableProbeLog)
			dbCleanup, err := sql.Open("polardbx", cleanupDsn)
			if err != nil {
				t.Errorf("Failed to open database for cleanup: %v", err)
				return
			}
			defer dbCleanup.Close()

			_, err = dbCleanup.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
			if err != nil {
				t.Errorf("Failed to drop database: %v", err)
			}
		})
	}

	t.Log("TestCnEnableFollowerRead completed successfully!")
}

func TestCNDirectMode(t *testing.T) {
	if *testType != "cn" {
		t.Skip("Skip non-cn test")
	}
	// Create indirect connection to get MPP information
	dsnIndirect := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
		"&slaveRead=false",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)

	// Open database connection
	db, err := sql.Open("polardbx", dsnIndirect)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close database: %v", err)
		}
	}()

	// Execute show mpp command to get CN information
	rows, err := db.Query("show mpp")
	if err != nil {
		t.Fatalf("Failed to execute show mpp: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("Failed to close rows: %v", err)
		}
	}()

	// Parse the results
	type mppInfo struct {
		id         string
		node       string
		role       string
		leader     string
		subCluster string
		loadWeight int64
	}

	var mppInfos []mppInfo

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for rows.Next() {
		var info mppInfo

		for i := range valuePtrs {
			valuePtrs[i] = &values[i]
		}

		err = rows.Scan(valuePtrs...)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		for i, col := range columns {
			switch strings.ToLower(col) {
			case "id":
				info.id = string(values[i].([]byte))
			case "node":
				info.node = string(values[i].([]byte))
			case "role":
				info.role = string(values[i].([]byte))
			case "leader":
				info.leader = string(values[i].([]byte))
			case "sub_cluster":
				info.subCluster = string(values[i].([]byte))
			case "load_weight":
				loadWeight, ok := values[i].(int64)
				if !ok {
					loadWeight = 100
				}
				info.loadWeight = loadWeight
			}
		}

		mppInfos = append(mppInfos, info)
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("Error iterating rows: %v", err)
	}

	// Process each MPP info
	for _, info := range mppInfos {
		t.Logf("Processing MPP Info: ID=%s, NODE=%s, ROLE=%s, LEADER=%s, SUB_CLUSTER=%s, LOAD_WEIGHT=%d",
			info.id, info.node, info.role, info.leader, info.subCluster, info.loadWeight)

		// Create direct connection string based on role
		var directDsn string
		if strings.EqualFold(info.role, "W") {
			// For write role, create read-write connection
			directDsn = fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
				"&directMode=true",
				*user, *passwd, info.node, *enableLog, *enableProbeLog)
			t.Logf("Creating read-write connection to %s", info.node)

			// Test read-write connection
			testCNReadWriteConnection(t, directDsn, info.node)
		} else if strings.EqualFold(info.role, "R") {
			// For read role, create read-only connection
			directDsn = fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s"+
				"&directMode=true",
				*user, *passwd, info.node, *enableLog, *enableProbeLog)
			t.Logf("Creating read-only connection to %s", info.node)

			// Test read-only connection
			testCNReadOnlyConnection(t, directDsn, info.node)
		} else {
			t.Logf("Unknown role %s for node %s, skipping", info.role, info.node)
		}
	}
}

func TestCNZoneName(t *testing.T) {
	if *testType != "cn" {
		t.Skip("Skip non-cn test")
	}
	// Create indirect connection to get MPP information
	dsnIndirect := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)

	// Open database connection
	db, err := sql.Open("polardbx", dsnIndirect)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close database: %v", err)
		}
	}()

	// Execute show mpp command to get CN information
	rows, err := db.Query("show mpp")
	if err != nil {
		t.Fatalf("Failed to execute show mpp: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("Failed to close rows: %v", err)
		}
	}()

	// Parse the results
	type mppInfo struct {
		id         string
		node       string
		role       string
		leader     string
		subCluster string
		loadWeight int64
	}

	var mppInfos []mppInfo
	var rwNodes []mppInfo // Read-write nodes
	var roNodes []mppInfo // Read-only nodes

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for rows.Next() {
		var info mppInfo

		for i := range valuePtrs {
			valuePtrs[i] = &values[i]
		}

		err = rows.Scan(valuePtrs...)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		for i, col := range columns {
			switch strings.ToLower(col) {
			case "id":
				info.id = string(values[i].([]byte))
			case "node":
				info.node = string(values[i].([]byte))
			case "role":
				info.role = string(values[i].([]byte))
			case "leader":
				info.leader = string(values[i].([]byte))
			case "sub_cluster":
				info.subCluster = string(values[i].([]byte))
			case "load_weight":
				loadWeight, ok := values[i].(int64)
				if !ok {
					loadWeight = 100
				}
				info.loadWeight = loadWeight
			}
		}

		mppInfos = append(mppInfos, info)

		// Collect read-write and read-only nodes separately
		if strings.EqualFold(info.role, "W") {
			rwNodes = append(rwNodes, info)
		} else if strings.EqualFold(info.role, "R") {
			roNodes = append(roNodes, info)
		}
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("Error iterating rows: %v", err)
	}

	// If we don't have enough nodes for testing, skip the test
	if len(rwNodes) < 2 && len(roNodes) < 2 {
		t.Skip("Not enough CN nodes for zone name testing")
	}

	// Group nodes by subCluster (zoneName)
	zoneNodes := make(map[string][]mppInfo)
	for _, node := range mppInfos {
		zoneName := strings.TrimSpace(node.subCluster)
		if zoneName != "" {
			zoneNodes[zoneName] = append(zoneNodes[zoneName], node)
		}
	}

	// If we don't have multiple zones, skip the test
	if len(zoneNodes) < 2 {
		t.Skip("Not enough zones for zone name testing")
	}

	// Test 1: Test zoneName parameter - should only connect to nodes in specified zone
	t.Log("Test 1: Testing zoneName parameter")
	for zoneName, nodes := range zoneNodes {
		if len(nodes) == 0 {
			continue
		}

		// Create DSN with zoneName parameter
		dsnWithZone := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&zoneName=%s",
			*user, *passwd, *addr, *enableLog, *enableProbeLog, zoneName)

		t.Logf("Connect with dsn: %s", dsnWithZone)
		dbZone, err := sql.Open("polardbx", dsnWithZone)
		if err != nil {
			t.Errorf("Failed to open database with zoneName %s: %v", zoneName, err)
			continue
		}

		conn, err := dbZone.Conn(context.Background())
		if err != nil {
			dbZone.Close()
			t.Errorf("Failed to get connection with zoneName %s: %v", zoneName, err)
			continue
		}

		// Get the actual connected node IP and port
		actualIP, err := getMySQLServerIP(conn)
		if err != nil {
			conn.Close()
			dbZone.Close()
			t.Errorf("Failed to get MySQL server IP with zoneName %s: %v", zoneName, err)
			continue
		}

		// Verify that the connected node is in the specified zone
		isInZone := false
		expectedNodes := make([]string, 0)
		for _, node := range nodes {
			expectedNodes = append(expectedNodes, node.node)
			if node.node == actualIP {
				isInZone = true
				break
			}
		}

		if !isInZone {
			t.Errorf("Connected to node %s which is not in zone %s. Expected one of: %v", actualIP, zoneName, expectedNodes)
		} else {
			t.Logf("Successfully connected to node %s in zone %s", actualIP, zoneName)
		}

		conn.Close()
		dbZone.Close()
	}

	// Test 2: Test multiple zoneNames - should connect to nodes in any of the specified zones
	t.Log("Test 2: Testing multiple zoneNames parameter")
	zoneNames := make([]string, 0, len(zoneNodes))
	for zoneName := range zoneNodes {
		zoneNames = append(zoneNames, zoneName)
		if len(zoneNames) >= 2 {
			break
		}
	}

	if len(zoneNames) >= 2 {
		multipleZones := strings.Join(zoneNames, ",")
		dsnWithMultipleZones := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&zoneName=%s",
			*user, *passwd, *addr, *enableLog, *enableProbeLog, multipleZones)

		t.Logf("Connect with dsn: %s", dsnWithMultipleZones)
		dbMultiZone, err := sql.Open("polardbx", dsnWithMultipleZones)
		if err != nil {
			t.Errorf("Failed to open database with multiple zoneNames %s: %v", multipleZones, err)
		} else {
			conn, err := dbMultiZone.Conn(context.Background())
			if err != nil {
				dbMultiZone.Close()
				t.Errorf("Failed to get connection with multiple zoneNames %s: %v", multipleZones, err)
			} else {
				actualIP, err := getMySQLServerIP(conn)
				if err != nil {
					conn.Close()
					dbMultiZone.Close()
					t.Errorf("Failed to get MySQL server IP with multiple zoneNames %s: %v", multipleZones, err)
				} else {
					// Verify that the connected node is in one of the specified zones
					isInAnyZone := false
					for _, zoneName := range zoneNames {
						for _, node := range zoneNodes[zoneName] {
							if node.node == actualIP {
								isInAnyZone = true
								break
							}
						}
						if isInAnyZone {
							break
						}
					}

					if !isInAnyZone {
						t.Errorf("Connected to node %s which is not in any of the zones %s", actualIP, multipleZones)
					} else {
						t.Logf("Successfully connected to node %s in one of the zones %s", actualIP, multipleZones)
					}

					conn.Close()
				}
			}
			dbMultiZone.Close()
		}
	}

	// Test 3: Test minZoneNodes and backupZoneName parameters
	t.Log("Test 3: Testing minZoneNodes and backupZoneName parameters")
	// Find a zone with at least 1 node and another zone as backup
	var primaryZone, backupZone string

	for zoneName, nodes := range zoneNodes {
		if len(nodes) >= 1 && primaryZone == "" {
			primaryZone = zoneName
		} else if len(nodes) >= 1 && backupZone == "" {
			backupZone = zoneName
		}

		if primaryZone != "" && backupZone != "" {
			break
		}
	}

	if primaryZone != "" && backupZone != "" {
		// Test with minZoneNodes=0 and primary zone does not have sufficient nodes
		dsnWithMinNodes := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&zoneName=%s&minZoneNodes=100&backupZoneName=%s",
			*user, *passwd, *addr, *enableLog, *enableProbeLog, primaryZone, backupZone)

		t.Logf("Connect with dsn: %s", dsnWithMinNodes)
		dbMinNodes, err := sql.Open("polardbx", dsnWithMinNodes)
		if err != nil {
			t.Errorf("Failed to open database with minZoneNodes: %v", err)
		} else {
			conn, err := dbMinNodes.Conn(context.Background())
			if err != nil {
				dbMinNodes.Close()
				t.Errorf("Failed to get connection with minZoneNodes: %v", err)
			} else {
				actualIP, err := getMySQLServerIP(conn)
				if err != nil {
					conn.Close()
					dbMinNodes.Close()
					t.Errorf("Failed to get MySQL server IP with minZoneNodes: %v", err)
				} else {
					// Should connect to backup node since it does not have sufficient nodes
					isInBackupNode := false
					for _, node := range zoneNodes[backupZone] {
						if node.node == actualIP {
							isInBackupNode = true
							break
						}
					}

					if !isInBackupNode {
						t.Errorf("Expected connection to backup zone %s, but connected to %s", backupZone, actualIP)
					} else {
						t.Logf("Successfully connected to backup zone %s node %s", backupZone, actualIP)
					}

					conn.Close()
				}
			}
			dbMinNodes.Close()
		}
	}

	t.Log("TestCNZoneName completed successfully!")
}

func TestCNInstanceName(t *testing.T) {
	if *testType != "cn" {
		t.Skip("Skip non-cn test")
	}
	// Create indirect connection to get MPP information
	dsnIndirect := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)

	// Open database connection
	db, err := sql.Open("polardbx", dsnIndirect)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close database: %v", err)
		}
	}()

	// Execute show mpp command to get CN information
	rows, err := db.Query("show mpp")
	if err != nil {
		t.Fatalf("Failed to execute show mpp: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("Failed to close rows: %v", err)
		}
	}()

	// Parse the results
	type mppInfo struct {
		id         string
		node       string
		role       string
		leader     string
		subCluster string
		loadWeight int64
	}

	var mppInfos []mppInfo

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for rows.Next() {
		var info mppInfo

		for i := range valuePtrs {
			valuePtrs[i] = &values[i]
		}

		err = rows.Scan(valuePtrs...)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		for i, col := range columns {
			switch strings.ToLower(col) {
			case "id":
				info.id = string(values[i].([]byte))
			case "node":
				info.node = string(values[i].([]byte))
			case "role":
				info.role = string(values[i].([]byte))
			case "leader":
				info.leader = string(values[i].([]byte))
			case "sub_cluster":
				info.subCluster = string(values[i].([]byte))
			case "load_weight":
				loadWeight, ok := values[i].(int64)
				if !ok {
					loadWeight = 100
				}
				info.loadWeight = loadWeight
			}
		}

		mppInfos = append(mppInfos, info)
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("Error iterating rows: %v", err)
	}

	// Group nodes by instanceName (ID column)
	instanceNodes := make(map[string][]mppInfo)
	instanceRoles := make(map[string]string) // Track role for each instance
	for _, info := range mppInfos {
		instanceNodes[info.id] = append(instanceNodes[info.id], info)
		// Record the role of the instance (assuming all nodes in an instance have the same role)
		instanceRoles[info.id] = info.role
	}

	// Test each instanceName
	t.Log("Test 1: Testing instanceName parameter")
	for instanceName, nodes := range instanceNodes {
		t.Logf("Testing instanceName: %s", instanceName)

		// Determine if this is a read-only instance based on MPP role
		isReadOnly := strings.EqualFold(instanceRoles[instanceName], "R")

		// Create DSN with instanceName parameter
		dsnBase := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&instanceName=%s",
			*user, *passwd, *addr, *enableLog, *enableProbeLog, instanceName)

		// Add slaveRead=true if this is a read-only instance
		dsnWithInstanceName := dsnBase
		if isReadOnly {
			dsnWithInstanceName = fmt.Sprintf("%s&slaveRead=true", dsnBase)
		}

		t.Logf("Connect with dsn: %s", dsnWithInstanceName)
		dbInstance, err := sql.Open("polardbx", dsnWithInstanceName)
		if err != nil {
			t.Errorf("Failed to open database with instanceName %s: %v", instanceName, err)
			continue
		}

		conn, err := dbInstance.Conn(context.Background())
		if err != nil {
			dbInstance.Close()
			t.Errorf("Failed to get connection with instanceName %s: %v", instanceName, err)
			continue
		}

		actualIP, err := getMySQLServerIP(conn)
		if err != nil {
			conn.Close()
			dbInstance.Close()
			t.Errorf("Failed to get MySQL server IP with instanceName %s: %v", instanceName, err)
			continue
		}

		// Verify that the connected node is in the specified instance
		isInInstance := false
		expectedNodes := make([]string, 0)
		for _, node := range nodes {
			expectedNodes = append(expectedNodes, node.node)
			if node.node == actualIP {
				isInInstance = true
				break
			}
		}

		if !isInInstance {
			t.Errorf("Connected to node %s which is not in instance %s. Expected one of: %v", actualIP, instanceName, expectedNodes)
		} else {
			t.Logf("Successfully connected to node %s in instance %s", actualIP, instanceName)
			// Log whether this was treated as a read-only connection
			if isReadOnly {
				t.Logf("Instance %s is read-only (role=R), connected with slaveRead=true", instanceName)
			} else {
				t.Logf("Instance %s is read-write (role=%s)", instanceName, instanceRoles[instanceName])
			}
		}

		conn.Close()
		dbInstance.Close()
	}

	t.Log("TestCNInstanceName completed successfully!")
}

func TestCNMppRole(t *testing.T) {
	if *testType != "cn" {
		t.Skip("Skip non-cn test")
	}
	// Create DSN for indirect connection to get MPP information
	dsnIndirect := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)

	// Open database connection
	db, err := sql.Open("polardbx", dsnIndirect)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close database: %v", err)
		}
	}()

	// Execute show mpp command to get CN information
	rows, err := db.Query("show mpp")
	if err != nil {
		t.Fatalf("Failed to execute show mpp: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("Failed to close rows: %v", err)
		}
	}()

	// Parse the results
	type mppInfo struct {
		id         string
		node       string
		role       string
		leader     string
		subCluster string
		loadWeight int64
	}

	var mppInfos []mppInfo

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for rows.Next() {
		var info mppInfo

		for i := range valuePtrs {
			valuePtrs[i] = &values[i]
		}

		err = rows.Scan(valuePtrs...)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		for i, col := range columns {
			switch strings.ToLower(col) {
			case "id":
				info.id = string(values[i].([]byte))
			case "node":
				info.node = string(values[i].([]byte))
			case "role":
				info.role = string(values[i].([]byte))
			case "leader":
				info.leader = string(values[i].([]byte))
			case "sub_cluster":
				info.subCluster = string(values[i].([]byte))
			case "load_weight":
				loadWeight, ok := values[i].(int64)
				if !ok {
					loadWeight = 100
				}
				info.loadWeight = loadWeight
			}
		}

		mppInfos = append(mppInfos, info)
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("Error iterating rows: %v", err)
	}

	// Group nodes by role
	roleNodes := make(map[string][]mppInfo)
	for _, info := range mppInfos {
		roleNodes[info.role] = append(roleNodes[info.role], info)
	}

	// Test mppRole=W (should connect to W nodes only)
	t.Log("Test 1: Testing mppRole=W parameter")
	if wNodes, exists := roleNodes["W"]; exists && len(wNodes) > 0 {
		// Create DSN with mppRole=W parameter
		dsnWithMppRoleW := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&mppRole=W",
			*user, *passwd, *addr, *enableLog, *enableProbeLog)

		t.Logf("Connect with dsn: %s", dsnWithMppRoleW)
		dbW, err := sql.Open("polardbx", dsnWithMppRoleW)
		if err != nil {
			t.Errorf("Failed to open database with mppRole=W: %v", err)
		} else {
			conn, err := dbW.Conn(context.Background())
			if err != nil {
				dbW.Close()
				t.Errorf("Failed to get connection with mppRole=W: %v", err)
			} else {
				actualIP, err := getMySQLServerIP(conn)
				if err != nil {
					conn.Close()
					dbW.Close()
					t.Errorf("Failed to get MySQL server IP with mppRole=W: %v", err)
				} else {
					// Verify that the connected node has role W
					isWNode := false
					expectedWNodes := make([]string, 0)
					for _, node := range wNodes {
						expectedWNodes = append(expectedWNodes, node.node)
						if node.node == actualIP {
							isWNode = true
							break
						}
					}

					if !isWNode {
						t.Errorf("Connected to node %s which is not a W node. Expected one of: %v", actualIP, expectedWNodes)
					} else {
						t.Logf("Successfully connected to W node %s", actualIP)
					}
					conn.Close()
				}
			}
			dbW.Close()
		}
	} else {
		t.Log("No W nodes found, skipping mppRole=W test")
	}

	// Test mppRole=R (should connect to R nodes only)
	t.Log("Test 2: Testing mppRole=R parameter")
	if rNodes, exists := roleNodes["R"]; exists && len(rNodes) > 0 {
		// Create DSN with mppRole=R parameter
		dsnWithMppRoleR := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&mppRole=R&slaveRead=true",
			*user, *passwd, *addr, *enableLog, *enableProbeLog)

		t.Logf("Connect with dsn: %s", dsnWithMppRoleR)
		dbR, err := sql.Open("polardbx", dsnWithMppRoleR)
		if err != nil {
			t.Errorf("Failed to open database with mppRole=R: %v", err)
		} else {
			conn, err := dbR.Conn(context.Background())
			if err != nil {
				dbR.Close()
				t.Errorf("Failed to get connection with mppRole=R: %v", err)
			} else {
				actualIP, err := getMySQLServerIP(conn)
				if err != nil {
					conn.Close()
					dbR.Close()
					t.Errorf("Failed to get MySQL server IP with mppRole=R: %v", err)
				} else {
					// Verify that the connected node has role R
					isRNode := false
					expectedRNodes := make([]string, 0)
					for _, node := range rNodes {
						expectedRNodes = append(expectedRNodes, node.node)
						if node.node == actualIP {
							isRNode = true
							break
						}
					}

					if !isRNode {
						t.Errorf("Connected to node %s which is not an R node. Expected one of: %v", actualIP, expectedRNodes)
					} else {
						t.Logf("Successfully connected to R node %s", actualIP)
					}
					conn.Close()
				}
			}
			dbR.Close()
		}
	} else {
		t.Log("No R nodes found, skipping mppRole=R test")
	}

	// Test default behavior without mppRole (should connect to W nodes for master, R nodes for slaveRead)
	t.Log("Test 3: Testing default behavior without mppRole parameter")

	// Test default master connection (should connect to W nodes)
	dsnDefaultMaster := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)

	t.Logf("Connect with default master dsn: %s", dsnDefaultMaster)
	dbDefaultMaster, err := sql.Open("polardbx", dsnDefaultMaster)
	if err != nil {
		t.Errorf("Failed to open database with default master connection: %v", err)
	} else {
		conn, err := dbDefaultMaster.Conn(context.Background())
		if err != nil {
			dbDefaultMaster.Close()
			t.Errorf("Failed to get connection with default master connection: %v", err)
		} else {
			actualIP, err := getMySQLServerIP(conn)
			if err != nil {
				conn.Close()
				dbDefaultMaster.Close()
				t.Errorf("Failed to get MySQL server IP with default master connection: %v", err)
			} else {
				// Verify that the connected node has role W (default for master)
				isWNode := false
				var expectedWNodes []string
				if wNodes, exists := roleNodes["W"]; exists {
					for _, node := range wNodes {
						expectedWNodes = append(expectedWNodes, node.node)
						if node.node == actualIP {
							isWNode = true
							break
						}
					}
				}

				if !isWNode {
					t.Logf("Note: Connected to node %s which is not a W node. Expected one of: %v. This may be expected behavior depending on cluster configuration.", actualIP, expectedWNodes)
				} else {
					t.Logf("Successfully connected to W node %s for default master connection", actualIP)
				}
				conn.Close()
			}
		}
		dbDefaultMaster.Close()
	}

	// Test default slaveRead connection (should connect to R nodes)
	dsnDefaultSlave := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&slaveRead=true",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)

	t.Logf("Connect with default slaveRead dsn: %s", dsnDefaultSlave)
	dbDefaultSlave, err := sql.Open("polardbx", dsnDefaultSlave)
	if err != nil {
		t.Errorf("Failed to open database with default slaveRead connection: %v", err)
	} else {
		conn, err := dbDefaultSlave.Conn(context.Background())
		if err != nil {
			dbDefaultSlave.Close()
			t.Errorf("Failed to get connection with default slaveRead connection: %v", err)
		} else {
			actualIP, err := getMySQLServerIP(conn)
			if err != nil {
				conn.Close()
				dbDefaultSlave.Close()
				t.Errorf("Failed to get MySQL server IP with default slaveRead connection: %v", err)
			} else {
				// Verify that the connected node has role R (default for slaveRead)
				isRNode := false
				var expectedRNodes []string
				if rNodes, exists := roleNodes["R"]; exists {
					for _, node := range rNodes {
						expectedRNodes = append(expectedRNodes, node.node)
						if node.node == actualIP {
							isRNode = true
							break
						}
					}
				}

				if !isRNode {
					t.Logf("Note: Connected to node %s which is not an R node. Expected one of: %v. This may be expected behavior depending on cluster configuration.", actualIP, expectedRNodes)
				} else {
					t.Logf("Successfully connected to R node %s for default slaveRead connection", actualIP)
				}
				conn.Close()
			}
		}
		dbDefaultSlave.Close()
	}

	t.Log("TestCNMppRole completed successfully!")
}

func TestCNGroup(t *testing.T) {
	if *testType != "cn" {
		t.Skip("Skip non-cn test")
	}
	// Create indirect connection to get MPP information
	dsnIndirect := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)

	// Open database connection
	db, err := sql.Open("polardbx", dsnIndirect)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close database: %v", err)
		}
	}()

	// Execute show mpp command to get CN information
	rows, err := db.Query("show mpp")
	if err != nil {
		t.Fatalf("Failed to execute show mpp: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("Failed to close rows: %v", err)
		}
	}()

	// Parse the results
	type mppInfo struct {
		id         string
		node       string
		role       string
		leader     string
		subCluster string
		loadWeight int64
	}

	var mppInfos []mppInfo

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for rows.Next() {
		var info mppInfo

		for i := range valuePtrs {
			valuePtrs[i] = &values[i]
		}

		err = rows.Scan(valuePtrs...)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		for i, col := range columns {
			switch strings.ToLower(col) {
			case "id":
				info.id = string(values[i].([]byte))
			case "node":
				info.node = string(values[i].([]byte))
			case "role":
				info.role = string(values[i].([]byte))
			case "leader":
				info.leader = string(values[i].([]byte))
			case "sub_cluster":
				info.subCluster = string(values[i].([]byte))
			case "load_weight":
				loadWeight, ok := values[i].(int64)
				if !ok {
					loadWeight = 100
				}
				info.loadWeight = loadWeight
			}
		}

		mppInfos = append(mppInfos, info)
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("Error iterating rows: %v", err)
	}

	// Group nodes by subCluster (zoneName)
	zoneNodes := make(map[string][]mppInfo)
	groups := make(map[string][]mppInfo)
	groupToZone := make(map[string]string)
	for _, node := range mppInfos {
		if node.role != "W" {
			continue
		}
		zoneName := strings.TrimSpace(node.subCluster)
		if zoneName != "" {
			zoneNodes[zoneName] = append(zoneNodes[zoneName], node)
		}
		groups[node.node] = append(groups[node.node], node)
		groupToZone[node.node] = zoneName
	}

	// If we don't have multiple zones, skip the test
	if len(groups) < 2 {
		t.Skip("Not enough zones for group testing")
	}

	// Test 1: Test cnGroup parameter - should only connect to nodes in specified group
	t.Log("Test 1: Testing cnGroup parameter")
	// Find a zone with at least one node
	var primaryGroup string
	var primaryNodes []mppInfo

	for group, nodes := range groups {
		if len(nodes) >= 1 {
			primaryGroup = group
			primaryNodes = zoneNodes[groupToZone[group]]
			break
		}
	}

	if primaryGroup != "" && len(primaryNodes) > 0 {
		// Create cnGroup string with IPs from the first node in the zone
		// Format: ip0:port0,ip1:port1,...

		// Create DSN with cnGroup parameter
		dsnWithGroup := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&cnGroup=%s",
			*user, *passwd, *addr, *enableLog, *enableProbeLog, primaryGroup)

		t.Logf("Connect with dsn: %s", dsnWithGroup)
		dbGroup, err := sql.Open("polardbx", dsnWithGroup)
		if err != nil {
			t.Errorf("Failed to open database with cnGroup %s: %v", primaryGroup, err)
		} else {
			conn, err := dbGroup.Conn(context.Background())
			if err != nil {
				dbGroup.Close()
				t.Errorf("Failed to get connection with cnGroup %s: %v", primaryGroup, err)
			} else {
				// Get the actual connected node IP and port
				actualIP, err := getMySQLServerIP(conn)
				if err != nil {
					conn.Close()
					dbGroup.Close()
					t.Errorf("Failed to get MySQL server IP with cnGroup %s: %v", primaryGroup, err)
				} else {
					// Verify that the connected node is in the specified zone
					isInZone := false
					expectedNodes := make([]string, 0)
					for _, node := range primaryNodes {
						expectedNodes = append(expectedNodes, node.node)
						if node.node == actualIP {
							isInZone = true
							break
						}
					}

					if !isInZone {
						t.Errorf("Connected to node %s which is not in zone %s. Expected one of: %v", actualIP, groupToZone[primaryGroup], expectedNodes)
					} else {
						t.Logf("Successfully connected to node %s in zone %s via cnGroup %s", actualIP, groupToZone[primaryGroup], primaryGroup)
					}

					conn.Close()
				}
			}
			dbGroup.Close()
		}
	}

	// Test 2: Test backupCnGroup parameter - similar behavior to backupZoneName
	t.Log("Test 2: Testing backupCnGroup parameter")
	// Find a primary group and a backup group
	var backupGroup string
	var backupNodes []mppInfo

	// Find a different group to use as backup
	for group, nodes := range groups {
		if group != primaryGroup && len(nodes) >= 1 {
			backupGroup = group
			backupNodes = zoneNodes[groupToZone[group]]
			break
		}
	}

	if primaryGroup != "" && backupGroup != "" && len(primaryNodes) > 0 && len(backupNodes) > 0 {
		// Test with minZoneNodes set to a large value to trigger backupCnGroup
		// This mimics the behavior of backupZoneName in TestCNZoneName
		dsnWithBackupGroup := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&cnGroup=%s&minZoneNodes=100&backupCnGroup=%s",
			*user, *passwd, *addr, *enableLog, *enableProbeLog, primaryGroup, backupGroup)

		t.Logf("Connect with dsn: %s", dsnWithBackupGroup)
		dbBackupGroup, err := sql.Open("polardbx", dsnWithBackupGroup)
		if err != nil {
			t.Errorf("Failed to open database with backupCnGroup: %v", err)
		} else {
			conn, err := dbBackupGroup.Conn(context.Background())
			if err != nil {
				dbBackupGroup.Close()
				t.Errorf("Failed to get connection with backupCnGroup: %v", err)
			} else {
				actualIP, err := getMySQLServerIP(conn)
				if err != nil {
					conn.Close()
					dbBackupGroup.Close()
					t.Errorf("Failed to get MySQL server IP with backupCnGroup: %v", err)
				} else {
					// Should connect to backup node since primary group does not have sufficient nodes
					// due to minZoneNodes being set to a large value
					isInBackupNode := false
					expectedBackupNodes := make([]string, 0)
					for _, node := range backupNodes {
						expectedBackupNodes = append(expectedBackupNodes, node.node)
						if node.node == actualIP {
							isInBackupNode = true
							break
						}
					}

					if !isInBackupNode {
						t.Errorf("Expected connection to backup group %s, but connected to %s. Expected one of: %v", backupGroup, actualIP, expectedBackupNodes)
					} else {
						t.Logf("Successfully connected to backup group %s node %s", backupGroup, actualIP)
					}

					conn.Close()
				}
			}
			dbBackupGroup.Close()
		}
	} else {
		t.Log("Skipping backupCnGroup test due to insufficient groups")
	}

	t.Log("TestCNGroup completed successfully!")
}

// runSimpleCase executes the simple case scenario with the given database connection
func runCNSimpleCase(t *testing.T, db *sql.DB) {
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
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s mode=auto", dbName))
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
			id BIGINT PRIMARY KEY,
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
	insertPartitionedSQL := fmt.Sprintf("INSERT INTO %s (id, name, email, age) VALUES (?, ?, ?, ?)", partitionedTableName)
	result, err = db.Exec(insertPartitionedSQL, 1, "Charlie", "charlie@example.com", 35)
	if err != nil {
		t.Fatalf("Failed to insert data into partitioned table: %v", err)
	}

	result, err = db.Exec(insertPartitionedSQL, 2, "David", "david@example.com", 40)
	if err != nil {
		t.Fatalf("Failed to insert data into partitioned table: %v", err)
	}

	// Read - Query data from partitioned table
	t.Log("Querying data from partitioned table")
	queryPartitionedSQL := fmt.Sprintf("SELECT id, name, email, age FROM %s WHERE id = ?", partitionedTableName)
	var pid int
	var pname, pemail string
	var page int
	err = db.QueryRow(queryPartitionedSQL, 1).Scan(&pid, &pname, &pemail, &page)
	if err != nil {
		t.Fatalf("Failed to query data from partitioned table: %v", err)
	}
	if pid != int(1) || pname != "Charlie" || pemail != "charlie@example.com" || page != 35 {
		t.Errorf("Query result mismatch in partitioned table: got (%d, %s, %s, %d), want (%d, Charlie, charlie@example.com, 35)", pid, pname, pemail, page, 1)
	}

	// Update - Update data in partitioned table
	t.Log("Updating data in partitioned table")
	updatePartitionedSQL := fmt.Sprintf("UPDATE %s SET age = ? WHERE id = ?", partitionedTableName)
	_, err = db.Exec(updatePartitionedSQL, 36, 1)
	if err != nil {
		t.Fatalf("Failed to update data in partitioned table: %v", err)
	}

	// Verify update in partitioned table
	t.Log("Verifying updated data in partitioned table")
	err = db.QueryRow(queryPartitionedSQL, 1).Scan(&pid, &pname, &pemail, &page)
	if err != nil {
		t.Fatalf("Failed to query updated data from partitioned table: %v", err)
	}
	if page != 36 {
		t.Errorf("Update failed in partitioned table: got age %d, want 36", page)
	}

	// Delete - Delete data from partitioned table
	t.Log("Deleting data from partitioned table")
	deletePartitionedSQL := fmt.Sprintf("DELETE FROM %s WHERE id = ?", partitionedTableName)
	_, err = db.Exec(deletePartitionedSQL, 2)
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

// testCNReadWriteConnection tests read-write connection by creating database and table (expected to succeed)
func testCNReadWriteConnection(t *testing.T, dsn string, node string) {
	db, err := sql.Open("polardbx", dsn)
	t.Logf("Test read write with dsn: %s", dsn)
	if err != nil {
		t.Errorf("Failed to open read-write connection to %s: %v", node, err)
		return
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close read-write connection to %s: %v", node, err)
		}
	}()

	// Test creating database (should succeed)
	dbName := fmt.Sprintf("test_cn_direct_mode_rw_%d", time.Now().UnixNano())
	t.Logf("Creating database %s on read-write node %s", dbName, node)

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	if err != nil {
		t.Errorf("Failed to create database on read-write node %s: %v", node, err)
		return
	}

	// Clean up database
	defer func() {
		_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
		if err != nil {
			t.Errorf("Failed to drop database on read-write node %s: %v", node, err)
		}
	}()

	// Use database
	_, err = db.Exec(fmt.Sprintf("USE %s", dbName))
	if err != nil {
		t.Errorf("Failed to use database on read-write node %s: %v", node, err)
		return
	}

	// Test creating table (should succeed)
	tableName := "test_table"
	t.Logf("Creating table %s on read-write node %s", tableName, node)

	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(50) NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`, tableName)

	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Errorf("Failed to create table on read-write node %s: %v", node, err)
		return
	}

	t.Logf("Successfully created database and table on read-write node %s", node)
}

// testCNReadOnlyConnection tests read-only connection by attempting to create database and table (expected to fail)
func testCNReadOnlyConnection(t *testing.T, dsn string, node string) {
	db, err := sql.Open("polardbx", dsn)
	t.Logf("Test read only with dsn: %s", dsn)
	if err != nil {
		t.Errorf("Failed to open read-only connection to %s: %v", node, err)
		return
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close read-only connection to %s: %v", node, err)
		}
	}()

	// Test creating database (should fail)
	dbName := fmt.Sprintf("test_cn_direct_mode_ro_%d", time.Now().UnixNano())
	t.Logf("Attempting to create database %s on read-only node %s (expected to fail)", dbName, node)

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	if err != nil {
		t.Logf("Correctly failed to create database on read-only node %s: %v", node, err)
	} else {
		// Clean up database if it was somehow created
		_, cleanupErr := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
		if cleanupErr != nil {
			t.Errorf("Failed to clean up database on read-only node %s: %v", node, cleanupErr)
		}
		t.Errorf("Expected failure when creating database on read-only node %s, but it succeeded", node)
		return
	}

	// Even if we couldn't create a database, let's try creating a table directly (should also fail)
	tableName := "test_table"
	t.Logf("Attempting to create table %s on read-only node %s (expected to fail)", tableName, node)

	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(50) NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`, tableName)

	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Logf("Correctly failed to create table on read-only node %s: %v", node, err)
	} else {
		t.Errorf("Expected failure when creating table on read-only node %s, but it succeeded", node)
		return
	}

	t.Logf("Correctly failed database and table creation attempts on read-only node %s", node)
}

func TestCNWrongJsonFile(t *testing.T) {
	if *testType != "cn" {
		t.Skip("Skip non-cn test")
	}

	tmpDir := os.TempDir()
	jsonFile := filepath.Join(tmpDir, fmt.Sprintf("PolarDB-X-%s-IPv4-go.json", sanitizeAddr(*addr)))
	t.Logf("jsonFile: %s", jsonFile)

	err := ioutil.WriteFile(jsonFile, []byte("123123"), 0644)
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	t.Logf("Wrote file with wrong content")

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s",
		*user, *passwd, *addr, *enableLog, *enableProbeLog)

	db, err := sql.Open("polardbx", dsn)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("Failed to close database: %v", err)
		}
	}()

	_, err = db.Query("select 1")
	if err != nil {
		t.Fatalf("Failed to execute select 1: %v", err)
	}
}
