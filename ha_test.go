package polardbx

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	haReadThreads  = flag.Int("haReadThreads", 2, "number of read thread of HA read write test.")
	haWriteThreads = flag.Int("haWriteThreads", 2, "number of write thread of HA read write test.")
	haTestDuration = flag.Int("testDuration", 10, "test duration in seconds.")
)

const (
	INIT_DATA_COUNT = 100
	LOG_ERR         = true
)

// TestHaConcurrency 执行 HA 并发测试
func TestHaConcurrency(t *testing.T) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s&loadBalanceAlgorithm=%s",
		*user, *passwd, *addr, *enableLog, *enableProbeLog, *loadBalanceAlgorithm)

	t.Logf("test duration: %d seconds, dsn: %s", *haTestDuration, dsn)
	if err := HaConcurrencyTest(t, dsn, *haReadThreads, *haWriteThreads, *haTestDuration); err != nil {
		t.Fatalf("HA Concurrency Test failed: %+v", err)
	}
}

// HaConcurrencyTest 执行 HA 并发测试
func HaConcurrencyTest(t *testing.T, dsn string, readThreads int, writeThreads int, timeSeconds int) error {
	// 1. 创建数据库和表
	db, err := sql.Open("polardbx", dsn)
	if err != nil {
		debug.PrintStack()
		t.Fatalf("Failed to open database: %v", err)
		return err
	}
	defer db.Close()

	// 创建数据库
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS test_ha_db")
	if err != nil {
		debug.PrintStack()
		t.Fatalf("Failed to create database: %v", err)
		return err
	}

	// 检查表是否存在
	var tableName string
	err = db.QueryRow("SHOW TABLES FROM test_ha_db LIKE 'test_ha_table'").Scan(&tableName)
	tableExists := err == nil

	if tableExists {
		// 检查数据量
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM test_ha_db.test_ha_table WHERE id>0 AND id<=?", INIT_DATA_COUNT).Scan(&count)
		if err != nil {
			debug.PrintStack()
			t.Fatalf("Failed to check table: %v", err)
			return err
		} else if count != INIT_DATA_COUNT {
			tableExists = false
		}
	}

	if !tableExists {
		// 创建表
		_, err = db.Exec("DROP TABLE IF EXISTS test_ha_db.test_ha_table")
		if err != nil {
			debug.PrintStack()
			t.Fatalf("Failed to drop table: %v", err)
			return err
		}

		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS test_ha_db.test_ha_table (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			value INT,
			tid INT) ENGINE=InnoDB`)
		if err != nil {
			debug.PrintStack()
			t.Fatalf("Failed to create table: %v", err)
			return err
		}

		_, err = db.Exec("TRUNCATE TABLE test_ha_db.test_ha_table")
		if err != nil {
			debug.PrintStack()
			t.Fatalf("Failed to truncate table: %v", err)
			return err
		}

		// 插入初始数据
		for i := 0; i < INIT_DATA_COUNT; i++ {
			_, err = db.Exec("INSERT INTO test_ha_db.test_ha_table (value, tid) VALUES (?, ?)", 1, 1)
			if err != nil {
				debug.PrintStack()
				t.Fatalf("Failed to insert data: %v", err)
				return err
			}
		}
	}

	// 2. 并发测试配置
	exit := int32(0)
	var readTransactions int64
	var writeTransactions int64
	var readErrors int64
	var writeErrors int64
	var errorVal atomic.Value

	testDuration := time.Duration(timeSeconds) * time.Second
	testEndTime := time.Now().Add(testDuration)

	var readUrlKeys []string
	readUrlCnt := make(map[string]*int64)
	var writeUrlKeys []string
	writeUrlCnt := make(map[string]*int64)
	urlMutex := sync.Mutex{}

	var wg sync.WaitGroup

	// 启动读线程
	for i := 0; i < readThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for atomic.LoadInt32(&exit) == 0 && time.Now().Before(testEndTime) {
				func() {
					db, err := sql.Open("polardbx", dsn)
					if err != nil {
						atomic.AddInt64(&readErrors, 1)
						if LOG_ERR {
							debug.PrintStack()
							t.Errorf("[ERROR] Open db failed: %+v", err)
						}
						time.Sleep(100 * time.Millisecond)
						return
					}
					defer db.Close()

					// 获取连接URL
					conn, err := db.Conn(context.Background())
					if err != nil {
						atomic.AddInt64(&readErrors, 1)
						if LOG_ERR {
							debug.PrintStack()
							t.Errorf("[ERROR] Get conn failed: %+v", err)
						}
						time.Sleep(100 * time.Millisecond)
						return
					}
					defer conn.Close()
					host, err := getMySQLServerIP(conn)
					if err != nil {
						atomic.AddInt64(&readErrors, 1)
						if LOG_ERR {
							debug.PrintStack()
							t.Errorf("[ERROR] Get host failed: %+v", err)
						}
						time.Sleep(100 * time.Millisecond)
						return
					}
					urlMutex.Lock()
					if readUrlCnt[host] == nil {
						readUrlCnt[host] = new(int64)
						readUrlKeys = append(readUrlKeys, host)
					}
					atomic.AddInt64(readUrlCnt[host], 1)
					urlMutex.Unlock()

					// 执行查询
					id := rand.Intn(INIT_DATA_COUNT) + 1
					var rowsCount int
					err = db.QueryRow("SELECT COUNT(*) FROM test_ha_db.test_ha_table WHERE id = ?", id).Scan(&rowsCount)
					if err != nil {
						if LOG_ERR {
							debug.PrintStack()
							t.Errorf("[ERROR] Read failed: %+v", err)
						}
						atomic.AddInt64(&readErrors, 1)
						time.Sleep(100 * time.Millisecond)
						return
					}

					if rowsCount != 1 {
						atomic.AddInt64(&readErrors, 1)
						t.Errorf("Read failed row count != 1, row count: %d", rowsCount)
						time.Sleep(100 * time.Millisecond)
						return
					}

					atomic.AddInt64(&readTransactions, 1)
				}()

				// 随机休眠
				time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			}
		}()
	}

	// 启动写线程
	for i := 0; i < writeThreads; i++ {
		threadId := i + 1
		wg.Add(1)
		go func() {
			defer wg.Done()
			for atomic.LoadInt32(&exit) == 0 && time.Now().Before(testEndTime) {
				func() {
					db, err := sql.Open("polardbx", dsn)
					if err != nil {
						atomic.AddInt64(&writeErrors, 1)
						if LOG_ERR {
							debug.PrintStack()
							t.Errorf("[ERROR] Open db failed: %+v", err)
						}
						time.Sleep(100 * time.Millisecond)
						return
					}
					defer db.Close()

					// 获取连接URL
					conn, err := db.Conn(context.Background())
					if err != nil {
						if LOG_ERR {
							debug.PrintStack()
							t.Errorf("[ERROR] Get conn failed: %+v", err)
						}
						time.Sleep(100 * time.Millisecond)
						atomic.AddInt64(&writeErrors, 1)
						return
					}
					defer conn.Close()
					host, err := getMySQLServerIP(conn)
					if err != nil {
						if LOG_ERR {
							debug.PrintStack()
							t.Errorf("[ERROR] Get host failed: %+v", err)
						}
						time.Sleep(100 * time.Millisecond)
						atomic.AddInt64(&writeErrors, 1)
						return
					}
					urlMutex.Lock()
					if writeUrlCnt[host] == nil {
						writeUrlCnt[host] = new(int64)
						writeUrlKeys = append(writeUrlKeys, host)
					}
					atomic.AddInt64(writeUrlCnt[host], 1)
					urlMutex.Unlock()

					// 开启事务
					tx, err := db.BeginTx(context.Background(), nil)
					if err != nil {
						if LOG_ERR {
							debug.PrintStack()
							t.Errorf("[ERROR] Write failed: %+v", err)
						}
						atomic.AddInt64(&writeErrors, 1)
						time.Sleep(100 * time.Millisecond)
						return
					}

					defer func() {
						if err != nil {
							tx.Rollback()
						}
					}()

					// 插入数据
					result, err := tx.Exec("INSERT INTO test_ha_db.test_ha_table (value, tid) VALUES (?, ?)",
						rand.Intn(1000000), threadId)
					if err != nil {
						if LOG_ERR {
							debug.PrintStack()
							t.Errorf("[ERROR] Write failed: %+v", err)
						}
						atomic.AddInt64(&writeErrors, 1)
						time.Sleep(100 * time.Millisecond)
						return
					}

					// 获取插入的ID
					lastInsertId, err := result.LastInsertId()
					if err != nil {
						if LOG_ERR {
							debug.PrintStack()
							t.Errorf("[ERROR] Write failed: %+v", err)
						}
						atomic.AddInt64(&writeErrors, 1)
						time.Sleep(100 * time.Millisecond)
						return
					}

					// 休眠50毫秒
					time.Sleep(50 * time.Millisecond)

					// 删除数据
					result, err = tx.Exec("DELETE FROM test_ha_db.test_ha_table WHERE id = ?", lastInsertId)
					if err != nil {
						if LOG_ERR {
							debug.PrintStack()
							t.Errorf("[ERROR] Write failed: %+v", err)
						}
						atomic.AddInt64(&writeErrors, 1)
						time.Sleep(100 * time.Millisecond)
						return
					}

					// 检查删除的行数
					rowsAffected, err := result.RowsAffected()
					if err != nil {
						if LOG_ERR {
							debug.PrintStack()
							t.Errorf("[ERROR] Write failed: %+v", err)
						}
						atomic.AddInt64(&writeErrors, 1)
						time.Sleep(100 * time.Millisecond)
						return
					}
					if rowsAffected != 1 {
						atomic.AddInt64(&writeErrors, 1)
						t.Errorf("Write failed, affect rows != 1, affect rows: %d", rowsAffected)
						time.Sleep(100 * time.Millisecond)
						return
					}

					// 提交事务
					err = tx.Commit()
					if err != nil {
						if LOG_ERR {
							debug.PrintStack()
							t.Errorf("[ERROR] Write failed: %+v", err)
						}
						atomic.AddInt64(&writeErrors, 1)
						time.Sleep(100 * time.Millisecond)
						return
					}

					atomic.AddInt64(&writeTransactions, 1)
				}()

				// 随机休眠
				time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			}
		}()
	}

	// 打印统计信息
	lastReadTransactions := int64(0)
	lastWriteTransactions := int64(0)
	lastReadErrors := int64(0)
	lastWriteErrors := int64(0)

	for atomic.LoadInt32(&exit) == 0 && time.Now().Before(testEndTime) {
		time.Sleep(1 * time.Second)

		currentReadTransactions := atomic.LoadInt64(&readTransactions)
		currentWriteTransactions := atomic.LoadInt64(&writeTransactions)
		currentReadErrors := atomic.LoadInt64(&readErrors)
		currentWriteErrors := atomic.LoadInt64(&writeErrors)

		fmt.Printf("%s read: %d err: %d, write: %d err: %d\n",
			time.Now().Format("2006/01/02 15:04:05.000"),
			currentReadTransactions-lastReadTransactions,
			currentReadErrors-lastReadErrors,
			currentWriteTransactions-lastWriteTransactions,
			currentWriteErrors-lastWriteErrors)

		lastReadTransactions = currentReadTransactions
		lastWriteTransactions = currentWriteTransactions
		lastReadErrors = currentReadErrors
		lastWriteErrors = currentWriteErrors

		// 显示URL统计
		fmt.Println("READS:")
		urlMutex.Lock()
		for _, url := range readUrlKeys {
			if count, exists := readUrlCnt[url]; exists {
				fmt.Printf("  CONNECT ADDR: %s, Count: %d\n", url, atomic.LoadInt64(count))
			}
		}
		fmt.Println("WRITES:")
		for _, url := range writeUrlKeys {
			if count, exists := writeUrlCnt[url]; exists {
				fmt.Printf("  CONNECT ADDR: %s, Count: %d\n", url, atomic.LoadInt64(count))
			}
		}
		urlMutex.Unlock()
	}

	// 等待所有协程完成
	wg.Wait()

	// 处理结果
	if err, ok := errorVal.Load().(error); ok && err != nil {
		fmt.Println("Test failed due to error:", err)
	}

	fmt.Printf("HA Concurrency Test completed. read: %d, write: %d\n",
		atomic.LoadInt64(&readTransactions), atomic.LoadInt64(&writeTransactions))

	// 显示URL统计
	fmt.Println("READS:")
	urlMutex.Lock()
	for _, url := range readUrlKeys {
		if count, exists := readUrlCnt[url]; exists {
			fmt.Printf("  CONNECT ADDR: %s, Count: %d\n", url, atomic.LoadInt64(count))
		}
	}
	fmt.Println("WRITES:")
	for _, url := range writeUrlKeys {
		if count, exists := writeUrlCnt[url]; exists {
			fmt.Printf("  CONNECT ADDR: %s, Count: %d\n", url, atomic.LoadInt64(count))
		}
	}
	urlMutex.Unlock()

	if err, ok := errorVal.Load().(error); ok && err != nil {
		fmt.Println("========================================")
		return err
	}

	return nil
}
