package polardbx

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var (
	addr                       = flag.String("addr", "127.0.0.1:3306", "Test Address, multiple addresses separated by commas.")
	user                       = flag.String("user", "root", "Test Username.")
	passwd                     = flag.String("passwd", "", "Test Password.")
	haCheckIntervalMillis      = flag.Int("haCheckIntervalMillis", 5000, "HA check interval millis.")
	haCheckSocketTimeoutMillis = flag.Int("haCheckSocketTimeoutMillis", 3000, "HA check socket timeout millis.")
	connectTimeout             = flag.Int("connectTimeout", 10000, "HA timeout millis.")
	enableLog                  = flag.String("enableLog", "true", "Enable log.")
	enableProbeLog             = flag.String("enableProbeLog", "false", "Enable probe log.")
	addr2                      = flag.String("addr2", "127.0.0.1:3306", "Test Address, multiple addresses separated by commas.")
	user2                      = flag.String("user2", "root", "Test Username.")
	passwd2                    = flag.String("passwd2", "", "Test Password.")
	loadBalanceAlgorithm       = flag.String("loadBalanceAlgorithm", "random", "Load balance algorithm.")
	testType                   = flag.String("testType", "dn", "Test type.")
	dbname                     = flag.String("dbname", "test_go_db", "Test database name.")
)

func TestMain(m *testing.M) {
	flag.Parse()
	flag.Usage = func() {
		log.Print("Usage of integration tests:\n")
		flag.PrintDefaults()
	}
	tmp := strings.Split(*addr, ":")
	port = tmp[len(tmp)-1]
	netAddr = fmt.Sprintf("%s(%s)", port, *addr)
	dsn = fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&enableLog=%s&enableProbeLog=%s",
		*user, *passwd, *addr, *dbname, *enableLog, *enableProbeLog)
	available = true
	os.Exit(m.Run())
}

func getMySQLServerIP(conn *sql.Conn) (string, error) {
	ipAddrStr := ""
	conn.Raw(func(polardbxConn interface{}) error {
		driverConn := reflect.ValueOf(polardbxConn).Elem().FieldByName("mysqlConn")
		netConn := driverConn.Elem().Elem().FieldByName("netConn")
		conn := netConn.Elem().Elem().FieldByName("conn")
		fd := conn.FieldByName("fd")
		raddr := fd.Elem().FieldByName("raddr")
		IP := raddr.Elem().Elem().FieldByName("IP")
		PORT := raddr.Elem().Elem().FieldByName("Port")
		var parts []string
		for i := 0; i < 4; i++ {
			byteVal := IP.Index(i).Uint()
			parts = append(parts, strconv.FormatUint(byteVal, 10))
		}
		ipAddrStr = strings.Join(parts, ".")
		ipAddrStr = ipAddrStr + ":" + strconv.Itoa(int(PORT.Int()))
		return nil
	})

	return ipAddrStr, nil
}
