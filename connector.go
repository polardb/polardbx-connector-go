package polardbx

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"

	"github.com/go-sql-driver/mysql"
)

type connector struct {
	hm  *HaManager
	cfg *PolarDBXConfig
}

func newConnector(hm *HaManager, pCfg *PolarDBXConfig) *connector {
	return &connector{
		hm:  hm,
		cfg: pCfg,
	}
}

// Connect implements driver.Connector interface.
// Connect returns a connection to the database.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	hm := c.hm
	cfg := c.cfg

	var connectAddress string
	var err error
	if hm.isDn {
		connectAddress, err = hm.getAvailableDnWithWait(cfg.HaTimeoutMillis, cfg.SlaveOnly,
			cfg.ApplyDelayThreshold, cfg.SlaveWeightThreshold, cfg.LoadBalanceAlgorithm)
	} else {
		connectAddress, err = hm.getAvailableCnWithWait(cfg.HaTimeoutMillis, cfg.ZoneName, cfg.MinZoneNodes,
			cfg.BackupZoneName, cfg.SlaveOnly, cfg.InstanceName, cfg.MppRole, cfg.LoadBalanceAlgorithm, cfg.CnGroup, cfg.BackupCnGroup)
	}

	if err != nil {
		return nil, err
	} else {
		if connectAddress == "" {
			return nil, ErrNoNodeFound
		}
		mainLogger.Debug(fmt.Sprintf("The connect node is: %s", connectAddress), cfg.EnableLog)

		mysqlDsn, _ := cfg.FormatMYSQLDSN(connectAddress)
		
		mysqlCfg, err := mysql.ParseDSN(mysqlDsn)
		if err != nil {
			c.countDownConn(connectAddress)
			return nil, err
		}
		
		if len(cfg.MysqlOptions) > 0 {
			err = mysqlCfg.Apply(cfg.MysqlOptions...)
			if err != nil {
				c.countDownConn(connectAddress)
				return nil, err
			}
		}

		openConnector, err := mysql.NewConnector(mysqlCfg)
		if err != nil {
			c.countDownConn(connectAddress)
			return nil, err
		}

		conn, err := openConnector.Connect(ctx)
		if err != nil {
			c.countDownConn(connectAddress)
			return nil, err
		}

		if hm.isDn || cfg.SlaveOnly {
			if cfg.EnableFollowerRead != -1 {
				mainLogger.Warn(fmt.Sprintf("The follower read is not supported, please check your config: %s", cfg.FormatPolarDBXDSN(cfg.Addr)), cfg.EnableLog)
			}
		} else {
			err = enableFollowerRead(conn, cfg.EnableFollowerRead)
			if err != nil {
				c.countDownConn(connectAddress)
				return nil, err
			}
		}

		return &polardbxConn{mysqlConn: conn, hm: hm, connId: connectAddress}, err
	}
}

// we have in advance add conn count, if it failed to create, we should count down
func (c *connector) countDownConn(addr string) {
	if !c.hm.isDn || c.cfg.SlaveOnly {
		c.hm.decrementConnCounter(addr)
	}
}

func (c *connector) Driver() driver.Driver {
	return &PolarDBXDriver{}
}

func enableFollowerRead(conn driver.Conn, followerReadState int32) (err error) {
	switch followerReadState {
	case -1:
		// do nothing
	case 0:
		err = executeStmt(conn, setFollowerReadFalse, []driver.Value{})
		if err != nil {
			return err
		}
	case 1:
		err1 := executeStmt(conn, setFollowerReadTrue, []driver.Value{})
		err2 := executeStmt(conn, setReadWeight, []driver.Value{})
		err3 := executeStmt(conn, enableConsistentReadFalse, []driver.Value{})
		if err1 == nil && err2 == nil && err3 == nil {
			return
		}
		var errBuf bytes.Buffer
		mergeError(&errBuf, err1)
		mergeError(&errBuf, err2)
		mergeError(&errBuf, err3)
		err = errors.New(errBuf.String())
	case 2:
		err1 := executeStmt(conn, setFollowerReadTrue, []driver.Value{})
		err2 := executeStmt(conn, setReadWeight, []driver.Value{})
		err3 := executeStmt(conn, enableConsistentReadTrue, []driver.Value{})
		if err1 == nil && err2 == nil && err3 == nil {
			return
		}
		var errBuf bytes.Buffer
		mergeError(&errBuf, err1)
		mergeError(&errBuf, err2)
		mergeError(&errBuf, err3)
		err = errors.New(errBuf.String())
	}
	return
}

func executeStmt(conn driver.Conn, query string, values []driver.Value) (err error) {
	stmt, err := conn.Prepare(query)
	if err != nil {
		return
	}
	_, err = stmt.Exec(values)
	if err != nil {
		return
	}
	defer func(stmt driver.Stmt) {
		err = stmt.Close()
		if err != nil {
			log.Printf("close stmt error: %v", err)
		}
	}(stmt)
	return
}

func mergeError(buf *bytes.Buffer, err error) {
	if err != nil {
		buf.WriteString(err.Error())
	}
}
