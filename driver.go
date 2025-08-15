package polardbx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/go-sql-driver/mysql"
)

type PolarDBXDriver struct{}

// Open new Connection.
// See https://github.com/go-sql-driver/mysql#dsn-data-source-name for how
// the DSN string is formatted
func (d PolarDBXDriver) Open(dsn string) (driver.Conn, error) {
	pCfg, err1 := ParsePolarDBXDSN(dsn)
	if err1 != nil {
		return nil, err1
	}

	if pCfg.DirectMode {
		driverCtx := &mysql.MySQLDriver{}
		return driverCtx.Open(pCfg.FormatMYSQLDSN(pCfg.Addr))
	}

	// construct HA Manager and get the real address for mysql connection
	cfg := pCfg.Clone()

	if haManager, err2 := GetManager(cfg); err2 != nil {
		return nil, err2
	} else {
		return newConnector(haManager, pCfg).Connect(context.Background())
	}
}

var driverName = "polardbx"

func init() {
	if driverName != "" {
		sql.Register(driverName, &PolarDBXDriver{})
	}
}

// NewConnector returns new driver.Connector.
func NewConnector(pCfg *PolarDBXConfig) (driver.Connector, error) {
	if pCfg.DirectMode {
		driverCtx := &mysql.MySQLDriver{}
		return driverCtx.OpenConnector(pCfg.FormatMYSQLDSN(pCfg.Addr))
	}

	// construct HA Manager and get the real address for mysql connection
	cfg := pCfg.Clone()

	if haManager, err2 := GetManager(cfg); err2 != nil {
		return nil, err2
	} else {
		return newConnector(haManager, pCfg), nil
	}
}

// OpenConnector implements driver.DriverContext.
func (d PolarDBXDriver) OpenConnector(dsn string) (driver.Connector, error) {
	pCfg, err1 := ParsePolarDBXDSN(dsn)
	if err1 != nil {
		return nil, err1
	}

	if pCfg.DirectMode {
		driverCtx := &mysql.MySQLDriver{}
		return driverCtx.OpenConnector(pCfg.FormatMYSQLDSN(pCfg.Addr))
	}

	// construct HA Manager and get the real address for mysql connection
	cfg := pCfg.Clone()

	if haManager, err2 := GetManager(cfg); err2 != nil {
		return nil, err2
	} else {
		if pCfg.RecordJdbcUrl {
			if err := recordDsn(dsn, pCfg, haManager); err != nil {
				return nil, err
			}
		}
		return newConnector(haManager, pCfg), nil
	}
}

func recordDsn(dsn string, pCfg *PolarDBXConfig, hm *HaManager) error {
	var connectAddress string
	var err error
	if hm.isDn {
		connectAddress, err = hm.getAvailableDnWithWait(pCfg.HaTimeoutMillis, pCfg.SlaveOnly,
			pCfg.ApplyDelayThreshold, pCfg.SlaveWeightThreshold, pCfg.LoadBalanceAlgorithm)
	} else {
		connectAddress, err = hm.getAvailableCnWithWait(pCfg.HaTimeoutMillis, pCfg.ZoneName, pCfg.MinZoneNodes,
			pCfg.BackupZoneName, pCfg.SlaveOnly, pCfg.InstanceName, pCfg.MppRole, pCfg.LoadBalanceAlgorithm)
	}
	if err != nil {
		return err
	} else {
		db, err := sql.Open("mysql", pCfg.FormatMYSQLDSN(connectAddress))
		if err != nil {
			return err
		}
		_, err = db.Exec(fmt.Sprintf(recordDsnQuery, dsn))
		if err != nil {
			return err
		}
		return nil
	}
}
