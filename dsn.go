package polardbx

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"

	mapset "github.com/deckarep/golang-set"
)

type PolarDBXConfig struct {
	// Dsn before Slash
	User   string // Username
	Passwd string // Password (requires User)
	Net    string // Network (e.g. "tcp", "tcp6", "unix". default: "tcp")
	Addr   string // Address (default: "127.0.0.1:3306" for "tcp" and "/tmp/mysql.sock" for "unix")
	Dbname string // Database name

	JsonFile string // json file path

	// PolarDBX params
	ClusterID                             int64
	HaTimeoutMillis                       int32 // connect timeout for getting node, default: 10000
	HaCheckConnectTimeoutMillis           int32 // dial timeout for HA mysql connection
	HaCheckSocketTimeoutMillis            int32 // read timeout for HA mysql connection
	HaCheckIntervalMillis                 int32 // the same as CheckLeaderTransferringIntervalMillis in DN
	CheckLeaderTransferringIntervalMillis int32
	LeaderTransferringWaitTimeoutMillis   int32 // timeout for leader transferring
	SmoothSwitchover                      bool  // TODO: to be supported
	IgnoreVip                             bool
	RecordJdbcUrl                         bool
	SlaveOnly                             bool // read write separation, slaveRead
	SlaveWeightThreshold                  int32
	ApplyDelayThreshold                   int32 // second
	DirectMode                            bool
	LoadBalanceAlgorithm                  string
	EnableLog                             bool
	EnableProbeLog                        bool

	// PolarDBX params: CN only
	ZoneName           string
	MinZoneNodes       int32
	BackupZoneName     string
	InstanceName       string
	MppRole            string
	EnableFollowerRead int32
	CnGroup            string
	BackupCnGroup      string

	// MYSQL params
	MysqlParams map[string]string

	// PolarDBX param set
	PropertiesSet mapset.Set
}

func NewPolarDBXConfig() *PolarDBXConfig {
	cfg := &PolarDBXConfig{
		ClusterID:                             -1,
		HaTimeoutMillis:                       10000,
		HaCheckConnectTimeoutMillis:           3000,
		HaCheckSocketTimeoutMillis:            3000,
		CheckLeaderTransferringIntervalMillis: 100,
		HaCheckIntervalMillis:                 5000,
		LeaderTransferringWaitTimeoutMillis:   5000,
		SlaveWeightThreshold:                  1,
		ApplyDelayThreshold:                   3,
		IgnoreVip:                             true,
		EnableFollowerRead:                    -1,
		LoadBalanceAlgorithm:                  random,
		SlaveOnly:                             false,
		EnableLog:                             true,
		EnableProbeLog:                        true,
		MysqlParams:                           make(map[string]string),
		PropertiesSet:                         mapset.NewSet(),
	}

	cfg.MysqlParams["parseTime"] = "true"
	cfg.MysqlParams["charset"] = "utf8mb4"

	cfg.PropertiesSet.Add("clusterid")
	cfg.PropertiesSet.Add("connecttimeout")
	cfg.PropertiesSet.Add("hacheckconnecttimeoutmillis")
	cfg.PropertiesSet.Add("hachecksockettimeoutmillis")
	cfg.PropertiesSet.Add("hacheckintervalmillis")
	cfg.PropertiesSet.Add("checkleadertransferringintervalmillis")
	cfg.PropertiesSet.Add("leadertransferringwaittimeoutmillis")
	cfg.PropertiesSet.Add("smoothswitchover")
	cfg.PropertiesSet.Add("ignorevip")
	cfg.PropertiesSet.Add("recordjdbcurl")
	cfg.PropertiesSet.Add("zonename")
	cfg.PropertiesSet.Add("slaveread")
	cfg.PropertiesSet.Add("instancename")
	cfg.PropertiesSet.Add("mpprole")
	cfg.PropertiesSet.Add("slaveweightthreshold")
	cfg.PropertiesSet.Add("applydelaythreshold")
	cfg.PropertiesSet.Add("directmode")
	cfg.PropertiesSet.Add("backupzonename")
	cfg.PropertiesSet.Add("minzonenodes")
	cfg.PropertiesSet.Add("enablefollowerread")
	cfg.PropertiesSet.Add("loadbalancealgorithm")
	cfg.PropertiesSet.Add("enablelog")
	cfg.PropertiesSet.Add("enableprobelog")
	cfg.PropertiesSet.Add("cngroup")
	cfg.PropertiesSet.Add("backupcngroup")

	return cfg
}

func (pCfg *PolarDBXConfig) Clone() *PolarDBXConfig {
	newConfig := &PolarDBXConfig{
		User:     pCfg.User,
		Passwd:   pCfg.Passwd,
		Net:      pCfg.Net,
		Addr:     pCfg.Addr,
		Dbname:   pCfg.Dbname,
		JsonFile: pCfg.JsonFile,

		ClusterID:                             pCfg.ClusterID,
		HaTimeoutMillis:                       pCfg.HaTimeoutMillis,
		HaCheckConnectTimeoutMillis:           pCfg.HaCheckConnectTimeoutMillis,
		HaCheckSocketTimeoutMillis:            pCfg.HaCheckSocketTimeoutMillis,
		HaCheckIntervalMillis:                 pCfg.HaCheckIntervalMillis,
		CheckLeaderTransferringIntervalMillis: pCfg.CheckLeaderTransferringIntervalMillis,
		LeaderTransferringWaitTimeoutMillis:   pCfg.LeaderTransferringWaitTimeoutMillis,
		SmoothSwitchover:                      pCfg.SmoothSwitchover,
		IgnoreVip:                             pCfg.IgnoreVip,
		RecordJdbcUrl:                         pCfg.RecordJdbcUrl,
		ZoneName:                              pCfg.ZoneName,
		SlaveOnly:                             pCfg.SlaveOnly,
		InstanceName:                          pCfg.InstanceName,
		MppRole:                               pCfg.MppRole,
		ApplyDelayThreshold:                   pCfg.ApplyDelayThreshold,
		DirectMode:                            pCfg.DirectMode,
		BackupZoneName:                        pCfg.BackupZoneName,
		MinZoneNodes:                          pCfg.MinZoneNodes,
		EnableFollowerRead:                    pCfg.EnableFollowerRead,
		SlaveWeightThreshold:                  pCfg.SlaveWeightThreshold,
		LoadBalanceAlgorithm:                  pCfg.LoadBalanceAlgorithm,
		EnableLog:                             pCfg.EnableLog,
		EnableProbeLog:                        pCfg.EnableProbeLog,
		CnGroup:                               pCfg.CnGroup,
		BackupCnGroup:                         pCfg.BackupCnGroup,
	}

	if pCfg.MysqlParams != nil {
		newConfig.MysqlParams = make(map[string]string, len(pCfg.MysqlParams))
		for k, v := range pCfg.MysqlParams {
			newConfig.MysqlParams[k] = v
		}
	}

	newConfig.PropertiesSet = mapset.NewSet()
	for _, item := range pCfg.PropertiesSet.ToSlice() {
		newConfig.PropertiesSet.Add(item)
	}

	return newConfig
}

func (pCfg *PolarDBXConfig) normalize() error {

	// Set default network if empty
	if pCfg.Net == "" {
		pCfg.Net = "tcp"
	}

	// Set default address if empty
	if pCfg.Addr == "" {
		switch pCfg.Net {
		case "tcp":
			pCfg.Addr = "127.0.0.1:3306"
		case "unix":
			pCfg.Addr = "/tmp/mysql.sock"
		default:
			return errors.New("default addr for network '" + pCfg.Net + "' unknown")
		}
	} else if pCfg.Net == "tcp" {
		pCfg.Addr = ensureHavePort(pCfg.Addr)
	}

	if strings.EqualFold(pCfg.LoadBalanceAlgorithm, leastConn) {
		pCfg.LoadBalanceAlgorithm = leastConn
	} else {
		pCfg.LoadBalanceAlgorithm = random
	}

	return nil
}

func writeDSNParam(buf *bytes.Buffer, hasParam *bool, name, value string) {
	buf.Grow(1 + len(name) + 1 + len(value))
	if !*hasParam {
		*hasParam = true
		buf.WriteByte('?')
	} else {
		buf.WriteByte('&')
	}
	buf.WriteString(name)
	buf.WriteByte('=')
	buf.WriteString(value)
}

func (pCfg *PolarDBXConfig) FormatPolarDBXDSN(addr string) string {
	var buf bytes.Buffer

	// [username[:password]@]
	if len(pCfg.User) > 0 {
		buf.WriteString(pCfg.User)
		if len(pCfg.Passwd) > 0 {
			buf.WriteByte(':')
			buf.WriteString(pCfg.Passwd)
		}
		buf.WriteByte('@')
	}

	// [protocol[(address)]]
	if len(pCfg.Net) > 0 {
		buf.WriteString(pCfg.Net)
		if len(addr) > 0 {
			buf.WriteByte('(')
			buf.WriteString(addr)
			buf.WriteByte(')')
		}
	}

	buf.WriteByte('/')

	// [?param1=value1&...&paramN=valueN]
	hasParam := false

	// other params
	if pCfg.MysqlParams != nil {
		var params []string
		for param := range pCfg.MysqlParams {
			if !strings.EqualFold(param, "timeout") && !strings.EqualFold(param, "readTimeout") {
				params = append(params, param)
			}
		}
		sort.Strings(params)
		for _, param := range params {
			writeDSNParam(&buf, &hasParam, param, url.QueryEscape(pCfg.MysqlParams[param]))
		}
		writeDSNParam(&buf, &hasParam, "timeout", url.QueryEscape(fmt.Sprintf("%dms", pCfg.HaCheckConnectTimeoutMillis)))
		writeDSNParam(&buf, &hasParam, "readTimeout", url.QueryEscape(fmt.Sprintf("%dms", pCfg.HaCheckSocketTimeoutMillis)))
	}

	return buf.String()
}

func (pCfg *PolarDBXConfig) FormatMYSQLDSN(addr string) (string, bool) {
	var buf bytes.Buffer

	// [username[:password]@]
	if len(pCfg.User) > 0 {
		buf.WriteString(pCfg.User)
		if len(pCfg.Passwd) > 0 {
			buf.WriteByte(':')
			buf.WriteString(pCfg.Passwd)
		}
		buf.WriteByte('@')
	}

	// [protocol[(address)]]
	if len(pCfg.Net) > 0 {
		buf.WriteString(pCfg.Net)
		if len(addr) > 0 {
			buf.WriteByte('(')
			buf.WriteString(addr)
			buf.WriteByte(')')
		}
	}

	buf.WriteByte('/')
	buf.WriteString(pCfg.Dbname)

	// [?param1=value1&...&paramN=valueN]
	hasParam := false

	// other params
	if pCfg.MysqlParams != nil {
		var params []string
		for param := range pCfg.MysqlParams {
			params = append(params, param)
		}
		sort.Strings(params)
		for _, param := range params {
			writeDSNParam(&buf, &hasParam, param, url.QueryEscape(pCfg.MysqlParams[param]))
		}
	}

	return buf.String(), hasParam
}

// ParsePolarDBXDSN parses the DSN string to polardb-x and mysql Config
func ParsePolarDBXDSN(dsn string) (pCfg *PolarDBXConfig, err error) {

	// New config with some default values
	pCfg = NewPolarDBXConfig()

	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]
	// Find the last '/' (since the password or the net addr might contain a '/')
	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true
			var j, k int

			// left part is empty if i <= 0
			if i > 0 {
				// [username[:password]@][protocol[(address)]]
				// Find the last '@' in leaderDsn[:i]
				for j = i; j >= 0; j-- {
					if dsn[j] == '@' {
						// username[:password]
						// Find the first ':' in leaderDsn[:j]
						for k = 0; k < j; k++ {
							if dsn[k] == ':' {
								pCfg.Passwd = dsn[k+1 : j]
								break
							}
						}
						pCfg.User = dsn[:k]

						break
					}
				}

				// [protocol[(address)]]
				// Find the first '(' in leaderDsn[j+1:i]
				for k = j + 1; k < i; k++ {
					if dsn[k] == '(' {
						// leaderDsn[i-1] must be == ')' if an address is specified
						if dsn[i-1] != ')' {
							if strings.ContainsRune(dsn[k+1:i], ')') {
								return nil, ErrInvalidDSNUnescaped
							}
							return nil, ErrInvalidDSNAddr
						}
						pCfg.Addr = dsn[k+1 : i-1]
						break
					}
				}
				pCfg.Net = dsn[j+1 : k]
			}

			// dbname[?param1=value1&...&paramN=valueN]
			// Find the first '?' in leaderDsn[i+1:]
			for j = i + 1; j < len(dsn); j++ {
				if dsn[j] == '?' {
					if err = parsePolarDBXDSNParams(pCfg, dsn[j+1:]); err != nil {
						return
					}
					break
				}
			}

			dbname := dsn[i+1 : j]
			pCfg.Dbname = dbname
			break
		}
	}

	if !foundSlash && len(dsn) > 0 {
		return nil, ErrInvalidDSNNoSlash
	}

	if err = pCfg.normalize(); err != nil {
		return nil, err
	}
	return
}

func parsePolarDBXDSNParams(pCfg *PolarDBXConfig, params string) error {
	for _, v := range strings.Split(params, "&") {
		key, value, found := strings.Cut(v, "=")
		if !found {
			continue
		}
		if pCfg.PropertiesSet.Contains(strings.ToLower(key)) {
			switch strings.ToLower(key) {
			case "clusterid":
				var isInt64 bool
				pCfg.ClusterID, isInt64 = readInt64(value)
				if !isInt64 {
					return errors.New("invalid int64 value: " + value)
				}
			case "connecttimeout":
				var isInt32 bool
				pCfg.HaTimeoutMillis, isInt32 = readInt32(value)
				if !isInt32 {
					return errors.New("invalid int32 value: " + value)
				}
			case "hacheckconnecttimeoutmillis":
				var isInt32 bool
				pCfg.HaCheckConnectTimeoutMillis, isInt32 = readInt32(value)
				if !isInt32 {
					return errors.New("invalid int32 value: " + value)
				}
			case "hachecksockettimeoutmillis":
				var isInt32 bool
				pCfg.HaCheckSocketTimeoutMillis, isInt32 = readInt32(value)
				if !isInt32 {
					return errors.New("invalid int32 value: " + value)
				}
			case "hacheckintervalmillis":
				var isInt32 bool
				pCfg.HaCheckIntervalMillis, isInt32 = readInt32(value)
				if !isInt32 {
					return errors.New("invalid int32 value: " + value)
				}
			case "checkleadertransferringintervalmillis":
				var isInt32 bool
				pCfg.CheckLeaderTransferringIntervalMillis, isInt32 = readInt32(value)
				if !isInt32 {
					return errors.New("invalid int32 value: " + value)
				}
			case "leadertransferringwaittimeoutmillis":
				var isInt32 bool
				pCfg.LeaderTransferringWaitTimeoutMillis, isInt32 = readInt32(value)
				if !isInt32 {
					return errors.New("invalid int32 value: " + value)
				}
			case "slaveweightthreshold":
				var isInt32 bool
				pCfg.SlaveWeightThreshold, isInt32 = readInt32(value)
				if !isInt32 {
					return errors.New("invalid int32 value: " + value)
				}
			case "applydelaythreshold":
				var isInt32 bool
				pCfg.ApplyDelayThreshold, isInt32 = readInt32(value)
				if !isInt32 {
					return errors.New("invalid int32 value: " + value)
				}
			case "ignorevip":
				var isBool bool
				pCfg.IgnoreVip, isBool = readBool(value)
				if !isBool {
					return errors.New("invalid bool value: " + value)
				}
			case "recordjdbcurl":
				var isBool bool
				pCfg.RecordJdbcUrl, isBool = readBool(value)
				if !isBool {
					return errors.New("invalid bool value: " + value)
				}
			case "enablefollowerread":
				var isInt32 bool
				pCfg.EnableFollowerRead, isInt32 = readInt32(value)
				if !isInt32 {
					return errors.New("invalid int32 value: " + value)
				}
			case "enablelog":
				var isBool bool
				pCfg.EnableLog, isBool = readBool(value)
				if !isBool {
					return errors.New("invalid bool value: " + value)
				}
			case "enableprobelog":
				var isBool bool
				pCfg.EnableProbeLog, isBool = readBool(value)
				if !isBool {
					return errors.New("invalid bool value: " + value)
				}
			case "slaveread":
				var isBool bool
				pCfg.SlaveOnly, isBool = readBool(value)
				if !isBool {
					return errors.New("invalid bool value: " + value)
				}
			case "directmode":
				var isBool bool
				pCfg.DirectMode, isBool = readBool(value)
				if !isBool {
					return errors.New("invalid bool value: " + value)
				}
			case "instancename":
				pCfg.InstanceName = value
			case "mpprole":
				pCfg.MppRole = value
			case "zonename":
				pCfg.ZoneName = value
			case "backupzonename":
				pCfg.BackupZoneName = value
			case "minzonenodes":
				var isInt32 bool
				pCfg.MinZoneNodes, isInt32 = readInt32(value)
				if !isInt32 {
					return errors.New("invalid int32 value: " + value)
				}
			case "loadbalancealgorithm":
				pCfg.LoadBalanceAlgorithm = value
			case "cngroup":
				pCfg.CnGroup = value
			case "backupcngroup":
				pCfg.BackupCnGroup = value
			default:

			}
		} else {
			pCfg.MysqlParams[key] = value
		}
	}
	return nil
}

func ensureHavePort(addr string) string {
	var buf bytes.Buffer

	hasAddr := false
	addresses := strings.Split(addr, ",")
	for _, address := range addresses {
		address = strings.TrimSpace(address)
		if hasAddr {
			buf.WriteString(",")
		} else {
			hasAddr = true
		}
		if _, _, err := net.SplitHostPort(address); err != nil {
			buf.WriteString(net.JoinHostPort(address, "3306"))
		} else {
			buf.WriteString(address)
		}
	}

	return buf.String()
}
