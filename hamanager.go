package polardbx

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/petermattis/goid"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deckarep/golang-set"
	"github.com/pkg/errors"
)

// 将字符串形式的地址解析为包含IP和端口的InetSocketAddress对象。
// 支持IPv4、IPv6以及通过DNS解析的地址格式。
//
// 格式说明：
// - IPv4（如 "127.0.0.1"）默认使用3306端口
// - IPv4带端口（如 "127.0.0.1:3307"）
// - IPv6（如 "[::1]"）默认使用3306端口
// - IPv6带端口（如 "[::1]:3307"）
// - TODO：DNS解析格式（如 "host@dns_server:port"）
//
// 示例：
// decode("192.168.1.1") -> IPv4 默认端口 3306
// decode("[fe80::1]:3307") -> IPv6 指定端口 3307
// decode("example.com@8.8.8.8:53") -> 使用指定DNS服务器解析域名
func decode(address string) (Address, error) {
	split := strings.Split(address, ":")

	if len(split) == 1 {
		// IPv4 without port
		return Address{Hostname: split[0], Port: 3306}, nil
	} else if len(split) == 2 {
		// IPv4 with port
		port, err := parsePort(split[1])
		return Address{Hostname: split[0], Port: port}, err
	} else {
		endPos := strings.Index(address, "]")
		if endPos != -1 {
			portStart := strings.Index(address[endPos+1:], ":") + endPos + 1
			if portStart != -1 {
				// IPv6 with port
				port, err := parsePort(address[portStart+1:])
				return Address{Hostname: address[1:endPos], Port: port}, err
			}
			return Address{Hostname: address[1:endPos], Port: 3306}, nil
		}
		// IPv6 without port
		return Address{Hostname: address, Port: 3306}, nil
	}
}

// genClusterTag 生成集群标识，同一个集群同一个数据库的连接，使用同一个HA管理器
// ip:port#clusterID
// e.g. "192.168.1.1:3306#123456"
func genClusterTag(clusterID int64, clusterAddress string) string {
	if clusterID == -1 {
		return clusterAddress + "#"
	}
	return fmt.Sprintf("%s#%d", clusterAddress, clusterID)
}

func getClusterId(pCfg *PolarDBXConfig) (clusterId int64, isDn bool, err error) {
	// convert string to address slices
	addressStrings := strings.Split(pCfg.Addr, ",")

	// Shuffle the addresses randomly
	rand.Shuffle(len(addressStrings), func(i, j int) {
		addressStrings[i], addressStrings[j] = addressStrings[j], addressStrings[i]
	})

	if len(addressStrings) == 0 {
		return 0, false, fmt.Errorf("Invalid addresses: %s", pCfg.Addr)
	}

	var lastErr error = nil
	// Try each address until we find one that works
	for _, address := range addressStrings {
		db, err := sql.Open("mysql", pCfg.FormatPolarDBXDSN(strings.TrimSpace(address)))
		if err != nil {
			lastErr = err
			checkLogger.ErrorWithStack(err, pCfg.EnableProbeLog)
			continue // Try next address
		}

		func() {
			defer func() {
				if err := db.Close(); err != nil {
					log.Printf("Failed to close database: %v", err)
				}
			}()

			var basicInfo BasicInFoQuery
			err = db.QueryRow(basicInfoQuery).Scan(&basicInfo.Version, &basicInfo.ClusterID, &basicInfo.Port)
			if err != nil {
				lastErr = err
				checkLogger.ErrorWithStack(err, pCfg.EnableProbeLog)
				return // Try next address
			}

			if strings.Contains(strings.ToUpper(basicInfo.Version), "-TDDL-") {
				clusterId, isDn, err = -1, false, nil
				return
			}

			clusterId, isDn, err = basicInfo.ClusterID, true, nil
		}()

		// If we got here without continuing, we found an available node
		if err == nil {
			return clusterId, isDn, nil
		}
	}

	if lastErr != nil {
		checkLogger.ErrorWithStack(lastErr, pCfg.EnableProbeLog)
	}
	return 0, false, lastErr
}

func (n *XClusterNodeBasic) Equals(other *XClusterNodeBasic) bool {
	if other == nil {
		return false
	}
	return n.Tag == other.Tag &&
		n.Connectable == other.Connectable &&
		n.Host == other.Host &&
		n.Port == other.Port &&
		n.PaxosPort == other.PaxosPort &&
		n.Role == other.Role &&
		reflect.DeepEqual(n.Peers, other.Peers) &&
		n.Version == other.Version &&
		n.ClusterID == other.ClusterID
}

func (hm *HaManager) saveDnToFile(nodes []*XClusterNodeBasic, filename string) error {
	data, err := json.MarshalIndent(nodes, "", "  ")
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(filename, data, 0644)
	if err != nil {
		return err
	}
	checkLogger.Info(fmt.Sprintf("Saved nodes to file: %s", filename), hm.pCfg.EnableProbeLog)
	return nil
}

func (hm *HaManager) loadDnFromFile(filename string) ([]*XClusterNodeBasic, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	var nodes []*XClusterNodeBasic
	if err := json.Unmarshal(data, &nodes); err != nil {
		return nil, err
	}
	checkLogger.Info(fmt.Sprintf("Loaded nodes from file: %s", filename), hm.pCfg.EnableProbeLog)
	return nodes, nil
}

func (hm *HaManager) saveMppToFile(mpp []*MppInfo, filename string) error {
	data, err := json.MarshalIndent(mpp, "", "  ")
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(filename, data, 0644)
	if err != nil {
		return err
	}
	checkLogger.Info(fmt.Sprintf("Saved mpp to file: %s", filename), hm.pCfg.EnableProbeLog)
	return nil
}

func (hm *HaManager) loadMppFromFile(filename string) ([]*MppInfo, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	var mpp []*MppInfo
	if err := json.Unmarshal(data, &mpp); err != nil {
		return nil, err
	}
	checkLogger.Info(fmt.Sprintf("Loaded mpp from file: %s", filename), hm.pCfg.EnableProbeLog)
	return mpp, nil
}

type HaManager struct {
	mu      sync.RWMutex
	connReq *chan struct{}
	pCfg    *PolarDBXConfig

	isDn    bool
	useIPv6 bool
	version uint32

	dnClusterInfo *XClusterInfo
	cnClusterInfo []*MppInfo
	connCnt       *sync.Map // map[string]*int64, 使用原子操作
}

var managers sync.Map
var mu sync.Mutex

func (hm *HaManager) getOrCreateConnCounter(key string) *int64 {
	if val, ok := hm.connCnt.Load(key); ok {
		return val.(*int64)
	}

	// 创建新的计数器
	counter := int64(0)
	actual, loaded := hm.connCnt.LoadOrStore(key, &counter)
	if loaded {
		return actual.(*int64)
	}
	return &counter
}

func (hm *HaManager) incrementConnCounter(key string) int64 {
	counter := hm.getOrCreateConnCounter(key)
	return atomic.AddInt64(counter, 1)
}

func (hm *HaManager) decrementConnCounter(key string) int64 {
	counter := hm.getOrCreateConnCounter(key)
	return atomic.AddInt64(counter, -1)
}

const ILLEGAL_CHARS = `[\\/:*?"<>|\n\r\t]`

func sanitizeAddr(addr string) string {
	re := regexp.MustCompile(ILLEGAL_CHARS)
	return re.ReplaceAllString(addr, "-")
}

// GetManager 根据集群信息获取或创建并返回一个HaManager实例。
// 如果集群信息中未指定集群ID，将尝试自动探测。
// 如果指定或自动生成了集群ID，将根据集群信息和系统临时目录路径生成一个JSON文件路径。
// 此函数还负责设置或更新HaManager实例的属性，如用户、密码。
func GetManager(pCfg *PolarDBXConfig) (*HaManager, error) {
	tag := genClusterTag(pCfg.ClusterID, pCfg.Addr)
	var manager *HaManager
	v, _ := managers.Load(tag)
	if v != nil {
		manager = v.(*HaManager)
	}
	clusterId := pCfg.ClusterID

	if manager == nil {
		tmpDir := os.TempDir()
		newClusterId, isDn, err := getClusterId(pCfg)
		if err != nil {
			return nil, err
		}
		if clusterId == -1 {
			clusterId = newClusterId
		}

		useIPv6 := isIPv6(pCfg.Addr)
		var jsonFile string
		if pCfg.JsonFile == "" {
			if isDn {
				if useIPv6 {
					jsonFile = filepath.Join(tmpDir, fmt.Sprintf("XCluster-%d-%s-IPv6.json", clusterId, sanitizeAddr(pCfg.Addr)))
				} else {
					jsonFile = filepath.Join(tmpDir, fmt.Sprintf("XCluster-%d-%s-IPv4.json", clusterId, sanitizeAddr(pCfg.Addr)))
				}
			} else {
				if useIPv6 {
					jsonFile = filepath.Join(tmpDir, fmt.Sprintf("XCluster-%s-IPv6.json", pCfg.Addr))
				} else {
					jsonFile = filepath.Join(tmpDir, fmt.Sprintf("XCluster-%s-IPv4.json", pCfg.Addr))
				}
			}
			pCfg.JsonFile = jsonFile
		} else {
			jsonFile = pCfg.JsonFile
		}

		// if file not exists, create a new file
		file, err := os.Open(jsonFile)
		if err != nil {
			if os.IsNotExist(err) {
				nFile, err := os.Create(jsonFile)
				if err != nil {
					return nil, err
				}
				defer func(nFile *os.File) {
					err := nFile.Close()
					if err != nil {
						log.Printf("Failed to close file: %v", err)
					}
				}(nFile)
			} else {
				return nil, err
			}
		} else {
			defer func(file *os.File) {
				err := file.Close()
				if err != nil {
					log.Printf("Failed to close file: %v", err)
				}
			}(file)
		}

		tag = genClusterTag(clusterId, pCfg.Addr)

		mu.Lock()
		defer mu.Unlock()
		if v, ok := managers.Load(tag); !ok {
			manager = &HaManager{
				pCfg:          pCfg,
				isDn:          isDn,
				useIPv6:       useIPv6,
				dnClusterInfo: &XClusterInfo{GlobalPortGap: -10000},
				connCnt:       &sync.Map{},
			}
			req := make(chan struct{})
			manager.connReq = &req

			mainLogger.Info(fmt.Sprintf("Starting HA checker for cluster %s", tag), manager.pCfg.EnableLog)
			if isDn {
				go manager.DnHaChecker()
			} else {
				go manager.CnHaChecker()
			}
			managers.Store(tag, manager)
			return manager, nil
		} else {
			manager = v.(*HaManager)
		}
	}
	manager.mu.Lock()
	manager.pCfg.User = pCfg.User
	manager.pCfg.Passwd = pCfg.Passwd
	manager.pCfg.HaCheckIntervalMillis = pCfg.HaCheckIntervalMillis
	manager.pCfg.HaCheckSocketTimeoutMillis = pCfg.HaCheckSocketTimeoutMillis
	manager.pCfg.HaCheckConnectTimeoutMillis = pCfg.HaCheckConnectTimeoutMillis
	manager.pCfg.CheckLeaderTransferringIntervalMillis = pCfg.CheckLeaderTransferringIntervalMillis
	manager.pCfg.LeaderTransferringWaitTimeoutMillis = pCfg.LeaderTransferringWaitTimeoutMillis

	if pCfg.JsonFile != "" {
		manager.pCfg.JsonFile = pCfg.JsonFile
	}
	manager.mu.Unlock()

	return manager, nil
}

func (hm *HaManager) getConnectionAddresses() (mapset.Set, error) {
	// get addresses from json file (previous node info) and leaderDsn addresses
	hm.mu.RLock()
	jsonFile := hm.pCfg.JsonFile
	probedAddresses := mapset.NewSet()
	hm.mu.RUnlock()

	var addressesFromJson interface{}
	var err error
	if jsonFile != "" {
		if hm.isDn {
			addressesFromJson, err = hm.loadDnFromFile(jsonFile)
		} else {
			addressesFromJson, err = hm.loadMppFromFile(jsonFile)
		}
	}

	if err != nil {
		return nil, err
	} else {
		if hm.isDn {
			for _, node := range addressesFromJson.([]*XClusterNodeBasic) {
				if strings.EqualFold(node.Role, "Leader") || strings.EqualFold(node.Role, "Follower") {
					probedAddresses.Add(node.Tag)
				}
			}
		} else {
			for _, node := range addressesFromJson.([]*MppInfo) {
				if strings.EqualFold(node.Role, "Leader") || strings.EqualFold(node.Role, "Follower") {
					probedAddresses.Add(node.Tag)
				}
			}
		}
	}

	hm.mu.RLock()
	dsnAddresses := hm.pCfg.Addr
	hm.mu.RUnlock()
	if strings.Contains(dsnAddresses, ",") {
		splitAddress := strings.Split(dsnAddresses, ",")
		for _, addr := range splitAddress {
			probedAddresses.Add(strings.TrimSpace(addr))
		}
	} else {
		probedAddresses.Add(dsnAddresses)
	}

	return probedAddresses, nil
}

func (hm *HaManager) getDnFollower(applyDelayThreshold, slaveWeightThreshold int32, loadBalanceAlgorithm string) (string, error) {
	// get healthy followers
	mysqlDsn, _ := hm.pCfg.FormatMYSQLDSN(hm.dnClusterInfo.LeaderInfo.Tag)
	checkLogger.Debug("get dn follower dsn "+mysqlDsn, hm.pCfg.EnableProbeLog)
	db, err := sql.Open("mysql", mysqlDsn)
	if err != nil {
		return "", errors.WithStack(err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Println("Error closing DB connection:", err)
		}
	}(db)
	querySql := fmt.Sprintf(clusterHealthQuery, applyDelayThreshold, slaveWeightThreshold)
	rows, err := db.Query(querySql)
	if err != nil {
		mainLogger.ErrorWithStack(err, hm.pCfg.EnableLog)
		return "", errors.WithStack(err)
	}
	followers := mapset.NewSet()
	for rows.Next() {
		var role, ipPort string
		err := rows.Scan(&role, &ipPort)
		if err != nil {
			return "", errors.WithStack(err)
		}
		if !strings.EqualFold(role, "Follower") {
			continue
		}
		address, err := decode(ipPort)
		if err != nil {
			return "", errors.WithStack(err)
		}
		address.Port += hm.dnClusterInfo.GlobalPortGap
		followers.Add(NodeWithLoadWeight{Tag: fmt.Sprintf("%s:%d", address.Hostname, address.Port), LoadWeight: defaultLoadWeight})
	}

	// do loadBalance
	hm.mu.Lock()
	defer hm.mu.Unlock()
	return hm.getNodeWithLoadBalance(followers, loadBalanceAlgorithm), nil
}

func (hm *HaManager) getNodeWithLoadBalance(candidates mapset.Set, loadBalanceAlgorithm string) string {
	if candidates.Cardinality() <= 0 {
		return ""
	}
	var selected *NodeWithLoadWeight
	if loadBalanceAlgorithm == random {
		totalWeight := int64(0)

		for _, ni := range candidates.ToSlice() {
			nodeWithWeight := ni.(NodeWithLoadWeight)
			totalWeight += nodeWithWeight.LoadWeight
			mainLogger.Debug(fmt.Sprintf("get node with lb, tag: %s, weight: %d", nodeWithWeight.Tag, nodeWithWeight.LoadWeight), hm.pCfg.EnableLog)
			randomValue := rand.New(rand.NewSource(time.Now().UnixNano())).Float64()
			if randomValue < float64(nodeWithWeight.LoadWeight)/float64(totalWeight) {
				selected = &nodeWithWeight
			}
		}

	} else if loadBalanceAlgorithm == leastConn {
		// Convert candidates to slice and shuffle it
		candidateSlice := candidates.ToSlice()
		rand.Shuffle(len(candidateSlice), func(i, j int) {
			candidateSlice[i], candidateSlice[j] = candidateSlice[j], candidateSlice[i]
		})

		selectedCnt := int64(math.MaxInt64)
		for _, ni := range candidateSlice {
			node := ni.(NodeWithLoadWeight)
			cnt := *hm.getOrCreateConnCounter(node.Tag)
			if selected == nil || cnt*selected.LoadWeight < selectedCnt*node.LoadWeight {
				selected = &node
				selectedCnt = cnt
				mainLogger.Debug(fmt.Sprintf("get node with lb, tag: %s, weight: %d, cnt: %d", node.Tag, node.LoadWeight, cnt), hm.pCfg.EnableLog)
			}
		}
	}
	if selected != nil {
		hm.incrementConnCounter(selected.Tag)
		mainLogger.Debug(fmt.Sprintf("LoadBalance: weighted %s, candidates: %v, selected: %s", loadBalanceAlgorithm, candidates.ToSlice(), selected.Tag), hm.pCfg.EnableLog)
		return selected.Tag
	} else {
		mainLogger.ErrorWithStack(errors.New("Can not get node with load balance"), hm.pCfg.EnableLog)
		return ""
	}
}

func (hm *HaManager) getDnLeader() (string, bool) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	leader := hm.dnClusterInfo.LeaderInfo
	if leader != nil {
		return leader.Tag, true
	}
	return "", false
}

func (hm *HaManager) getDnInfo(address string) (*XClusterNodeBasic, error) {
	// parse hostname and port from address
	parsedAddress, err := decode(address)
	if err != nil {
		return nil, err
	}

	// connect to mysql
	db, err := sql.Open("mysql", hm.pCfg.FormatPolarDBXDSN(address))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Failed to close database: %v", err)
		}
	}()

	// query basic info
	var basicInfo BasicInFoQuery
	err = db.QueryRow(basicInfoQuery).Scan(&basicInfo.Version, &basicInfo.ClusterID, &basicInfo.Port)
	if err != nil {
		return nil, err
	}

	// if no cluster id, update it
	// if mismatch with the cluster, just ignore this node
	if atomic.LoadInt64(&hm.pCfg.ClusterID) == -1 {
		atomic.StoreInt64(&hm.pCfg.ClusterID, basicInfo.ClusterID)
	} else if basicInfo.ClusterID != atomic.LoadInt64(&hm.pCfg.ClusterID) {
		mainLogger.ErrorWithStack(errors.New(fmt.Sprintf("cluster id mismatch: %d != %d", basicInfo.ClusterID, atomic.LoadInt64(&hm.pCfg.ClusterID))), hm.pCfg.EnableLog)
		return nil, ErrClusterMismatch
	}

	// query role and its leader address
	var leader, role string
	err = db.QueryRow(clusterLocalQuery).Scan(&leader, &role)
	if err != nil {
		return nil, err
	}
	updateTime := time.Now().Format(dateFormat)

	// if not a leader, get tag and peers info and return
	if !strings.EqualFold(role, "Leader") {
		checkLogger.Debug(fmt.Sprintf("%s is %s.", address, role), hm.pCfg.EnableProbeLog)
		var peers []*XClusterNodeBasic
		if leader != "" {
			checkLogger.Debug(fmt.Sprintf("%s is leader.", leader), hm.pCfg.EnableProbeLog)
			// get leader info
			paxosAddr, err := decode(leader)
			if err != nil {
				return nil, err
			}
			connectPort := paxosAddr.Port + atomic.LoadInt32(&hm.dnClusterInfo.GlobalPortGap)
			tag := fmt.Sprintf("%s:%d", paxosAddr.Hostname, connectPort)
			peer := &XClusterNodeBasic{
				Tag:         tag,
				Connectable: true,
				Host:        paxosAddr.Hostname,
				Port:        connectPort,
				PaxosPort:   paxosAddr.Port,
				Role:        "Leader",
				Peers:       nil,
				Version:     basicInfo.Version,
				ClusterID:   basicInfo.ClusterID,
				UpdateTime:  updateTime,
			}
			peers = append(peers, peer)
		}
		return &XClusterNodeBasic{
			Tag:         address,
			Connectable: true,
			Host:        parsedAddress.Hostname,
			Port:        parsedAddress.Port,
			PaxosPort:   -1,
			Role:        role,
			Peers:       peers,
			Version:     basicInfo.Version,
			ClusterID:   basicInfo.ClusterID,
			UpdateTime:  updateTime,
		}, nil
	}

	checkLogger.Debug(fmt.Sprintf("%s is leader", address), hm.pCfg.EnableProbeLog)

	// calculate port gap
	paxosAddress, err := decode(leader)
	if err != nil {
		return nil, err
	}
	portGap := basicInfo.Port - paxosAddress.Port
	atomic.StoreInt32(&hm.dnClusterInfo.GlobalPortGap, portGap)

	// get peers of Leader and return
	rows, err := db.Query(clusterGlobalQuery)
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Printf("Failed to close rows: %v", err)
		}
	}(rows)

	var leaderRealIp string
	peers := make([]*XClusterNodeBasic, 0)
	if rows == nil {
		return nil, err
	}
	for rows.Next() {
		var peerRole, ipPort string
		err = rows.Scan(&peerRole, &ipPort)
		if err != nil {
			return nil, err
		}
		peerPaxosAddr, err := decode(ipPort)
		if err != nil {
			return nil, err
		}
		peerHost := peerPaxosAddr.Hostname
		peerPaxosPort := peerPaxosAddr.Port
		// basicInfo.Port 是当前节点的数据库服务端口，
		// connectPort 是其他节点的 Paxos 端口通过加上 portGap 得到实际的服务端口
		connectPort := peerPaxosPort + portGap
		peerIsLeader := strings.EqualFold(peerRole, "Leader")

		peers = append(peers, &XClusterNodeBasic{
			Tag:         fmt.Sprintf("%s:%d", peerHost, connectPort),
			Connectable: true,
			Host:        peerHost,
			Port:        connectPort,
			PaxosPort:   peerPaxosPort,
			Role:        peerRole,
			Peers:       nil,
			Version:     basicInfo.Version,
			ClusterID:   basicInfo.ClusterID,
			UpdateTime:  updateTime,
		})

		// in case the input address is a VIP
		if peerIsLeader {
			leaderRealIp = peerHost
		}
	}

	sort.Slice(peers, func(i, j int) bool {
		return peers[i].Tag < peers[j].Tag
	})

	host := parsedAddress.Hostname
	if leaderRealIp != "" {
		host = leaderRealIp
	}

	return &XClusterNodeBasic{
		Tag:         address,
		Connectable: true,
		Host:        host,
		Port:        basicInfo.Port,
		PaxosPort:   paxosAddress.Port,
		Role:        role,
		Peers:       peers,
		Version:     basicInfo.Version,
		ClusterID:   basicInfo.ClusterID,
		UpdateTime:  updateTime,
	}, nil
}

func (hm *HaManager) getMppInfo(address string) ([]*MppInfo, error) {
	// connect to mysql
	db, err := sql.Open("mysql", hm.pCfg.FormatPolarDBXDSN(address))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Failed to close database: %v", err)
		}
	}()

	// query mpp info
	var mppInfos []*MppInfo
	rows, err := db.Query(showMppQuery)
	if err != nil {
		return nil, err
	}

	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Printf("Failed to close rows: %v", err)
		}
	}(rows)

	// get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for i := range valuePtrs {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		var mppInfo MppInfo
		var zoneNameString string
		mppInfo.LoadWeight = defaultLoadWeight

		err = rows.Scan(valuePtrs...)
		if err != nil {
			return nil, err
		}

		for i, col := range columns {
			switch strings.ToLower(col) {
			case "id":
				mppInfo.InstanceName = string(values[i].([]byte))
			case "node":
				mppInfo.Tag = string(values[i].([]byte))
			case "role":
				mppInfo.Role = string(values[i].([]byte))
			case "leader":
				mppInfo.IsLeader = string(values[i].([]byte))
			case "sub_cluster":
				zoneNameString = string(values[i].([]byte))
			case "load_weight":
				loadWeight, err := strconv.Atoi(string(values[i].([]byte)))
				if err != nil {
					loadWeight = defaultLoadWeight
					mainLogger.Debug(fmt.Sprint("Get load_weight not ok", values[i]), hm.pCfg.EnableLog)
				}

				if loadWeight < 0 {
					loadWeight = 0
				}

				if loadWeight > defaultLoadWeight {
					loadWeight = defaultLoadWeight
				}

				mppInfo.LoadWeight = int64(loadWeight)
			}
		}

		mppInfo.ZoneList = getZoneList(zoneNameString)
		mppInfos = append(mppInfos, &mppInfo)
	}

	return mppInfos, nil
}

func mayBecomeOrKnowLeader(role string) bool {
	return strings.EqualFold(role, "Leader") || strings.EqualFold(role, "Follower") ||
		strings.EqualFold(role, "Candidate") || strings.EqualFold(role, "Learner")
}

func (hm *HaManager) probeAndUpdateLeader() *XClusterNodeBasic {
	probedAddresses, err := hm.getConnectionAddresses()
	if err != nil {
		checkLogger.ErrorWithStack(err, hm.pCfg.EnableProbeLog)
		return nil
	}
	nodes := make(map[string]*XClusterNodeBasic, probedAddresses.Cardinality())

	checkLogger.Debug(fmt.Sprintf("Probing nodes: %v", probedAddresses.ToSlice()), hm.pCfg.EnableLog)

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(probedAddresses.Cardinality())
	for _, address := range probedAddresses.ToSlice() {
		go func() {
			defer wg.Done()
			info, err := hm.getDnInfo(address.(string))
			if err != nil {
				checkLogger.ErrorWithStack(err, hm.pCfg.EnableProbeLog)
			}
			if info != nil {
				mu.Lock()
				nodes[info.Tag] = info
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// try fetch leader with real ip first (IPv4 or IPv6)
	var leader *XClusterNodeBasic
	for _, node := range nodes {
		if strings.EqualFold(node.Role, "Leader") && node.Tag == fmt.Sprintf("%s:%d", node.Host, node.Port) {
			if leader == nil {
				leader = node
			} else {
				mainLogger.ErrorWithStack(errors.New(fmt.Sprintf("More than one leader found: %s, %s", leader, node)), hm.pCfg.EnableLog)
				return nil
			}
		}
	}

	// try fetch VIP leader if no real leader found
	if leader == nil && !hm.pCfg.IgnoreVip {
		for _, node := range nodes {
			if strings.EqualFold(node.Role, "Leader") {
				leader = node
				break
			}
		}
	}

	// if leader found, add leader's peers to nodes
	// if leader not found, try to check whether the leader is in
	// the peers of nodes
	if leader != nil {
		peers := leader.Peers
		for _, x := range peers {
			if mayBecomeOrKnowLeader(x.Role) {
				nodes[x.Tag] = x
			}
		}
	} else {
		// no leader found
		tmpNodes := make([]*XClusterNodeBasic, 0, len(nodes))
		for _, node := range nodes {
			tmpNodes = append(tmpNodes, node)
		}
		for _, node := range tmpNodes {
			for _, x := range node.Peers {
				if strings.EqualFold(x.Role, "Leader") {
					nodes[x.Tag] = x
					if !hm.pCfg.IgnoreVip || x.Tag == fmt.Sprintf("%s:%d", x.Host, x.Port) {
						leader = x
					}
				}
			}
		}

		if leader == nil {
			return nil
		}
	}

	// update cluster peers
	var nodeList []*XClusterNodeBasic
	for _, node := range nodes {
		nodeList = append(nodeList, node)
		// update cnt map - ensure counter exists
		hm.getOrCreateConnCounter(node.Tag)
	}
	sort.Slice(nodeList, func(i, j int) bool {
		return nodeList[i].Tag < nodeList[j].Tag
	})

	hm.mu.RLock()
	jsonFile := hm.pCfg.JsonFile
	hm.mu.RUnlock()
	err = hm.saveDnToFile(nodeList, jsonFile)
	if err != nil {
		checkLogger.ErrorWithStack(err, hm.pCfg.EnableProbeLog)
	}

	versionStr := leader.Version
	hm.version = versionString2Int32(versionStr)

	// update leader info
	oldDB := hm.dnClusterInfo.LongConnection
	if oldDB != nil {
		err := oldDB.Close()
		if err != nil {
			checkLogger.ErrorWithStack(err, hm.pCfg.EnableProbeLog)
		}
		hm.dnClusterInfo.LongConnection = nil
	}
	newDB, err := sql.Open("mysql", hm.pCfg.FormatPolarDBXDSN(leader.Tag))
	if err != nil {
		checkLogger.ErrorWithStack(err, hm.pCfg.EnableProbeLog)
		return nil
	}

	var variableName, value string
	err = newDB.QueryRow(checkLeaderTransferQuery).Scan(&variableName, &value)
	if err != nil {
		checkLogger.ErrorWithStack(err, hm.pCfg.EnableProbeLog)
		return nil
	}
	isTransferring, _ := readBool(value)

	if isTransferring {
		checkLogger.Debug(fmt.Sprintf("The cluster is under leader transfer, current leader: %s.", leader.Tag), hm.pCfg.EnableProbeLog)
		hm.mu.Lock()
		hm.dnClusterInfo.LeaderInfo = nil
		hm.dnClusterInfo.LongConnection = nil
		hm.dnClusterInfo.LeaderTransferInfo = &LeaderTransferInfo{nanos: time.Now().UnixNano(), tag: leader.Tag}
		hm.mu.Unlock()
		return nil
	}

	conn, err := newDB.Conn(context.Background())
	if err != nil {
		checkLogger.ErrorWithStack(err, hm.pCfg.EnableProbeLog)
	}

	checkLogger.Debug(fmt.Sprintf("Updating leader to %s.", leader.Tag), hm.pCfg.EnableProbeLog)
	hm.mu.Lock()
	hm.dnClusterInfo.LeaderInfo = leader
	hm.dnClusterInfo.LongConnection = conn
	hm.dnClusterInfo.LeaderTransferInfo = nil
	close(*hm.connReq)
	newReq := make(chan struct{})
	hm.connReq = &newReq
	hm.mu.Unlock()

	return leader
}

func (hm *HaManager) getAvailableDnWithWait(timeout int32, slaveOnly bool,
	applyDelayThreshold, slaveWeightThreshold int32, loadBalanceAlgorithm string) (string, error) {
	_, isFirstTime := hm.getDnLeader()
	if isFirstTime {
		timeout = maxInt32(timeout, (hm.pCfg.HaCheckIntervalMillis+hm.pCfg.HaCheckSocketTimeoutMillis)*3)
	}
	mainLogger.Debug(fmt.Sprintf("Getting node with timeout: %d ms.", timeout), hm.pCfg.EnableLog)
	timeoutNanos := time.Now().UnixNano() + int64(timeout)*1000000
	for {
		nowNanos := time.Now().UnixNano()
		// time out
		if nowNanos >= timeoutNanos {
			// last try
			if leader, ok := hm.getDnLeader(); ok {
				if !slaveOnly {
					return leader, nil
				} else if follower, err := hm.getDnFollower(applyDelayThreshold, slaveWeightThreshold, loadBalanceAlgorithm); follower != "" {
					return follower, nil
				} else if err != nil {
					mainLogger.ErrorWithStack(err, hm.pCfg.EnableLog)
				}
			}
			if slaveOnly {
				mainLogger.ErrorWithStack(errors.New("No follower found in slave read mode."), hm.pCfg.EnableLog)
			}
			return "", ErrNoNodeFound
		}

		// still have time to try
		if leader, ok := hm.getDnLeader(); ok {
			if !slaveOnly {
				return leader, nil
			} else if follower, err := hm.getDnFollower(applyDelayThreshold, slaveWeightThreshold, loadBalanceAlgorithm); follower != "" {
				return follower, nil
			} else if err != nil {
				mainLogger.ErrorWithStack(err, hm.pCfg.EnableLog)
			}
		}

		// no leader found, wait until timeout or leader updated
		nowNanos = time.Now().UnixNano()
		sleepMillis := (timeoutNanos - nowNanos) / 1000000

		hm.mu.RLock()
		if hm.dnClusterInfo.LeaderInfo != nil {
			hm.mu.RUnlock()
			time.Sleep(time.Duration(hm.pCfg.HaCheckIntervalMillis) * time.Millisecond)
			continue
		}
		localChan := *hm.connReq
		hm.mu.RUnlock()

		select {
		case <-time.After(maxDuration(0, time.Duration(sleepMillis)*time.Millisecond)):
			checkLogger.Debug(fmt.Sprintf("[%d]Waiting leader time out, sleep milli: %d, retry getting leader.", goid.Get(), sleepMillis), hm.pCfg.EnableProbeLog)
		case <-localChan:
			checkLogger.Debug("Leader has been updated, retry getting leader.", hm.pCfg.EnableProbeLog)
		}

	}
}

func (hm *HaManager) getAvailableCnWithWait(timeout int32, zoneName string, minZoneNodes int32,
	backupZoneName string, slaveRead bool, instanceName string, mppRole string, loadBalanceAlgorithm string,
	cnGroup string, backupCnGroup string) (string, error) {
	mainLogger.Debug(fmt.Sprintf("get cn with timeout %d, zoneName %s, minZoneNodes %d, backupZoneName %s, slaveRead %t, instanceName %s, mppRole %s, loadBalanceAlgorithm %s, cnGroup %s, backupCnGroup %s", timeout, zoneName, minZoneNodes, backupZoneName, slaveRead, instanceName, mppRole, loadBalanceAlgorithm, cnGroup, backupCnGroup), hm.pCfg.EnableLog)
	hm.mu.Lock()
	isFirstTime := hm.getValidCn(zoneName, minZoneNodes, backupZoneName,
		slaveRead, instanceName, mppRole, loadBalanceAlgorithm, cnGroup, backupCnGroup) != ""
	hm.mu.Unlock()
	if isFirstTime {
		timeout = maxInt32(timeout, (hm.pCfg.HaCheckIntervalMillis+hm.pCfg.HaCheckSocketTimeoutMillis)*3)
	}
	timeoutNanos := time.Now().UnixNano() + int64(timeout)*1000000
	for {
		nowNanos := time.Now().UnixNano()
		// time out
		if nowNanos >= timeoutNanos {
			// last try
			hm.mu.Lock()
			if cn := hm.getValidCn(zoneName, minZoneNodes, backupZoneName,
				slaveRead, instanceName, mppRole, loadBalanceAlgorithm, cnGroup, backupCnGroup); cn != "" {
				hm.mu.Unlock()
				return cn, nil
			} else {
				hm.mu.Unlock()
				return "", ErrNoNodeFound
			}
		}

		// still have time to try
		hm.mu.Lock()
		if cn := hm.getValidCn(zoneName, minZoneNodes, backupZoneName,
			slaveRead, instanceName, mppRole, loadBalanceAlgorithm, cnGroup, backupCnGroup); cn != "" {
			hm.mu.Unlock()
			return cn, nil
		}
		localChan := *hm.connReq
		hm.mu.Unlock()

		// no cn found, wait until timeout or cluster updated
		nowNanos = time.Now().UnixNano()
		sleepMillis := (timeoutNanos - nowNanos) / 1000000

		select {
		case <-time.After(maxDuration(0, time.Duration(sleepMillis)*time.Millisecond)):
			mainLogger.Debug(fmt.Sprintf("Waiting cn time out: %d ms, retry getting cn.", timeout), hm.pCfg.EnableLog)
		case <-localChan:
			mainLogger.Debug("Cluster has been updated, retry getting cn.", hm.pCfg.EnableLog)
		}

	}
}

func (hm *HaManager) genZoneNameFromCnGroup(cnGroup string) string {
	// Parse cnGroup to get a set of hosts
	cnGroupSet, err := decodeCnGroup(cnGroup)
	if err != nil || cnGroupSet == nil || len(cnGroupSet) == 0 {
		return ""
	}

	// Create a set for zones (using map[string]bool as a set)
	zoneSet := make(map[string]bool)

	// Iterate through MPPs (which is hm.cnClusterInfo in Go)
	for _, mppInfo := range hm.cnClusterInfo {
		// Extract host from the Tag field
		host := decodeHostFromMPPInfo(mppInfo.Tag)

		// Check if the host is in cnGroupSet
		if cnGroupSet[host] {
			// Add zones from ZoneList (equivalent to subCluster in Java)
			for _, zone := range mppInfo.ZoneList {
				if zone != "" {
					zoneSet[zone] = true
				}
			}
		}
	}

	// If we have zones, join them into a comma-separated string
	if len(zoneSet) > 0 {
		zones := make([]string, 0, len(zoneSet))
		for zone := range zoneSet {
			zones = append(zones, zone)
		}
		return strings.Join(zones, ",")
	}

	return ""
}

func decodeZoneFromMppInfo(mppInfo string) []string {
	if mppInfo != "" {
		return strings.Split(strings.TrimSpace(mppInfo), ",")
	}
	return []string{}
}

func decodeHostFromMPPInfo(tag string) string {
	if tag != "" {
		parts := strings.Split(tag, ":")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
	}
	return ""
}

func (hm *HaManager) getValidCn(zoneName string, minZoneNodes int32,
	backupZoneName string, slaveRead bool, instanceName string, mppRole string, loadBalanceAlgorithm string,
	cnGroup string, backupCnGroup string) string {
	mainLogger.Debug(fmt.Sprintf("try to get valid cn: %s, minZoneNodes: %d, instanceName: %s", zoneName, minZoneNodes, instanceName), hm.pCfg.EnableLog)

	if zoneName == "" && backupZoneName == "" {
		zoneName = hm.genZoneNameFromCnGroup(cnGroup)
		backupZoneName = hm.genZoneNameFromCnGroup(backupCnGroup)
	}

	zoneSet := getZoneSet(zoneName)
	backupZoneSet := getZoneSet(backupZoneName)
	validCn := mapset.NewSet()
	backupCn := mapset.NewSet()

	for _, cn := range hm.cnClusterInfo {
		mainLogger.Debug(fmt.Sprintf("candidate cn: %v, zoneSet: %v, zonesetSize: %d", cn.Tag, zoneSet, zoneSet.Cardinality()), hm.pCfg.EnableLog)
		if (instanceName == "" || instanceName == cn.InstanceName) &&
			((slaveRead && !strings.EqualFold(mppRole, w) && !strings.EqualFold(cn.Role, w)) ||
				(!slaveRead && (strings.EqualFold(mppRole, w) || mppRole == "") && strings.EqualFold(cn.Role, w))) {
			mainLogger.Debug(fmt.Sprintf("valid cn: %v, zoneSet: %v, zonesetSize: %d", cn, zoneSet, zoneSet.Cardinality()), hm.pCfg.EnableLog)
			if zoneSet.Cardinality() == 0 || isOverlapped(zoneSet, cn.ZoneList) {
				// Store both tag and load weight as a key-value pair
				validCn.Add(NodeWithLoadWeight{Tag: cn.Tag, LoadWeight: cn.LoadWeight})
			}
			if isOverlapped(backupZoneSet, cn.ZoneList) {
				// Store both tag and load weight as a key-value pair
				backupCn.Add(NodeWithLoadWeight{Tag: cn.Tag, LoadWeight: cn.LoadWeight})
			}
		}
	}

	if validCn.Cardinality() >= int(minZoneNodes) {
		return hm.getNodeWithLoadBalance(validCn, loadBalanceAlgorithm)
	} else if backupCn.Cardinality() > 0 {
		return hm.getNodeWithLoadBalance(backupCn, loadBalanceAlgorithm)
	} else {
		return ""
	}
}

func isOverlapped(targetSet mapset.Set, candidateSet []string) bool {
	for _, zoneName := range candidateSet {
		if targetSet.Contains(zoneName) {
			return true
		}
	}
	return false
}

func getZoneSet(zoneName string) mapset.Set {
	zoneSet := mapset.NewSet()
	if zoneName == "" {
		return zoneSet
	}
	aZs := strings.Split(zoneName, ",")
	for _, aZ := range aZs {
		zoneSet.Add(strings.TrimSpace(aZ))
	}
	return zoneSet
}

func getZoneList(zoneName string) []string {
	var zoneList []string
	if zoneName == "" {
		return zoneList
	}
	aZs := strings.Split(zoneName, ",")
	for _, aZ := range aZs {
		zoneList = append(zoneList, strings.TrimSpace(aZ))
	}
	return zoneList
}

// decodeCnGroup parses a comma-separated list of "ip:port" strings and returns a set of IPs.
// Input: ip0:port0,ip1:port1,ip2:port2. Return: {ip0, ip1, ip2}
func decodeCnGroup(cnGroup string) (map[string]bool, error) {
	if cnGroup == "" {
		return nil, nil
	}

	cnGroupSet := make(map[string]bool)
	entries := strings.Split(cnGroup, ",")

	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		parts := strings.Split(entry, ":")
		if len(parts) < 1 {
			return nil, fmt.Errorf("invalid format for entry: %s", entry)
		}

		ip := strings.TrimSpace(parts[0])
		if ip == "" {
			return nil, fmt.Errorf("empty IP in entry: %s", entry)
		}

		cnGroupSet[ip] = true
	}

	return cnGroupSet, nil
}

func (hm *HaManager) DnHaChecker() {
	for {
		// clear outdated leader transfer info
		hm.mu.Lock()
		transferInfo := hm.dnClusterInfo.LeaderTransferInfo
		if transferInfo != nil {
			now := time.Now().UnixNano()
			timeoutNanos := int64(hm.pCfg.LeaderTransferringWaitTimeoutMillis) * 1e6 // ms to ns
			if now-transferInfo.nanos > timeoutNanos {
				hm.dnClusterInfo.LeaderTransferInfo = nil
			}
		}
		hm.mu.Unlock()

		var clusterState int32
		// if leader found and its connection in the pool, check whether it is still a leader
		hm.mu.RLock()
		leader := hm.dnClusterInfo.LeaderInfo
		conn := hm.dnClusterInfo.LongConnection
		hm.mu.RUnlock()
		if leader != nil && conn != nil {
			checkLogger.Debug("Starting ping leader.", hm.pCfg.EnableProbeLog)
			clusterState = hm.pingLeader(leader, conn)
		} else {
			checkLogger.Debug("Starting fully check.", hm.pCfg.EnableProbeLog)
			startTime := time.Now().UnixNano()
			clusterState = hm.fullyCheck()
			endTime := time.Now().UnixNano()
			checkLogger.Debug(fmt.Sprintf("Fully check time is %d ms.", (endTime-startTime)/1e6), hm.pCfg.EnableProbeLog)
		}

		checkLogger.Debug(fmt.Sprintf("Cluster state is %d.", clusterState), hm.pCfg.EnableProbeLog)
		// sleep for varying intervals for different cases
		var interval int
		if clusterState == leaderAlive {
			// leader is alive, ping it every [1, 100] ms
			interval = max(1, min(100, int(hm.pCfg.HaCheckIntervalMillis)))
		} else if clusterState == leaderLost {
			// lost connection with leader, retry every (~, 3000] ms
			interval = max(1, min(3000, int(hm.pCfg.HaCheckIntervalMillis)))
		} else if clusterState == leaderTransferring {
			// leader is transferring, retry every (~, transfer_time_out] ms
			interval = max(10, int(hm.pCfg.CheckLeaderTransferringIntervalMillis))
		} else if clusterState == leaderTransferred {
			// leader has transferred, retry now
			interval = 0
		}
		time.Sleep(time.Duration(interval) * time.Millisecond)
		checkLogger.Debug("New round for HA checking.", hm.pCfg.EnableProbeLog)
	}
}

func (hm *HaManager) pingLeader(leader *XClusterNodeBasic, conn *sql.Conn) int32 {
	err := conn.PingContext(context.Background())
	if err != nil {
		if strings.Contains(err.Error(), "NOT_IN_LEADER_TRANSFER") {
			hm.mu.Lock()
			hm.dnClusterInfo.LeaderInfo = nil
			hm.dnClusterInfo.LeaderTransferInfo = &LeaderTransferInfo{nanos: time.Now().UnixNano(), tag: leader.Tag}
			hm.mu.Unlock()
			return leaderTransferring
		} else {
			hm.mu.Lock()
			hm.dnClusterInfo.LeaderInfo = nil
			hm.mu.Unlock()
			return leaderTransferred
		}
	}
	return leaderAlive
}

func (hm *HaManager) fullyCheck() int32 {
	// do full HA check
	leader := hm.probeAndUpdateLeader()
	hm.mu.RLock()
	leaderTransferInfo := hm.dnClusterInfo.LeaderTransferInfo
	conn := hm.dnClusterInfo.LongConnection
	hm.mu.RUnlock()
	if leader != nil && conn != nil {
		_, err := conn.ExecContext(context.Background(), setPingMode)
		if err != nil {
			checkLogger.ErrorWithStack(errors.WithStack(errors.New(fmt.Sprintf("set ping mode failed: %s", err.Error()))), hm.pCfg.EnableProbeLog)
		}
		return leaderAlive
	}
	if leaderTransferInfo != nil {
		return leaderTransferring
	}
	return leaderLost
}

func (hm *HaManager) CnHaChecker() {
	for {
		cnMap := make(map[string]*MppInfo)
		probedAddresses, err := hm.getConnectionAddresses()
		if err != nil {
			checkLogger.ErrorWithStack(err, hm.pCfg.EnableProbeLog)
		}
		for _, ai := range probedAddresses.ToSlice() {
			addr := ai.(string)
			mppInfo, err := hm.getMppInfo(addr)
			if err != nil {
				checkLogger.ErrorWithStack(err, hm.pCfg.EnableProbeLog)
				continue
			}
			// add mpp info to cnMap
			for _, mpp := range mppInfo {
				cnMap[mpp.Tag] = mpp
			}
			// add VIP to cnMap
			// if the address is not a VIP, it should have been added in cnMap
			if _, ok := cnMap[addr]; !ok && !hm.pCfg.IgnoreVip {
				cnMap[addr] = &MppInfo{Tag: addr}
			}
		}

		// map to slice
		var cnCluster []*MppInfo
		for _, mppInfo := range cnMap {
			cnCluster = append(cnCluster, mppInfo)
		}

		// save to json file
		checkLogger.Debug(fmt.Sprintf("CnCluster is %v.", cnCluster), hm.pCfg.EnableProbeLog)
		err = hm.saveMppToFile(cnCluster, hm.pCfg.JsonFile)
		if err != nil {
			checkLogger.ErrorWithStack(err, hm.pCfg.EnableProbeLog)
		}

		var clusterState int32
		if len(cnCluster) > 0 {
			// update MPP
			checkLogger.Info(fmt.Sprintf("CnCluster size is %d.", len(cnCluster)), hm.pCfg.EnableProbeLog)
			clusterState = cnAlive
			hm.mu.Lock()
			hm.cnClusterInfo = cnCluster
			close(*hm.connReq)
			newReq := make(chan struct{})
			hm.connReq = &newReq
			hm.mu.Unlock()
		} else {
			clusterState = cnLost
		}

		// sleep for varying intervals for different cases
		if clusterState == cnAlive {
			time.Sleep(time.Duration(hm.pCfg.HaCheckIntervalMillis) * time.Millisecond)
		} else if clusterState == cnLost {
			time.Sleep(time.Duration(minInt32(500, hm.pCfg.HaCheckIntervalMillis)) * time.Millisecond)
		}

	}
}

// min returns the minimum of two int values
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// max returns the maximum of two int values
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// minInt32 returns the minimum of two int32 values
func minInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

// maxInt32 returns the maximum of two int32 values
func maxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

// minDuration returns the minimum of two time.Duration values
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// maxDuration returns the maximum of two time.Duration values
func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
