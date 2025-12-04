package polardbx

import (
	"database/sql"
	"fmt"
)

type BasicInFoQuery struct {
	Version   string
	ClusterID int64
	Port      int32
}

type LeaderTransferInfo struct {
	tag   string // ip:port
	nanos int64  // timestamp
}

type Address struct {
	Hostname string
	Port     int32
}

type XClusterNodeBasic struct {
	Tag         string               `json:"tag"`
	Connectable bool                 `json:"connectable"`
	Host        string               `json:"host"`
	Port        int32                `json:"port"`
	PaxosPort   int32                `json:"paxos_port"`
	Role        string               `json:"role"`
	Peers       []*XClusterNodeBasic `json:"peers"`
	Version     string               `json:"version"`
	ClusterID   int64                `json:"cluster_id"`
	UpdateTime  string               `json:"update_time"`
}

type XClusterInfo struct {
	LeaderInfo         *XClusterNodeBasic
	LeaderTransferInfo *LeaderTransferInfo
	GlobalPortGap      int32
	LongConnection     *sql.Conn
}

type MppInfo struct {
	Tag          string   `json:"tag"` // NODE(ip:port)
	Role         string   `json:"role"`
	InstanceName string   `json:"instance_name"` // ID
	ZoneList     []string `json:"zone_list"`     // sub_cluster
	IsLeader     string   `json:"is_leader"`
	LoadWeight   int64    `json:"load_weight"`
}

type NodeWithLoadWeight struct {
	Tag        string
	LoadWeight int64
}

// String returns a string representation of XClusterNodeBasic
func (x *XClusterNodeBasic) String() string {
	if x == nil {
		return "<nil>"
	}

	// Handle peers slice formatting
	peersStr := "[]"
	if len(x.Peers) > 0 {
		peersStr = fmt.Sprintf("[")
		for i, peer := range x.Peers {
			if i > 0 {
				peersStr += ", "
			}
			peersStr += peer.String()
		}
		peersStr += "]"
	}

	return fmt.Sprintf("XClusterNodeBasic{Tag:%s Connectable:%t Host:%s Port:%d PaxosPort:%d Role:%s Peers:%s Version:%s ClusterID:%d UpdateTime:%s}",
		x.Tag, x.Connectable, x.Host, x.Port, x.PaxosPort, x.Role, peersStr, x.Version, x.ClusterID, x.UpdateTime)
}
