# polardbx-connector-go
This is a high-availability Golang driver for PolarDB-X. The features implemented so far are as follows:

- [x] Automatically reconnects to the new leader node after switching
- [x] Supports read-write separation
- [ ] Supports CoreDNS
- [x] Built-in `polardbx` driver keyword
- [ ] Supports transparent switching
- [x] Supports `COM_PING` for HA checks
- [x] Supports Load Balancing (random or leastConn)

A simple example using `polardbx` as `drivername`:
```go
import (
	_ "github.com/polardb/polardbx-connector-go"
)
db, err := sql.Open("polardbx", "user:password@tcp(ip:port)/dbname")
if err != nil {
    panic(err)
}

db.SetConnMaxLifetime(time.Minute * 3)
db.SetMaxOpenConns(10)
db.SetMaxIdleConns(10)
```

## Installation
```zsh
go get github.com/polardb/polardbx-connector-go
```

## New Parameters
Standard Edition (Dn):

| Parameter Name                          | Type   | Description                                                                              |
|-----------------------------------------|--------|------------------------------------------------------------------------------------------|
| `clusterId`                             | int64  | Cluster identifier. Optional. Automatically obtained on first connection.                |
| `connectTimeout`                        | int32  | Timeout for acquiring an available DN node, default 3000ms.                              |
| `haCheckConnectTimeoutMillis`           | int32  | HA connection check timeout, default 3000ms.                                             |
| `haCheckSocketTimeoutMillis`            | int32  | HA query check timeout, default 3000ms.                                                  |
| `haCheckIntervalMillis`                 | int32  | HA check interval, default 5000ms.                                                       |
| `checkLeaderTransferringIntervalMillis` | int32  | Leader transfer detection interval, default 100ms.                                       |
| `leaderTransferringWaitTimeoutMillis`   | int32  | Maximum wait time for leader transfer, default 5000ms.                                   |
| `ignoreVip`                             | bool   | Whether to ignore VIP address, default true.                                             |
| `recordJdbcUrl`                         | bool   | Whether to record DSN, default false.                                                    |
| `slaveRead`                             | bool   | Whether to use follower read, default false.                                             |
| `slaveWeightThreshold`                  | int32  | Follower ELECTION_WEIGHT must be greater than this value to connect, default 1.          |
| `applyDelayThreshold`                   | int32  | Follower APPLY_DELAY_SECONDS must be <= this value to connect, unit: seconds, default 3. |
| `directMode`                            | bool   | Whether to skip HA management, default false.                                            |
| `loadBalanceAlgorithm`                  | string | Load balancing algorithm: `random` or `least_connection`.                                |
| `enableLog`                             | bool   | Whether to print driver logs, default true.                                              |

example:
```go
db, err := sql.Open("polardbx", "user:password@tcp(ip:port)/dbname?slaveRead=false&slaveWeightThreshold=1&applyDelayThreshold=3)
```

Enterprise Edition (CN):

| Parameter Name               | Type    | Description                                                                                                     |
|------------------------------|---------|-----------------------------------------------------------------------------------------------------------------|
| `connectTimeout`             | int32   | Timeout for acquiring an available CN node.                                                                     |
| `haCheckIntervalMillis`      | int32   | HA check interval, default 5000ms.                                                                              |
| `zoneName`                   | string  | Zone name (AZ name), default empty (random selection). Example: AZ0, AZ1.                                       |
| `minZoneNodes`               | int32   | Minimum number of nodes in the zone (greater than), default 0.                                                  |
| `backupZoneName`             | string  | Backup zone name used when `minZoneNodes` is not satisfied.                                                     |
| `slaveRead`                  | bool    | Whether to use R instance, default false.                                                                       |
| `instanceName`               | string  | CN instance name, default empty.                                                                                |
| `mppRole`                    | string  | Default W for primary, R for secondary. Can also be set to CR.                                                  |
| `enableFollowerRead`         | int32   | 0: Use primary; 1: Use secondary for normal read; 2: Secondary for consistent read; -1: No operation (default). |
| `loadBalanceAlgorithm`       | string  | Load balancing algorithm: `random` or `least_connection`.                                                       |
| `enableLog`                  | bool    | Whether to print driver logs, default false.                                                                    |

example:
```go
db, err := sql.Open("polardbx", "user:password@tcp(ip:port)/dbname?slaveRead=true&loadBalanceAlgorithm=random")
```

## Development
### Download the Repository
```zsh
git clone http://github.com/polardb/polardbx-connector-go.git
```

### Install Dependencies
```zsh
cd polardbx-connector-go
go mod tidy
```

### Build and Test
```zsh
go test -v -args -dnAddr="ip1:port1, ip2:port2" -dnUser="user1" -dnPasswd="passwd1" -cnAddr="ip3:port3, ip4:port4" -cnUser="user2" -cnPasswd="passwd2" -zoneName="name1" -backupZoneName="name2" -instanceName="name3"
```




