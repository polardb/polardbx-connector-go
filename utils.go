package polardbx

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Returns the bool value of the input.
// The 2nd return value indicates if the input was a valid bool value
func readBool(input string) (value bool, valid bool) {
	switch input {
	case "1", "true", "TRUE", "True":
		return true, true
	case "0", "false", "FALSE", "False":
		return false, true
	}

	// Not a valid bool value
	return
}

func readInt32(input string) (value int32, valid bool) {
	var parsed int64
	parsed, err := strconv.ParseInt(input, 10, 32)
	if err == nil {
		value = int32(parsed)
		valid = true
	}
	return
}

func readInt64(input string) (value int64, valid bool) {
	value, err := strconv.ParseInt(input, 10, 64)
	if err == nil {
		valid = true
	}
	return
}

func isIPv6(address string) bool {
	if strings.Contains(address, ",") {
		splitAddress := strings.Split(address, ",")
		for _, addr := range splitAddress {
			if isIPv6(strings.TrimSpace(addr)) {
				return true
			}
		}
		return false
	} else {
		ip := net.ParseIP(address)
		return ip != nil && ip.To16() != nil && ip.To4() == nil
	}
}

// 解析端口号字符串为整数
// 功能：
// 1. 将输入的字符串转换为整数端口号
// 2. 如果转换失败，返回错误信息"invalid port number"
//
// 参数：
// - portStr: 表示端口号的字符串
//
// 返回值：
// - int: 解析成功的端口号（范围1-65535）
// - error: 错误信息（当无法解析或端口超出有效范围时）
//
// 示例：
// parsePort("3307") -> 3307, nil
// parsePort("abc") -> 0, "invalid port number"
func parsePort(portStr string) (port int32, err error) {
	_, err = fmt.Sscanf(portStr, "%d", &port)
	if err != nil {
		err = ErrInvalidPort
	}
	return
}

// versionString2Int32 解析Leader节点的版本号字符串，提取有效数字部分并转换为整数形式
// 将主版本号、次版本号和修订号分别乘以相应的权重（10000, 100, 1）
// 合并成一个uint32类型的数值，用于后续逻辑判断或处理
// e.g. 8.0.32-X-Cluster-8.4.20-20250221 -> 803200000
func versionString2Int32(versionStr string) uint32 {
	if versionStr != "" {
		limit := 0
		for limit < len(versionStr) {
			ch := versionStr[limit]
			if (ch < '0' || ch > '9') && ch != '.' {
				break
			}
			limit++
		}
		numOnly := versionStr[:limit]
		split := strings.Split(numOnly, ".")
		if len(split) >= 3 {
			v1, _ := strconv.Atoi(split[0])
			v2, _ := strconv.Atoi(split[1])
			v3, _ := strconv.Atoi(split[2])
			return uint32(10000*v1 + 100*v2 + v3)
		}
	}
	return 0
}

func conditionWithTimeout(cond *sync.Cond, timeout time.Duration) bool {
	ch := make(chan bool, 1)

	go func() {
		cond.L.Lock()
		// memory leak
		cond.Wait()
		cond.L.Unlock()
		close(ch)
	}()

	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}
