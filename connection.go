package polardbx

import (
	"context"
	"database/sql/driver"
)

type polardbxConn struct {
	mysqlConn driver.Conn
	connId    string
	hm        *HaManager
}

func (c *polardbxConn) Prepare(query string) (driver.Stmt, error) {
	return c.mysqlConn.Prepare(query)
}

func (c *polardbxConn) Close() error {
	err := c.mysqlConn.Close()
	return err
}

func (c *polardbxConn) Begin() (driver.Tx, error) {
	return c.mysqlConn.Begin()
}

func (c *polardbxConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.mysqlConn.(driver.Execer).Exec(query, args)
}

func (c *polardbxConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.mysqlConn.(driver.Queryer).Query(query, args)
}

func (c *polardbxConn) Ping(ctx context.Context) (err error) {
	return c.mysqlConn.(driver.Pinger).Ping(ctx)
}

func (c *polardbxConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.mysqlConn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func (c *polardbxConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.mysqlConn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *polardbxConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.mysqlConn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *polardbxConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.mysqlConn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
}

func (c *polardbxConn) CheckNamedValue(nv *driver.NamedValue) (err error) {
	return c.mysqlConn.(driver.NamedValueChecker).CheckNamedValue(nv)
}

func (c *polardbxConn) ResetSession(ctx context.Context) error {
	return c.mysqlConn.(driver.SessionResetter).ResetSession(ctx)
}

func (c *polardbxConn) IsValid() bool {
	return c.mysqlConn.(driver.Validator).IsValid()
}
