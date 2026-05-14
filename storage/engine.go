package storage

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// Engine wraps a MySQL connection for one node.
type Engine struct {
	nodeID string
	db     *sql.DB
}

func NewEngine(nodeID string) *Engine {
	// Note: If you want all nodes to share ONE database instead of having their own,
	// change "localhost" here to the Master's IP (e.g., "192.168.1.15")
	dsn := "root:root@tcp(localhost:3306)/?parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("[Node %s] Failed to open MySQL: %v", nodeID, err)
	}
	if err := db.Ping(); err != nil {
		log.Printf("[Node %s] WARNING: Cannot ping MySQL: %v", nodeID, err)
	}
	log.Printf("[Node %s] Connected to MySQL", nodeID)
	return &Engine{nodeID: nodeID, db: db}
}

// FIXED: Sanitizes the URL into a safe MySQL database name
func (e *Engine) prefixedDB(dbName string) string {
	safeID := strings.ReplaceAll(e.nodeID, "http://", "")
	safeID = strings.ReplaceAll(safeID, ".", "_")
	safeID = strings.ReplaceAll(safeID, ":", "_")
	return fmt.Sprintf("node_%s_%s", safeID, dbName)
}

func (e *Engine) CreateDB(dbName string) error {
	_, err := e.db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", e.prefixedDB(dbName)))
	return err
}

func (e *Engine) DropDB(dbName string) error {
	_, err := e.db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", e.prefixedDB(dbName)))
	return err
}

func (e *Engine) CreateTable(dbName, tableName string, attributes []string) error {
	cols := []string{"`id` INT AUTO_INCREMENT PRIMARY KEY"}
	for _, attr := range attributes {
		cols = append(cols, fmt.Sprintf("`%s` VARCHAR(255)", attr))
	}
	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s)",
		e.prefixedDB(dbName), tableName, strings.Join(cols, ", "),
	)
	_, err := e.db.Exec(query)
	return err
}

func (e *Engine) DropTable(dbName, tableName string) error {
	_, err := e.db.Exec(fmt.Sprintf(
		"DROP TABLE IF EXISTS `%s`.`%s`",
		e.prefixedDB(dbName), tableName,
	))
	return err
}

func (e *Engine) Insert(dbName, tableName string, record map[string]interface{}) error {
	cols, placeholders, args := []string{}, []string{}, []interface{}{}
	for k, v := range record {
		cols = append(cols, fmt.Sprintf("`%s`", k))
		placeholders = append(placeholders, "?")
		args = append(args, v)
	}
	query := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (%s) VALUES (%s)",
		e.prefixedDB(dbName), tableName,
		strings.Join(cols, ", "), strings.Join(placeholders, ", "),
	)
	_, err := e.db.Exec(query, args...)
	return err
}

func (e *Engine) Select(dbName, tableName string, where map[string]interface{}) ([]map[string]interface{}, error) {
	whereSQL, args := buildWhere(where)
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s`%s", e.prefixedDB(dbName), tableName, whereSQL)
	return e.queryRows(query, args...)
}

func (e *Engine) Update(dbName, tableName string, where, set map[string]interface{}) (int64, error) {
	setClauses, setArgs := []string{}, []interface{}{}
	for k, v := range set {
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", k))
		setArgs = append(setArgs, v)
	}
	whereSQL, whereArgs := buildWhere(where)
	args := append(setArgs, whereArgs...)
	query := fmt.Sprintf(
		"UPDATE `%s`.`%s` SET %s%s",
		e.prefixedDB(dbName), tableName, strings.Join(setClauses, ", "), whereSQL,
	)
	res, err := e.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (e *Engine) Delete(dbName, tableName string, where map[string]interface{}) (int64, error) {
	whereSQL, args := buildWhere(where)
	query := fmt.Sprintf("DELETE FROM `%s`.`%s`%s", e.prefixedDB(dbName), tableName, whereSQL)
	res, err := e.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (e *Engine) ExecRaw(dbName, rawSQL string) ([]map[string]interface{}, error) {
	
	dsn := fmt.Sprintf("root:root@tcp(localhost:3306)/%s?parseTime=true", e.prefixedDB(dbName))
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	upper := strings.ToUpper(strings.TrimSpace(rawSQL))
	if strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "SHOW") {
		rows, err := db.Query(rawSQL)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanRows(rows)
	}
	res, err := db.Exec(rawSQL)
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	return []map[string]interface{}{{"rows_affected": n}}, nil
}

func buildWhere(where map[string]interface{}) (string, []interface{}) {
	if len(where) == 0 {
		return "", nil
	}
	conds, args := []string{}, []interface{}{}
	for k, v := range where {
		conds = append(conds, fmt.Sprintf("`%s` = ?", k))
		args = append(args, v)
	}
	return " WHERE " + strings.Join(conds, " AND "), args
}

func (e *Engine) queryRows(query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := e.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

func scanRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var results []map[string]interface{}
	for rows.Next() {
		ptrs := make([]interface{}, len(cols))
		vals := make([]interface{}, len(cols))
		for i := range ptrs {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := make(map[string]interface{})
		for i, col := range cols {
			if b, ok := vals[i].([]byte); ok {
				m[col] = string(b)
			} else {
				m[col] = vals[i]
			}
		}
		results = append(results, m)
	}
	return results, nil
}
