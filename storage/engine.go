// Package storage provides a MySQL database wrapper for distributed nodes,
// ensuring isolated namespaces for each node on a shared database server.
package storage

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	// Import the MySQL driver. The underscore indicates it's imported for its side-effects (driver registration).
	_ "github.com/go-sql-driver/mysql"
)

// Engine wraps a MySQL connection for one node.
// It maintains the node's unique ID to namespace all database operations.
type Engine struct {
	nodeID string
	db     *sql.DB
}

// NewEngine initializes a new database engine for a specific node.
// It establishes the connection pool and verifies connectivity.
func NewEngine(nodeID string) *Engine {
	
	dsn := "root:root@tcp(localhost:3306)/?parseTime=true" // Data Source Name

	// Open establishes the database connection pool configuration.
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("[Node %s] Failed to open MySQL: %v", nodeID, err)
	}

	// Ping actually tests the active connection to the database server.
	if err := db.Ping(); err != nil {
		log.Printf("[Node %s] WARNING: Cannot ping MySQL: %v", nodeID, err)
	}
	log.Printf("[Node %s] Connected to MySQL", nodeID)

	return &Engine{nodeID: nodeID, db: db}
}

// FIXED: Sanitizes the URL into a safe MySQL database name
// prefixedDB prevents naming collisions by prefixing the nodeID to the requested database name.
func (e *Engine) prefixedDB(dbName string) string {
	// Remove the protocol scheme to keep the name clean
	safeID := strings.ReplaceAll(e.nodeID, "http://", "")
	// Replace invalid MySQL identifier characters (dots and colons) with underscores
	safeID = strings.ReplaceAll(safeID, ".", "_")
	safeID = strings.ReplaceAll(safeID, ":", "_")
	// Format: node_localhost_8080_dbname
	return fmt.Sprintf("node_%s_%s", safeID, dbName)
}

// CreateDB creates a new database natively namespaced for the current node.
func (e *Engine) CreateDB(dbName string) error {
	_, err := e.db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", e.prefixedDB(dbName)))
	return err
}

// DropDB drops the namespaced database if it exists.
func (e *Engine) DropDB(dbName string) error {
	_, err := e.db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", e.prefixedDB(dbName)))
	return err
}

// CreateTable dynamically builds a CREATE TABLE statement given a list of attributes.
// It automatically injects an auto-incrementing integer 'id' as the primary key.
func (e *Engine) CreateTable(dbName, tableName string, attributes []string) error {
	cols := []string{"`id` INT AUTO_INCREMENT PRIMARY KEY"}

	// Append all dynamic attributes as VARCHAR(255) strings
	for _, attr := range attributes {
		cols = append(cols, fmt.Sprintf("`%s` VARCHAR(255)", attr))
	}

	// Construct the final CREATE TABLE query string targeting the prefixed DB
	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s)",
		e.prefixedDB(dbName), tableName, strings.Join(cols, ", "),
	)

	_, err := e.db.Exec(query)
	return err
}

// DropTable safely removes a table from the node's specific database.
func (e *Engine) DropTable(dbName, tableName string) error {
	_, err := e.db.Exec(fmt.Sprintf(
		"DROP TABLE IF EXISTS `%s`.`%s`",
		e.prefixedDB(dbName), tableName,
	))
	return err
}

// Insert adds a new record into the specified table using a map of column-value pairs.
// It uses prepared statements (?) to prevent SQL injection.
func (e *Engine) Insert(dbName, tableName string, record map[string]interface{}) error {
	cols, placeholders, args := []string{}, []string{}, []interface{}{}

	// Extract columns, generate placeholders (?), and collect arguments safely
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

	// Execute the prepared query with the extracted arguments
	_, err := e.db.Exec(query, args...)
	return err
}

// Select retrieves rows matching the provided 'where' conditions.
func (e *Engine) Select(dbName, tableName string, where map[string]interface{}) ([]map[string]interface{}, error) {
	whereSQL, args := buildWhere(where)
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s`%s", e.prefixedDB(dbName), tableName, whereSQL)
	return e.queryRows(query, args...)
}

// Update modifies existing records that match the 'where' condition with the new values in 'set'.
// Returns the number of rows affected by the operation.
func (e *Engine) Update(dbName, tableName string, where, set map[string]interface{}) (int64, error) {
	setClauses, setArgs := []string{}, []interface{}{}

	// Build the SET clause (e.g., `name` = ?, `age` = ?)
	for k, v := range set {
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", k))
		setArgs = append(setArgs, v)
	}

	whereSQL, whereArgs := buildWhere(where)

	// Combine arguments: first SET values, then WHERE values
	args := append(setArgs, whereArgs...)
	query := fmt.Sprintf(
		"UPDATE `%s`.`%s` SET %s%s",
		e.prefixedDB(dbName), tableName, strings.Join(setClauses, ", "), whereSQL,
	)

	res, err := e.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	// Retrieve the number of rows actually modified
	n, _ := res.RowsAffected()
	return n, nil
}

// Delete removes records matching the 'where' conditions.
// Returns the number of rows affected by the operation.
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

// ExecRaw executes a raw SQL string directly against the prefixed database.
// It opens a temporary targeted connection to the specific database context.
func (e *Engine) ExecRaw(dbName, rawSQL string) ([]map[string]interface{}, error) {

	// Open a new connection specific to the namespaced database
	dsn := fmt.Sprintf("root:root@tcp(localhost:3306)/%s?parseTime=true", e.prefixedDB(dbName))
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close() // Ensure the temporary connection is closed

	// Check if the query is a data retrieval query (SELECT or SHOW)
	upper := strings.ToUpper(strings.TrimSpace(rawSQL))
	if strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "SHOW") {
		rows, err := db.Query(rawSQL)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanRows(rows) // Parse the result rows
	}

	// Otherwise, execute as an action query (INSERT, UPDATE, DELETE, CREATE, etc.)
	res, err := db.Exec(rawSQL)
	if err != nil {
		return nil, err
	}

	n, _ := res.RowsAffected()
	return []map[string]interface{}{{"rows_affected": n}}, nil
}

// buildWhere is a helper function that converts a map of conditions into a secure SQL WHERE clause.
// It returns the clause string and a slice of arguments for prepared statement placeholders.
func buildWhere(where map[string]interface{}) (string, []interface{}) {
	if len(where) == 0 {
		return "", nil // No conditions provided, query affects all rows
	}
	conds, args := []string{}, []interface{}{}

	// Iterate map to build "`column` = ?" strings and collect their respective values
	for k, v := range where {
		conds = append(conds, fmt.Sprintf("`%s` = ?", k))
		args = append(args, v)
	}

	return " WHERE " + strings.Join(conds, " AND "), args
}

// queryRows executes a query that returns rows and delegates the scanning logic to scanRows.
func (e *Engine) queryRows(query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := e.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close() // Ensure rows are closed to prevent memory/connection leaks
	return scanRows(rows)
}

// scanRows dynamically maps SQL result rows into a slice of generic maps (map[string]interface{}).
// This reflective approach is necessary because the columns are not known at compile time.
func scanRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	// Retrieve column names from the result set
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}

	// Iterate through each row returned by the database
	for rows.Next() {
		// Create a slice of interface{} to hold pointers to the values
		ptrs := make([]interface{}, len(cols))
		vals := make([]interface{}, len(cols))

		// Map pointers to the underlying value storage
		for i := range ptrs {
			ptrs[i] = &vals[i]
		}

		// Scan the database row's values into our pointers
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		m := make(map[string]interface{})

		// Convert byte slices to strings and store them in the map keyed by column name
		for i, col := range cols {
			if b, ok := vals[i].([]byte); ok {
				m[col] = string(b) // The MySQL driver often returns string data as raw byte slices
			} else {
				m[col] = vals[i]
			}
		}

		results = append(results, m)
	}
	return results, nil
}
