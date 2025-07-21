package main

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	// MySQL connection string - modify as needed
	// Format: username:password@tcp(host:port)/database
	dsn = "root@tcp(localhost:4000)/"

	// Size of padding in bytes
	paddingSize = 256

	maxValue0 = 1000
)

func main() {
	// Connect to MySQL
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("Failed to connect to TiDB:", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		log.Fatal("Failed to ping TiDB:", err)
	}
	fmt.Println("Successfully connected to TiDB")

	// Create database and table
	if err := setupDatabase(db); err != nil {
		log.Fatal("Failed to setup database:", err)
	}

	// Use WaitGroup to wait for all goroutines
	var wg sync.WaitGroup

	// Spawn 4 goroutines for insert/delete operations
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			insertDeleteWorker(db, workerID)
		}(i)
	}

	// Spawn 1 goroutine for DDL operations
	wg.Add(1)
	go func() {
		defer wg.Done()
		ddlWorker(db)
	}()

	// Wait for all goroutines (they run indefinitely)
	wg.Wait()
}

func setupDatabase(db *sql.DB) error {
	// Create database if not exists
	_, err := db.Exec("CREATE DATABASE IF NOT EXISTS `uniq`")
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Use the database
	_, err = db.Exec("USE `uniq`")
	if err != nil {
		return fmt.Errorf("failed to use database: %w", err)
	}

	// Drop table if exists (for clean start)
	_, err = db.Exec("DROP TABLE IF EXISTS `rows`")
	if err != nil {
		return fmt.Errorf("failed to drop existing table: %w", err)
	}

	// Create table
	createTableSQL := fmt.Sprintf(`CREATE TABLE uniq.rows (
		id int NOT NULL AUTO_INCREMENT,
		val0 int NOT NULL,
		val1 int NOT NULL,
		padding varchar(%v) NOT NULL DEFAULT '',
		PRIMARY KEY (id)
	)`, paddingSize)

	_, err = db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Small batch size for easier to reproduce
	vars := []string{
		`set @@global.tidb_ddl_reorg_worker_cnt = 1`,
		`set @@global.tidb_ddl_reorg_batch_size = 32`,
	}
	for _, v := range vars {
		_, err = db.Exec(v)
		if err != nil {
			return fmt.Errorf("failed to set variable: %s, %w", v, err)
		}
	}

	fmt.Println("Database and table created successfully")
	return nil
}

func insertDeleteWorker(db *sql.DB, workerID int) {
	fmt.Printf("Worker %d started\n", workerID)

	paddingBuffer := make([]byte, paddingSize/2)

	for {
		rowsPerOperation := []int{10, 50, 100, 200}[rand.Intn(4)]
		paddingBuffer = paddingBuffer[:rowsPerOperation/2]

		val0Values := make([]int, rowsPerOperation)
		val1Values := make([]int, rowsPerOperation)
		paddingValues := make([]string, rowsPerOperation)

		val0 := rand.Intn(maxValue0)
		for i := 0; i < rowsPerOperation; i++ {
			val0Values[i] = val0
			val1Values[i] = val0 * 10
			// Fill padding with random data
			rand.Read(paddingBuffer)
			paddingValues[i] = hex.EncodeToString(paddingBuffer)

			val0 = (val0 + 1) % maxValue0
		}

		// Perform insert operation
		if err := insertRows(db, val0Values, val1Values, paddingValues, workerID); err != nil {
			log.Printf("Worker %d - Insert error: %v", workerID, err)
		}

		// Small delay before delete
		time.Sleep(500 * time.Millisecond)

		// Perform delete operation
		if err := deleteRows(db, val0Values, workerID); err != nil {
			log.Panicf("Worker %d - Delete error: %v", workerID, err)
		}

		// Small delay before next iteration
		time.Sleep(500 * time.Millisecond)
	}
}

func insertRows(db *sql.DB, val0Values, val1Values []int, paddingValues []string, workerID int) error {
	// Build insert query with multiple values
	valueStrings := make([]string, len(val0Values))
	args := make([]interface{}, 0, len(val0Values)*2)

	for i, val0 := range val0Values {
		valueStrings[i] = "(?, ?, ?)"
		args = append(args, val0, val1Values[i], paddingValues[i])
	}

	query := fmt.Sprintf("INSERT INTO `uniq`.`rows` (val0, val1, padding) VALUES %s",
		strings.Join(valueStrings, ", "))

	_, err := db.Exec(query, args...)
	if err != nil {
		// Handle duplicate key errors gracefully
		if strings.Contains(err.Error(), "Duplicate entry") {
			fmt.Printf("Worker %d - Duplicate key ignored\n", workerID)
			return nil
		}
		return fmt.Errorf("insert failed: %w", err)
	}

	fmt.Printf("Worker %d - Inserted %d rows\n", workerID, len(val0Values))
	return nil
}

func deleteRows(db *sql.DB, val0Values []int, workerID int) error {
	if len(val0Values) == 0 {
		return nil
	}

	// Build IN clause
	placeholders := make([]string, len(val0Values))
	args := make([]interface{}, len(val0Values))

	for i, val0 := range val0Values {
		placeholders[i] = "?"
		args[i] = val0
	}

	query := fmt.Sprintf("DELETE FROM `uniq`.`rows` WHERE val0 IN (%s)",
		strings.Join(placeholders, ", "))

	result, err := db.Exec(query, args...)
	if err != nil {
		// Error 8028 (HY000): public column val0 has changed
		if strings.Contains(err.Error(), "public column") && strings.Contains(err.Error(), "has changed") {
			fmt.Printf("Worker %d - Ignore public column has changed error (%s)\n", workerID, err.Error())
			return nil
		}
		return fmt.Errorf("delete failed: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	fmt.Printf("Worker %d - Deleted %d rows\n", workerID, rowsAffected)
	return nil
}

func ddlWorker(db *sql.DB) {
	fmt.Println("DDL worker started")

	for {
		// Wait 10 seconds
		time.Sleep(1 * time.Second)

		// column := []string{"val0", "val1"}[rand.Intn(2)]
		column := "val0"

		// Modify column to bigint
		fmt.Printf("DDL - Modifying %s to bigint\n", column)
		_, err := db.Exec(fmt.Sprintf("ALTER TABLE `uniq`.`rows` MODIFY COLUMN `%s` bigint NOT NULL", column))
		if err != nil {
			log.Printf("DDL error (bigint): %v", err)
		} else {
			fmt.Printf("DDL - Successfully modified %s to bigint\n", column)
		}

		// Modify column back to int
		fmt.Printf("DDL - Modifying %s to int\n", column)
		_, err = db.Exec(fmt.Sprintf("ALTER TABLE `uniq`.`rows` MODIFY COLUMN `%s` int NOT NULL", column))
		if err != nil {
			log.Printf("DDL error (int): %v", err)
		} else {
			fmt.Printf("DDL - Successfully modified %s to int\n", column)
		}
	}
}
