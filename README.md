# MySQL DDL and Delete Program

This Go program demonstrates concurrent database operations with MySQL including:
- Concurrent insert/delete operations with 4 goroutines
- DDL operations (ALTER TABLE) with 1 goroutine
- Handling unique constraints and duplicate key errors

## Prerequisites

1. **MySQL Server**: Make sure MySQL is running and accessible
2. **Go**: Go 1.23.3 or later
3. **MySQL Driver**: Already included in go.mod

## Database Configuration

Before running the program, you need to update the database connection string in `main.go`:

```go
const dsn = "root:password@tcp(localhost:3306)/"
```

Replace with your MySQL credentials:
- `root`: your MySQL username
- `password`: your MySQL password
- `localhost:3306`: your MySQL host and port

## What the Program Does

### 1. Database Setup
- Creates database `uniq` if it doesn't exist
- Creates table `rows` with the specified schema:
  ```sql
  CREATE TABLE uniq.rows (
      id int NOT NULL AUTO_INCREMENT,
      val0 int NOT NULL,
      val1 int NOT NULL,
      PRIMARY KEY (id),
      UNIQUE KEY val0_unique_idx (val0),
      KEY val1_idx (val1)
  )
  ```

### 2. Insert/Delete Workers (4 goroutines)
Each worker repeatedly:
- Generates 10 random `val0` values (0-1000)
- Calculates `val1` as `val0 * 10`
- Inserts the rows (handles duplicate key errors gracefully)
- Deletes the same rows using the `val0` values

### 3. DDL Worker (1 goroutine)
Every 10 seconds alternates between:
- `ALTER TABLE uniq.rows MODIFY COLUMN val0 bigint NOT NULL`
- `ALTER TABLE uniq.rows MODIFY COLUMN val0 int NOT NULL`

## Running the Program

1. **Install dependencies**:
   ```bash
   go mod tidy
   ```

2. **Run the program**:
   ```bash
   go run main.go
   ```

## Expected Output

The program will output logs showing:
- Database connection status
- Worker operations (inserts/deletes)
- DDL operations
- Error handling for duplicate keys and other issues

## Features

- **Concurrent Safety**: Multiple goroutines safely access the database
- **Error Handling**: Graceful handling of duplicate key constraints
- **Logging**: Detailed logging of all operations
- **Clean Shutdown**: Proper resource cleanup

## Stopping the Program

Press `Ctrl+C` to stop the program. The goroutines run indefinitely until manually stopped.
