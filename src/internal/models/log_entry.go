package models

import (
	"time"
)

// LogEntry represents a parsed database log entry
type LogEntry struct {
	ID         int64     `json:"id"`
	Timestamp  time.Time `json:"timestamp"`
	ProcessID  int       `json:"process_id"`
	Username   string    `json:"username"`
	Database   string    `json:"database"`
	LogType    string    `json:"log_type"`   // LOG, DETAIL, ERROR, etc.
	EventType  string    `json:"event_type"` // execute, statement, duration
	SQLQuery   string    `json:"sql_query"`
	Parameters []string  `json:"parameters"`
	Duration   float64   `json:"duration"` // in milliseconds
	RawLog     string    `json:"raw_log"`
	CreatedAt  time.Time `json:"created_at"`
}

// QueryStats represents aggregated query statistics
type QueryStats struct {
	ID           int64     `json:"id"`
	SQLSignature string    `json:"sql_signature"` // normalized query without parameters
	CallCount    int64     `json:"call_count"`
	TotalTime    float64   `json:"total_time"`
	AvgTime      float64   `json:"avg_time"`
	MinTime      float64   `json:"min_time"`
	MaxTime      float64   `json:"max_time"`
	LastSeen     time.Time `json:"last_seen"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// DatabaseType represents supported database types
type DatabaseType string

const (
	DatabaseTypePostgreSQL DatabaseType = "postgresql"
	DatabaseTypeMySQL      DatabaseType = "mysql"
	DatabaseTypeMongoDB    DatabaseType = "mongodb"
)

// LogParsingConfig represents configuration for log parsing
type LogParsingConfig struct {
	DatabaseType     DatabaseType  `json:"database_type"`
	LogFilePath      string        `json:"log_file_path"`
	PollInterval     time.Duration `json:"poll_interval"`
	BatchSize        int           `json:"batch_size"`
	MaxRetries       int           `json:"max_retries"`
	EnableStatistics bool          `json:"enable_statistics"`
}
