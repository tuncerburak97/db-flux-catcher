package parser

import (
	"context"
	"time"

	"github.com/burak/db-flux-cather/src/internal/models"
)

// DatabaseLogParser interface for parsing database log files
type DatabaseLogParser interface {
	// ParseLine parses a single log line and returns a LogEntry
	ParseLine(line string) (*models.LogEntry, error)

	// ParseLines parses multiple log lines and returns a slice of LogEntry
	ParseLines(lines []string) ([]*models.LogEntry, error)

	// GetDatabaseType returns the database type this parser supports
	GetDatabaseType() models.DatabaseType

	// IsValidLogLine checks if a line is a valid log line for this database type
	IsValidLogLine(line string) bool

	// NormalizeQuery normalizes a SQL query by removing parameters for statistics
	NormalizeQuery(query string) string

	// ExtractParameters extracts parameters from log lines if available
	ExtractParameters(line string) ([]string, error)

	// GetLogTimestamp extracts timestamp from log line
	GetLogTimestamp(line string) (time.Time, error)
}

// LogRepository interface for database operations
type LogRepository interface {
	// SaveLogEntry saves a single log entry to database
	SaveLogEntry(ctx context.Context, entry *models.LogEntry) error

	// SaveLogEntries saves multiple log entries in batch
	SaveLogEntries(ctx context.Context, entries []*models.LogEntry) error

	// GetLogEntries retrieves log entries with filters
	GetLogEntries(ctx context.Context, filters LogFilters) ([]*models.LogEntry, error)

	// UpdateQueryStats updates query statistics
	UpdateQueryStats(ctx context.Context, stats *models.QueryStats) error

	// GetQueryStats retrieves query statistics
	GetQueryStats(ctx context.Context, sqlSignature string) (*models.QueryStats, error)

	// GetTopQueries returns top queries by execution time or count
	GetTopQueries(ctx context.Context, limit int, orderBy string) ([]*models.QueryStats, error)

	// CreateTables creates necessary database tables
	CreateTables(ctx context.Context) error
}

// LogFilters for filtering log entries
type LogFilters struct {
	StartTime   *time.Time
	EndTime     *time.Time
	Database    string
	Username    string
	EventType   string
	MinDuration *float64
	MaxDuration *float64
	Limit       int
	Offset      int
}

// LogProcessor interface for processing log entries
type LogProcessor interface {
	// ProcessLogEntry processes a single log entry
	ProcessLogEntry(ctx context.Context, entry *models.LogEntry) error

	// ProcessLogEntries processes multiple log entries
	ProcessLogEntries(ctx context.Context, entries []*models.LogEntry) error

	// GenerateStatistics generates query statistics
	GenerateStatistics(ctx context.Context, entries []*models.LogEntry) error
}

// FileReader interface for reading log files
type FileReader interface {
	// StartReading starts reading the log file continuously
	StartReading(ctx context.Context, filePath string, callback func(lines []string) error) error

	// ReadFromPosition reads file from a specific position
	ReadFromPosition(filePath string, position int64) ([]string, int64, error)

	// GetFileSize returns the current file size
	GetFileSize(filePath string) (int64, error)

	// WatchFile watches for file changes
	WatchFile(ctx context.Context, filePath string) (<-chan []string, error)
}
