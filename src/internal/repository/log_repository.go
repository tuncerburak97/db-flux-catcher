package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/burak/db-flux-cather/src/internal/models"
	"github.com/burak/db-flux-cather/src/internal/parser"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LogRepositoryImpl implements LogRepository interface
type LogRepositoryImpl struct {
	pool *pgxpool.Pool
}

// NewLogRepository creates a new log repository instance
func NewLogRepository(pool *pgxpool.Pool) *LogRepositoryImpl {
	return &LogRepositoryImpl{
		pool: pool,
	}
}

// CreateTables creates the necessary database tables
func (r *LogRepositoryImpl) CreateTables(ctx context.Context) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS log_entries (
			id BIGSERIAL PRIMARY KEY,
			timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
			process_id INTEGER NOT NULL,
			username VARCHAR(255) NOT NULL,
			database_name VARCHAR(255) NOT NULL,
			log_type VARCHAR(50) NOT NULL,
			event_type VARCHAR(50) NOT NULL,
			sql_query TEXT,
			parameters JSONB,
			duration DOUBLE PRECISION DEFAULT 0,
			raw_log TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)`,

		`CREATE TABLE IF NOT EXISTS query_stats (
			id BIGSERIAL PRIMARY KEY,
			sql_signature TEXT NOT NULL UNIQUE,
			call_count BIGINT DEFAULT 0,
			total_time DOUBLE PRECISION DEFAULT 0,
			avg_time DOUBLE PRECISION DEFAULT 0,
			min_time DOUBLE PRECISION DEFAULT 0,
			max_time DOUBLE PRECISION DEFAULT 0,
			last_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)`,

		// Indexes for better performance
		`CREATE INDEX IF NOT EXISTS idx_log_entries_timestamp ON log_entries(timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_log_entries_database ON log_entries(database_name)`,
		`CREATE INDEX IF NOT EXISTS idx_log_entries_username ON log_entries(username)`,
		`CREATE INDEX IF NOT EXISTS idx_log_entries_event_type ON log_entries(event_type)`,
		`CREATE INDEX IF NOT EXISTS idx_log_entries_duration ON log_entries(duration)`,
		`CREATE INDEX IF NOT EXISTS idx_query_stats_signature ON query_stats(sql_signature)`,
		`CREATE INDEX IF NOT EXISTS idx_query_stats_call_count ON query_stats(call_count)`,
		`CREATE INDEX IF NOT EXISTS idx_query_stats_avg_time ON query_stats(avg_time)`,
	}

	for _, query := range queries {
		_, err := r.pool.Exec(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	return nil
}

// SaveLogEntry saves a single log entry to database
func (r *LogRepositoryImpl) SaveLogEntry(ctx context.Context, entry *models.LogEntry) error {
	query := `
		INSERT INTO log_entries (
			timestamp, process_id, username, database_name, log_type, 
			event_type, sql_query, parameters, duration, raw_log, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING id`

	// Convert parameters to JSON
	var parametersJSON []byte
	if len(entry.Parameters) > 0 {
		var err error
		parametersJSON, err = json.Marshal(entry.Parameters)
		if err != nil {
			return fmt.Errorf("failed to marshal parameters: %w", err)
		}
	}

	var id int64
	err := r.pool.QueryRow(ctx, query,
		entry.Timestamp,
		entry.ProcessID,
		entry.Username,
		entry.Database,
		entry.LogType,
		entry.EventType,
		entry.SQLQuery,
		parametersJSON,
		entry.Duration,
		entry.RawLog,
		entry.CreatedAt,
	).Scan(&id)

	if err != nil {
		return fmt.Errorf("failed to save log entry: %w", err)
	}

	entry.ID = id
	return nil
}

// SaveLogEntries saves multiple log entries in batch
func (r *LogRepositoryImpl) SaveLogEntries(ctx context.Context, entries []*models.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Use pgx batch for better performance
	batch := &pgx.Batch{}
	query := `
		INSERT INTO log_entries (
			timestamp, process_id, username, database_name, log_type, 
			event_type, sql_query, parameters, duration, raw_log, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

	for _, entry := range entries {
		// Convert parameters to JSON
		var parametersJSON []byte
		if len(entry.Parameters) > 0 {
			var err error
			parametersJSON, err = json.Marshal(entry.Parameters)
			if err != nil {
				continue // Skip this entry if parameters can't be marshaled
			}
		}

		batch.Queue(query,
			entry.Timestamp,
			entry.ProcessID,
			entry.Username,
			entry.Database,
			entry.LogType,
			entry.EventType,
			entry.SQLQuery,
			parametersJSON,
			entry.Duration,
			entry.RawLog,
			entry.CreatedAt,
		)
	}

	batchResults := r.pool.SendBatch(ctx, batch)
	defer batchResults.Close()

	for i := 0; i < len(entries); i++ {
		_, err := batchResults.Exec()
		if err != nil {
			return fmt.Errorf("failed to save batch entry %d: %w", i, err)
		}
	}

	return nil
}

// GetLogEntries retrieves log entries with filters
func (r *LogRepositoryImpl) GetLogEntries(ctx context.Context, filters parser.LogFilters) ([]*models.LogEntry, error) {
	query := `
		SELECT id, timestamp, process_id, username, database_name, log_type, 
			   event_type, sql_query, parameters, duration, raw_log, created_at
		FROM log_entries
		WHERE 1=1`

	var args []interface{}
	var conditions []string
	argIndex := 1

	// Add filters
	if filters.StartTime != nil {
		conditions = append(conditions, fmt.Sprintf("timestamp >= $%d", argIndex))
		args = append(args, *filters.StartTime)
		argIndex++
	}

	if filters.EndTime != nil {
		conditions = append(conditions, fmt.Sprintf("timestamp <= $%d", argIndex))
		args = append(args, *filters.EndTime)
		argIndex++
	}

	if filters.Database != "" {
		conditions = append(conditions, fmt.Sprintf("database_name = $%d", argIndex))
		args = append(args, filters.Database)
		argIndex++
	}

	if filters.Username != "" {
		conditions = append(conditions, fmt.Sprintf("username = $%d", argIndex))
		args = append(args, filters.Username)
		argIndex++
	}

	if filters.EventType != "" {
		conditions = append(conditions, fmt.Sprintf("event_type = $%d", argIndex))
		args = append(args, filters.EventType)
		argIndex++
	}

	if filters.MinDuration != nil {
		conditions = append(conditions, fmt.Sprintf("duration >= $%d", argIndex))
		args = append(args, *filters.MinDuration)
		argIndex++
	}

	if filters.MaxDuration != nil {
		conditions = append(conditions, fmt.Sprintf("duration <= $%d", argIndex))
		args = append(args, *filters.MaxDuration)
		argIndex++
	}

	if len(conditions) > 0 {
		query += " AND " + strings.Join(conditions, " AND ")
	}

	query += " ORDER BY timestamp DESC"

	if filters.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, filters.Limit)
		argIndex++
	}

	if filters.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIndex)
		args = append(args, filters.Offset)
		argIndex++
	}

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query log entries: %w", err)
	}
	defer rows.Close()

	var entries []*models.LogEntry
	for rows.Next() {
		entry := &models.LogEntry{}
		var parametersJSON []byte

		err := rows.Scan(
			&entry.ID,
			&entry.Timestamp,
			&entry.ProcessID,
			&entry.Username,
			&entry.Database,
			&entry.LogType,
			&entry.EventType,
			&entry.SQLQuery,
			&parametersJSON,
			&entry.Duration,
			&entry.RawLog,
			&entry.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan log entry: %w", err)
		}

		// Parse parameters JSON
		if len(parametersJSON) > 0 {
			if err := json.Unmarshal(parametersJSON, &entry.Parameters); err != nil {
				// If unmarshaling fails, continue without parameters
				entry.Parameters = nil
			}
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// UpdateQueryStats updates query statistics
func (r *LogRepositoryImpl) UpdateQueryStats(ctx context.Context, stats *models.QueryStats) error {
	query := `
		INSERT INTO query_stats (sql_signature, call_count, total_time, avg_time, min_time, max_time, last_seen, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (sql_signature) DO UPDATE SET
			call_count = query_stats.call_count + EXCLUDED.call_count,
			total_time = query_stats.total_time + EXCLUDED.total_time,
			avg_time = (query_stats.total_time + EXCLUDED.total_time) / (query_stats.call_count + EXCLUDED.call_count),
			min_time = LEAST(query_stats.min_time, EXCLUDED.min_time),
			max_time = GREATEST(query_stats.max_time, EXCLUDED.max_time),
			last_seen = EXCLUDED.last_seen,
			updated_at = EXCLUDED.updated_at
		RETURNING id`

	now := time.Now()
	var id int64
	err := r.pool.QueryRow(ctx, query,
		stats.SQLSignature,
		stats.CallCount,
		stats.TotalTime,
		stats.AvgTime,
		stats.MinTime,
		stats.MaxTime,
		stats.LastSeen,
		now,
		now,
	).Scan(&id)

	if err != nil {
		return fmt.Errorf("failed to update query stats: %w", err)
	}

	stats.ID = id
	return nil
}

// GetQueryStats retrieves query statistics
func (r *LogRepositoryImpl) GetQueryStats(ctx context.Context, sqlSignature string) (*models.QueryStats, error) {
	query := `
		SELECT id, sql_signature, call_count, total_time, avg_time, min_time, max_time, last_seen, created_at, updated_at
		FROM query_stats
		WHERE sql_signature = $1`

	stats := &models.QueryStats{}
	err := r.pool.QueryRow(ctx, query, sqlSignature).Scan(
		&stats.ID,
		&stats.SQLSignature,
		&stats.CallCount,
		&stats.TotalTime,
		&stats.AvgTime,
		&stats.MinTime,
		&stats.MaxTime,
		&stats.LastSeen,
		&stats.CreatedAt,
		&stats.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get query stats: %w", err)
	}

	return stats, nil
}

// GetTopQueries returns top queries by execution time or count
func (r *LogRepositoryImpl) GetTopQueries(ctx context.Context, limit int, orderBy string) ([]*models.QueryStats, error) {
	var orderClause string
	switch orderBy {
	case "avg_time":
		orderClause = "avg_time DESC"
	case "total_time":
		orderClause = "total_time DESC"
	case "call_count":
		orderClause = "call_count DESC"
	case "max_time":
		orderClause = "max_time DESC"
	default:
		orderClause = "avg_time DESC"
	}

	query := fmt.Sprintf(`
		SELECT id, sql_signature, call_count, total_time, avg_time, min_time, max_time, last_seen, created_at, updated_at
		FROM query_stats
		ORDER BY %s
		LIMIT $1`, orderClause)

	rows, err := r.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query top queries: %w", err)
	}
	defer rows.Close()

	var stats []*models.QueryStats
	for rows.Next() {
		stat := &models.QueryStats{}
		err := rows.Scan(
			&stat.ID,
			&stat.SQLSignature,
			&stat.CallCount,
			&stat.TotalTime,
			&stat.AvgTime,
			&stat.MinTime,
			&stat.MaxTime,
			&stat.LastSeen,
			&stat.CreatedAt,
			&stat.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan query stats: %w", err)
		}
		stats = append(stats, stat)
	}

	return stats, nil
}
