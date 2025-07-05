package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/burak/db-flux-cather/src/internal/models"
	"github.com/burak/db-flux-cather/src/internal/parser"
)

// LogService handles log processing business logic
type LogService struct {
	repository   parser.LogRepository
	parser       parser.DatabaseLogParser
	config       *models.LogParsingConfig
	statsCache   map[string]*models.QueryStats
	statsCacheMu sync.RWMutex
	processingMu sync.Mutex
}

// NewLogService creates a new log service instance
func NewLogService(repository parser.LogRepository, logParser parser.DatabaseLogParser, config *models.LogParsingConfig) *LogService {
	return &LogService{
		repository:   repository,
		parser:       logParser,
		config:       config,
		statsCache:   make(map[string]*models.QueryStats),
		statsCacheMu: sync.RWMutex{},
		processingMu: sync.Mutex{},
	}
}

// ProcessLogEntry processes a single log entry
func (s *LogService) ProcessLogEntry(ctx context.Context, entry *models.LogEntry) error {
	s.processingMu.Lock()
	defer s.processingMu.Unlock()

	// Save the log entry to database
	if err := s.repository.SaveLogEntry(ctx, entry); err != nil {
		return fmt.Errorf("failed to save log entry: %w", err)
	}

	// Generate statistics if enabled and entry has duration
	if s.config.EnableStatistics && entry.Duration > 0 && entry.SQLQuery != "" {
		if err := s.updateQueryStatistics(ctx, entry); err != nil {
			log.Printf("Failed to update query statistics: %v", err)
			// Don't return error here, as the main log entry was saved successfully
		}
	}

	return nil
}

// ProcessLogEntries processes multiple log entries
func (s *LogService) ProcessLogEntries(ctx context.Context, entries []*models.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	s.processingMu.Lock()
	defer s.processingMu.Unlock()

	// Save all entries in batch
	if err := s.repository.SaveLogEntries(ctx, entries); err != nil {
		return fmt.Errorf("failed to save log entries: %w", err)
	}

	// Generate statistics if enabled
	if s.config.EnableStatistics {
		if err := s.GenerateStatistics(ctx, entries); err != nil {
			log.Printf("Failed to generate statistics: %v", err)
			// Don't return error here, as the main log entries were saved successfully
		}
	}

	return nil
}

// GenerateStatistics generates query statistics from log entries
func (s *LogService) GenerateStatistics(ctx context.Context, entries []*models.LogEntry) error {
	// Group entries by normalized SQL signature
	queryGroups := make(map[string][]*models.LogEntry)

	for _, entry := range entries {
		if entry.SQLQuery == "" || entry.Duration <= 0 {
			continue
		}

		signature := s.parser.NormalizeQuery(entry.SQLQuery)
		if signature == "" {
			continue
		}

		queryGroups[signature] = append(queryGroups[signature], entry)
	}

	// Process each query group
	for signature, groupEntries := range queryGroups {
		if err := s.processQueryGroup(ctx, signature, groupEntries); err != nil {
			log.Printf("Failed to process query group %s: %v", signature, err)
		}
	}

	return nil
}

// processQueryGroup processes a group of entries for the same query signature
func (s *LogService) processQueryGroup(ctx context.Context, signature string, entries []*models.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Calculate statistics
	var totalTime float64
	var minTime, maxTime float64
	var lastSeen time.Time

	minTime = entries[0].Duration
	maxTime = entries[0].Duration
	lastSeen = entries[0].Timestamp

	for _, entry := range entries {
		totalTime += entry.Duration

		if entry.Duration < minTime {
			minTime = entry.Duration
		}
		if entry.Duration > maxTime {
			maxTime = entry.Duration
		}
		if entry.Timestamp.After(lastSeen) {
			lastSeen = entry.Timestamp
		}
	}

	avgTime := totalTime / float64(len(entries))
	callCount := int64(len(entries))

	// Check if we have existing stats for this query
	s.statsCacheMu.RLock()
	existingStats, exists := s.statsCache[signature]
	s.statsCacheMu.RUnlock()

	var stats *models.QueryStats
	if exists {
		// Update existing stats
		stats = &models.QueryStats{
			ID:           existingStats.ID,
			SQLSignature: signature,
			CallCount:    callCount,
			TotalTime:    totalTime,
			AvgTime:      avgTime,
			MinTime:      minTime,
			MaxTime:      maxTime,
			LastSeen:     lastSeen,
			CreatedAt:    existingStats.CreatedAt,
			UpdatedAt:    time.Now(),
		}
	} else {
		// Create new stats
		stats = &models.QueryStats{
			SQLSignature: signature,
			CallCount:    callCount,
			TotalTime:    totalTime,
			AvgTime:      avgTime,
			MinTime:      minTime,
			MaxTime:      maxTime,
			LastSeen:     lastSeen,
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}
	}

	// Update stats in database
	if err := s.repository.UpdateQueryStats(ctx, stats); err != nil {
		return fmt.Errorf("failed to update query stats: %w", err)
	}

	// Update cache
	s.statsCacheMu.Lock()
	s.statsCache[signature] = stats
	s.statsCacheMu.Unlock()

	return nil
}

// updateQueryStatistics updates statistics for a single query
func (s *LogService) updateQueryStatistics(ctx context.Context, entry *models.LogEntry) error {
	signature := s.parser.NormalizeQuery(entry.SQLQuery)
	if signature == "" {
		return nil
	}

	return s.processQueryGroup(ctx, signature, []*models.LogEntry{entry})
}

// GetLogEntries retrieves log entries with filters
func (s *LogService) GetLogEntries(ctx context.Context, filters parser.LogFilters) ([]*models.LogEntry, error) {
	return s.repository.GetLogEntries(ctx, filters)
}

// GetQueryStats retrieves query statistics
func (s *LogService) GetQueryStats(ctx context.Context, sqlSignature string) (*models.QueryStats, error) {
	// Check cache first
	s.statsCacheMu.RLock()
	cached, exists := s.statsCache[sqlSignature]
	s.statsCacheMu.RUnlock()

	if exists {
		return cached, nil
	}

	// Get from database
	stats, err := s.repository.GetQueryStats(ctx, sqlSignature)
	if err != nil {
		return nil, err
	}

	// Update cache
	if stats != nil {
		s.statsCacheMu.Lock()
		s.statsCache[sqlSignature] = stats
		s.statsCacheMu.Unlock()
	}

	return stats, nil
}

// GetTopQueries returns top queries by execution time or count
func (s *LogService) GetTopQueries(ctx context.Context, limit int, orderBy string) ([]*models.QueryStats, error) {
	return s.repository.GetTopQueries(ctx, limit, orderBy)
}

// ParseLogLines parses log lines using the configured parser
func (s *LogService) ParseLogLines(lines []string) ([]*models.LogEntry, error) {
	return s.parser.ParseLines(lines)
}

// GetDatabaseType returns the database type this service is configured for
func (s *LogService) GetDatabaseType() models.DatabaseType {
	return s.parser.GetDatabaseType()
}

// InitializeDatabase creates necessary database tables
func (s *LogService) InitializeDatabase(ctx context.Context) error {
	return s.repository.CreateTables(ctx)
}

// ClearStatsCache clears the in-memory statistics cache
func (s *LogService) ClearStatsCache() {
	s.statsCacheMu.Lock()
	defer s.statsCacheMu.Unlock()
	s.statsCache = make(map[string]*models.QueryStats)
}

// GetCacheSize returns the current size of the stats cache
func (s *LogService) GetCacheSize() int {
	s.statsCacheMu.RLock()
	defer s.statsCacheMu.RUnlock()
	return len(s.statsCache)
}

// ProcessLogFile processes a single log file
func (s *LogService) ProcessLogFile(ctx context.Context, filePath string) error {
	log.Printf("Processing log file: %s", filePath)

	// This method will be implemented when we add file reader
	// For now, it's a placeholder
	return fmt.Errorf("file processing not implemented yet")
}

// GetStatistics returns general statistics about log processing
func (s *LogService) GetStatistics(ctx context.Context) (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	// Get total log entries count
	filters := parser.LogFilters{Limit: 1}
	entries, err := s.repository.GetLogEntries(ctx, filters)
	if err != nil {
		return nil, fmt.Errorf("failed to get log entries count: %w", err)
	}

	stats["total_entries"] = len(entries)
	stats["cache_size"] = s.GetCacheSize()
	stats["database_type"] = s.GetDatabaseType()
	stats["last_updated"] = time.Now()

	return stats, nil
}
