package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/burak/db-flux-cather/src/internal/models"
)

// PostgreSQLParser implements DatabaseLogParser for PostgreSQL logs
type PostgreSQLParser struct {
	// Regex patterns for parsing different log types
	baseLogPattern        *regexp.Regexp
	executePattern        *regexp.Regexp
	statementPattern      *regexp.Regexp
	durationPattern       *regexp.Regexp
	parametersPattern     *regexp.Regexp
	parameterValuePattern *regexp.Regexp
	timestampPattern      *regexp.Regexp
}

// NewPostgreSQLParser creates a new PostgreSQL log parser
func NewPostgreSQLParser() *PostgreSQLParser {
	return &PostgreSQLParser{
		// Base log pattern: timestamp [pid] user@database LOG/DETAIL: content
		baseLogPattern: regexp.MustCompile(`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} UTC) \[(\d+)\] ([^@]+)@([^\s]+) (LOG|DETAIL|ERROR|WARNING|NOTICE):\s*(.+)$`),

		// Execute pattern: execute stmtcache_...: SQL_QUERY
		executePattern: regexp.MustCompile(`^execute\s+([^:]+):\s*(.+)$`),

		// Statement pattern: statement: SQL_QUERY
		statementPattern: regexp.MustCompile(`^statement:\s*(.+)$`),

		// Duration pattern: duration: 123.456 ms
		durationPattern: regexp.MustCompile(`^duration:\s*([0-9]+\.?[0-9]*)\s*ms$`),

		// Parameters pattern: Parameters: $1 = 'value', $2 = 'value'
		parametersPattern: regexp.MustCompile(`^Parameters:\s*(.+)$`),

		// Individual parameter pattern: $1 = 'value' or $1 = 'value'
		parameterValuePattern: regexp.MustCompile(`\$\d+\s*=\s*'([^']*)'`),

		// Timestamp pattern for parsing
		timestampPattern: regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} UTC`),
	}
}

// ParseLine parses a single PostgreSQL log line
func (p *PostgreSQLParser) ParseLine(line string) (*models.LogEntry, error) {
	if !p.IsValidLogLine(line) {
		return nil, fmt.Errorf("invalid PostgreSQL log line format")
	}

	matches := p.baseLogPattern.FindStringSubmatch(line)
	if len(matches) < 7 {
		return nil, fmt.Errorf("failed to parse base log structure")
	}

	// Parse timestamp
	timestamp, err := p.parseTimestamp(matches[1])
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	// Parse process ID
	processID, err := strconv.Atoi(matches[2])
	if err != nil {
		return nil, fmt.Errorf("failed to parse process ID: %w", err)
	}

	entry := &models.LogEntry{
		Timestamp: timestamp,
		ProcessID: processID,
		Username:  matches[3],
		Database:  matches[4],
		LogType:   matches[5],
		RawLog:    line,
		CreatedAt: time.Now(),
	}

	// Parse content based on log type
	content := matches[6]
	if err := p.parseContent(entry, content); err != nil {
		return nil, fmt.Errorf("failed to parse content: %w", err)
	}

	return entry, nil
}

// ParseLines parses multiple PostgreSQL log lines
func (p *PostgreSQLParser) ParseLines(lines []string) ([]*models.LogEntry, error) {
	var entries []*models.LogEntry
	var currentEntry *models.LogEntry

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		// Check if this is a new log entry or continuation
		if p.IsValidLogLine(line) {
			// If we have a current entry, add it to the list
			if currentEntry != nil {
				entries = append(entries, currentEntry)
			}

			// Parse new entry
			entry, err := p.ParseLine(line)
			if err != nil {
				continue // Skip invalid lines
			}
			currentEntry = entry
		} else if currentEntry != nil {
			// This might be a parameter line or continuation
			if p.isParameterLine(line) {
				params, err := p.ExtractParameters(line)
				if err == nil {
					currentEntry.Parameters = params
				}
			}
		}
	}

	// Add the last entry if exists
	if currentEntry != nil {
		entries = append(entries, currentEntry)
	}

	return entries, nil
}

// GetDatabaseType returns PostgreSQL type
func (p *PostgreSQLParser) GetDatabaseType() models.DatabaseType {
	return models.DatabaseTypePostgreSQL
}

// IsValidLogLine checks if line is valid PostgreSQL log format
func (p *PostgreSQLParser) IsValidLogLine(line string) bool {
	return p.baseLogPattern.MatchString(line)
}

// NormalizeQuery normalizes SQL query for statistics
func (p *PostgreSQLParser) NormalizeQuery(query string) string {
	// Remove parameters ($1, $2, etc.)
	normalized := regexp.MustCompile(`\$\d+`).ReplaceAllString(query, "?")

	// Remove extra whitespace
	normalized = regexp.MustCompile(`\s+`).ReplaceAllString(normalized, " ")

	// Trim and lowercase
	normalized = strings.TrimSpace(strings.ToLower(normalized))

	return normalized
}

// ExtractParameters extracts parameters from parameter lines
func (p *PostgreSQLParser) ExtractParameters(line string) ([]string, error) {
	if !p.isParameterLine(line) {
		return nil, fmt.Errorf("not a parameter line")
	}

	matches := p.parametersPattern.FindStringSubmatch(line)
	if len(matches) < 2 {
		return nil, fmt.Errorf("failed to parse parameters")
	}

	paramContent := matches[1]
	paramMatches := p.parameterValuePattern.FindAllStringSubmatch(paramContent, -1)

	var parameters []string
	for _, match := range paramMatches {
		if len(match) > 1 {
			parameters = append(parameters, match[1])
		}
	}

	return parameters, nil
}

// GetLogTimestamp extracts timestamp from log line
func (p *PostgreSQLParser) GetLogTimestamp(line string) (time.Time, error) {
	matches := p.baseLogPattern.FindStringSubmatch(line)
	if len(matches) < 2 {
		return time.Time{}, fmt.Errorf("failed to extract timestamp")
	}

	return p.parseTimestamp(matches[1])
}

// parseTimestamp parses PostgreSQL timestamp format
func (p *PostgreSQLParser) parseTimestamp(timestampStr string) (time.Time, error) {
	// PostgreSQL format: 2025-07-05 13:45:10.994 UTC
	layout := "2006-01-02 15:04:05.000 MST"
	return time.Parse(layout, timestampStr)
}

// parseContent parses the content part of log entry
func (p *PostgreSQLParser) parseContent(entry *models.LogEntry, content string) error {
	// Check for execute statement
	if matches := p.executePattern.FindStringSubmatch(content); len(matches) >= 3 {
		entry.EventType = "execute"
		entry.SQLQuery = strings.TrimSpace(matches[2])
		return nil
	}

	// Check for statement
	if matches := p.statementPattern.FindStringSubmatch(content); len(matches) >= 2 {
		entry.EventType = "statement"
		entry.SQLQuery = strings.TrimSpace(matches[1])
		return nil
	}

	// Check for duration
	if matches := p.durationPattern.FindStringSubmatch(content); len(matches) >= 2 {
		entry.EventType = "duration"
		duration, err := strconv.ParseFloat(matches[1], 64)
		if err != nil {
			return fmt.Errorf("failed to parse duration: %w", err)
		}
		entry.Duration = duration
		return nil
	}

	// Default case - store as raw content
	entry.EventType = "other"
	entry.SQLQuery = content
	return nil
}

// isParameterLine checks if line contains parameters
func (p *PostgreSQLParser) isParameterLine(line string) bool {
	return p.parametersPattern.MatchString(line)
}
