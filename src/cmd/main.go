package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/burak/db-flux-cather/src/config"
	"github.com/burak/db-flux-cather/src/internal/models"
	"github.com/burak/db-flux-cather/src/internal/parser"
	"github.com/burak/db-flux-cather/src/internal/reader"
	"github.com/burak/db-flux-cather/src/internal/repository"
	"github.com/burak/db-flux-cather/src/internal/service"
	"github.com/burak/db-flux-cather/src/pkg"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

func main() {
	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}

	// Initialize database
	db, err := pkg.NewDatabase(&cfg.Database)
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	// Initialize cache
	cache, err := pkg.NewCache(&cfg.Cache)
	if err != nil {
		log.Fatal("Failed to connect to cache:", err)
	}
	defer cache.Close()

	// Initialize log parsing components
	logRepo := repository.NewLogRepository(db.Pool)

	// Create database tables
	if err := logRepo.CreateTables(context.Background()); err != nil {
		log.Fatal("Failed to create database tables:", err)
	}

	// Initialize PostgreSQL parser
	postgresParser := parser.NewPostgreSQLParser()

	// Convert config to models.LogParsingConfig
	logParsingConfig := &models.LogParsingConfig{
		DatabaseType:     models.DatabaseType(cfg.LogParsing.DatabaseType),
		LogFilePath:      cfg.LogParsing.LogFilePath,
		PollInterval:     cfg.LogParsing.PollInterval,
		BatchSize:        cfg.LogParsing.BatchSize,
		MaxRetries:       cfg.LogParsing.MaxRetries,
		EnableStatistics: cfg.LogParsing.EnableStatistics,
	}

	// Initialize log service
	logService := service.NewLogService(logRepo, postgresParser, logParsingConfig)

	// Initialize file reader
	fileReader := reader.NewFileReader(cfg.LogParsing.PollInterval)

	// Initialize Fiber app
	app := fiber.New(fiber.Config{
		AppName: "DB Flux Cather",
	})

	// Middleware
	app.Use(logger.New())
	app.Use(cors.New())

	// Routes
	app.Get("/", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"message": "DB Flux Cather API",
			"status":  "running",
		})
	})

	// Health check endpoint
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":    "healthy",
			"timestamp": time.Now(),
		})
	})

	// Statistics endpoint
	app.Get("/stats", func(c *fiber.Ctx) error {
		stats, err := logService.GetStatistics(context.Background())
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to get statistics",
			})
		}
		return c.JSON(stats)
	})

	// Top queries endpoint
	app.Get("/top-queries", func(c *fiber.Ctx) error {
		limit := c.QueryInt("limit", 10)
		orderBy := c.Query("order_by", "avg_time")

		queries, err := logService.GetTopQueries(context.Background(), limit, orderBy)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to get top queries",
			})
		}
		return c.JSON(queries)
	})

	// Log entries endpoint
	app.Get("/logs", func(c *fiber.Ctx) error {
		limit := c.QueryInt("limit", 100)
		offset := c.QueryInt("offset", 0)

		filters := parser.LogFilters{
			Limit:  limit,
			Offset: offset,
		}

		// Add optional filters
		if database := c.Query("database"); database != "" {
			filters.Database = database
		}
		if username := c.Query("username"); username != "" {
			filters.Username = username
		}
		if eventType := c.Query("event_type"); eventType != "" {
			filters.EventType = eventType
		}

		logs, err := logService.GetLogEntries(context.Background(), filters)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "Failed to get log entries",
			})
		}
		return c.JSON(logs)
	})

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start log processing in a separate goroutine
	go func() {
		log.Printf("Starting log processing for file: %s", cfg.LogParsing.LogFilePath)

		// Callback function to process parsed log lines
		processLogLines := func(lines []string) error {
			entries, err := logService.ParseLogLines(lines)
			if err != nil {
				log.Printf("Error parsing log lines: %v", err)
				return err
			}

			if len(entries) > 0 {
				if err := logService.ProcessLogEntries(ctx, entries); err != nil {
					log.Printf("Error processing log entries: %v", err)
					return err
				}
				log.Printf("Processed %d log entries", len(entries))
			}

			return nil
		}

		// Start reading the log file
		if err := fileReader.StartReading(ctx, cfg.LogParsing.LogFilePath, processLogLines); err != nil {
			log.Printf("Error reading log file: %v", err)
		}
	}()

	// Start server in a separate goroutine
	serverAddr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	go func() {
		log.Printf("Starting server on %s", serverAddr)
		if err := app.Listen(serverAddr); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// Cancel context to stop log processing
	cancel()

	// Shutdown server
	if err := app.Shutdown(); err != nil {
		log.Printf("Error during server shutdown: %v", err)
	}

	log.Println("Server shutdown complete")
}
