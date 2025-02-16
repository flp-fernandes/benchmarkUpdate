package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Item representa um registro para update
type Item struct {
	ID    int
	Value string
}

// Configuração do benchmark
const (
	dbConnString = "postgres://postgres:postgres@localhost:5432/postgres"
	tableName    = "benchmark_items"
)

// setupTestDB prepara o banco para os testes
func setupTestDB(ctx context.Context) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dbConnString)
	if err != nil {
		return nil, err
	}

	// Configurar pool
	config.MaxConns = 10
	config.MinConns = 1
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = time.Minute * 30

	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	// Criar tabela de teste
	_, err = db.Exec(ctx, fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id INTEGER PRIMARY KEY,
			value TEXT NOT NULL
		);
		CREATE INDEX idx_%s_value ON %s(value);
	`, tableName, tableName, tableName, tableName))
	if err != nil {
		return nil, fmt.Errorf("erro criando tabela: %w", err)
	}

	// Inserir dados iniciais
	log.Printf("Inserindo dados iniciais na tabela %s...", tableName)
	batch := &pgx.Batch{}

	// Inserir dados base (máximo necessário para os testes)
	for i := 1; i <= 10000; i++ {
		batch.Queue(fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES ($1, $2)",
			tableName,
		), i, fmt.Sprintf("initial_value_%d", i))
	}

	br := db.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return nil, fmt.Errorf("erro inserindo dado inicial %d: %w", i+1, err)
		}
	}

	log.Printf("Dados iniciais inseridos com sucesso")
	return db, nil
}

// generateTestData gera dados de teste
func generateTestData(size int) []Item {
	items := make([]Item, size)
	for i := range items {
		items[i] = Item{
			ID:    i + 1,
			Value: fmt.Sprintf("value_%d", rand.Intn(1000000)),
		}
	}
	return items
}

// Implementações de update

func batchUpdateSingleQuery(ctx context.Context, db *pgxpool.Pool, items []Item) error {
	// Construir a parte VALUES da query
	valueStrings := make([]string, len(items))
	valueArgs := make([]interface{}, len(items)*2)
	for i, item := range items {
		valueStrings[i] = fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2)
		valueArgs[i*2] = item.ID
		valueArgs[i*2+1] = item.Value
	}

	query := fmt.Sprintf(`
		UPDATE %s 
		SET value = tmp.value
		FROM (VALUES %s) AS tmp(id, value)
		WHERE %s.id = tmp.id::integer
	`, tableName, valueStrings[0], tableName)

	_, err := db.Exec(ctx, query, valueArgs...)
	if err != nil {
		return fmt.Errorf("erro no update em lote: %w", err)
	}

	return nil
}

func batchUpdateUsingBatch(ctx context.Context, db *pgxpool.Pool, items []Item) error {
	batch := &pgx.Batch{}

	for _, item := range items {
		batch.Queue(fmt.Sprintf(
			"UPDATE %s SET value = $1 WHERE id = $2",
			tableName,
		), item.Value, item.ID)
	}

	br := db.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("erro no item %d: %w", i, err)
		}
	}

	return nil
}

// Benchmark principal
func BenchmarkUpdates(b *testing.B) {
	ctx := context.Background()

	// Preparar banco
	db, err := setupTestDB(ctx)
	if err != nil {
		b.Fatalf("Erro ao preparar banco: %v", err)
	}
	defer db.Close()

	// Tamanhos de lote para testar
	batchSizes := []int{100, 1000, 5000, 10000}

	for _, size := range batchSizes {
		items := generateTestData(size)

		// Benchmark SingleQuery
		b.Run(fmt.Sprintf("SingleQuery_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				// Restaurar dados
				resetTestData(ctx, db, items)
				b.StartTimer()

				if err := batchUpdateSingleQuery(ctx, db, items); err != nil {
					b.Fatalf("Erro no SingleQuery: %v", err)
				}
			}
		})

		// Benchmark Batch
		b.Run(fmt.Sprintf("Batch_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				// Restaurar dados
				resetTestData(ctx, db, items)
				b.StartTimer()

				if err := batchUpdateUsingBatch(ctx, db, items); err != nil {
					b.Fatalf("Erro no Batch: %v", err)
				}
			}
		})

		// Benchmark com memória
		b.Run(fmt.Sprintf("Memory_SingleQuery_%d", size), func(b *testing.B) {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			beforeAlloc := m.TotalAlloc

			if err := batchUpdateSingleQuery(ctx, db, items); err != nil {
				b.Fatalf("Erro no SingleQuery: %v", err)
			}

			runtime.ReadMemStats(&m)
			b.ReportMetric(float64(m.TotalAlloc-beforeAlloc), "bytes_allocated")
		})

		b.Run(fmt.Sprintf("Memory_Batch_%d", size), func(b *testing.B) {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			beforeAlloc := m.TotalAlloc

			if err := batchUpdateUsingBatch(ctx, db, items); err != nil {
				b.Fatalf("Erro no Batch: %v", err)
			}

			runtime.ReadMemStats(&m)
			b.ReportMetric(float64(m.TotalAlloc-beforeAlloc), "bytes_allocated")
		})
	}
}

// Helper para resetar dados de teste
func resetTestData(ctx context.Context, db *pgxpool.Pool, items []Item) error {
	_, err := db.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", tableName))
	if err != nil {
		return err
	}

	batch := &pgx.Batch{}
	for _, item := range items {
		batch.Queue(fmt.Sprintf(
			"INSERT INTO %s (id, value) VALUES ($1, $2)",
			tableName,
		), item.ID, item.Value)
	}

	br := db.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}

	return nil
}

func testQueryConstruction() {
	// Teste com um conjunto pequeno de dados
	items := []Item{
		{ID: 1, Value: "test1"},
		{ID: 2, Value: "test2"},
		{ID: 3, Value: "test3"},
	}

	valueStrings := make([]string, 0, len(items))
	valueArgs := make([]interface{}, 0, len(items)*2)

	for i, item := range items {
		paramBase := i*2 + 1
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d)", paramBase, paramBase+1))
		valueArgs = append(valueArgs, item.ID, item.Value)
	}

	query := fmt.Sprintf(`
        UPDATE %s 
        SET value = tmp.value
        FROM (VALUES %s) AS tmp(id, value)
        WHERE %s.id = tmp.id::integer
    `, tableName, strings.Join(valueStrings, ","), tableName)

	log.Printf("Query teste: %s", query)
	log.Printf("Args teste: %v", valueArgs)
	log.Printf("Número de placeholders: %d", strings.Count(query, "$"))
	log.Printf("Número de argumentos: %d", len(valueArgs))
}

func main() {
	// Testar construção da query primeiro
	// testQueryConstruction()

	// Para rodar o benchmark manualmente
	ctx := context.Background()
	db, err := setupTestDB(ctx)
	if err != nil {
		log.Fatalf("Erro ao preparar banco: %v", err)
	}
	defer db.Close()

	// Testar com diferentes tamanhos
	sizes := []int{100, 1000, 5000, 10000}
	for _, size := range sizes {
		items := generateTestData(size)

		start := time.Now()
		err = batchUpdateSingleQuery(ctx, db, items)
		singleQueryDuration := time.Since(start)

		if err != nil {
			log.Printf("Erro no SingleQuery (%d items): %v", size, err)
			continue
		}

		start = time.Now()
		err = batchUpdateUsingBatch(ctx, db, items)
		batchDuration := time.Since(start)

		if err != nil {
			log.Printf("Erro no Batch (%d items): %v", size, err)
			continue
		}

		log.Printf("Tamanho do lote: %d", size)
		log.Printf("SingleQuery: %v", singleQueryDuration)
		log.Printf("Batch: %v", batchDuration)
		log.Printf("---")
	}
}
