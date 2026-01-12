package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
	"github.com/schollz/progressbar/v3"
)

func main() {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, relying on environment variables")
	}

	sourceURL := os.Getenv("XATA_DATABASE_URL")
	destURL := os.Getenv("DATABASE_URL")

	if sourceURL == "" {
		log.Fatal("XATA_DATABASE_URL is not set")
	}
	if destURL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	ctx := context.Background()

	// Connect to Source (Xata)
	fmt.Println("Connecting to Source (Xata)...")
	sourceConn, err := pgx.Connect(ctx, sourceURL)
	if err != nil {
		log.Fatalf("Unable to connect to source database: %v", err)
	}
	defer sourceConn.Close(ctx)
	fmt.Println("Connected to Source.")

	// Connect to Destination (Postgres)
	fmt.Println("Connecting to Destination (Postgres)...")
	destConn, err := pgx.Connect(ctx, destURL)
	if err != nil {
		log.Fatalf("Unable to connect to destination database: %v", err)
	}
	defer destConn.Close(ctx)
	fmt.Println("Connected to Destination.")

	// Run migration
	if err := migrate(ctx, sourceConn, destConn); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	fmt.Println("Migration completed successfully!")
}

type Column struct {
	Name       string
	DataType   string
	IsNullable string
	Default    *string
}

type Table struct {
	Name       string
	Columns    []Column
	PrimaryKey []string
}

func migrate(ctx context.Context, source, dest *pgx.Conn) error {
	fmt.Println("Introspecting schema...")
	tables, err := introspectSchema(ctx, source)
	if err != nil {
		return fmt.Errorf("failed to introspect schema: %w", err)
	}
	fmt.Printf("Found %d tables.\n", len(tables))

	fmt.Println("Creating schema on destination...")
	if err := createSchema(ctx, dest, tables); err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}
	fmt.Println("Schema created.")

	fmt.Println("Starting data transfer...")
	if err := copyData(ctx, source, dest, tables); err != nil {
		return fmt.Errorf("failed to copy data: %w", err)
	}

	return nil
}

func introspectSchema(ctx context.Context, conn *pgx.Conn) ([]Table, error) {
	// 1. Get Tables
	rows, err := conn.Query(ctx, `
		SELECT tablename 
		FROM pg_catalog.pg_tables 
		WHERE schemaname = 'public' 
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []Table
	for rows.Next() {
		var t Table
		if err := rows.Scan(&t.Name); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	rows.Close()

	// 2. Get Columns and PK for each table
	for i := range tables {
		t := &tables[i]

		// Columns
		// Use pg_catalog to get the correct type definition (e.g. text[] instead of ARRAY)
		cRows, err := conn.Query(ctx, `
			SELECT 
				a.attname, 
				format_type(a.atttypid, a.atttypmod), 
				a.attnotnull, 
				pg_get_expr(d.adbin, d.adrelid)
			FROM pg_attribute a
			JOIN pg_class c ON a.attrelid = c.oid
			JOIN pg_namespace n ON c.relnamespace = n.oid
			LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
			WHERE n.nspname = 'public' 
			  AND c.relname = $1
			  AND a.attnum > 0 
			  AND NOT a.attisdropped
			ORDER BY a.attnum
		`, t.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get columns for table %s: %w", t.Name, err)
		}

		for cRows.Next() {
			var c Column
			var notNull bool
			if err := cRows.Scan(&c.Name, &c.DataType, &notNull, &c.Default); err != nil {
				cRows.Close()
				return nil, err
			}

			if notNull {
				c.IsNullable = "NO"
			} else {
				c.IsNullable = "YES"
			}

			// Sanitize Xata specifics
			// 1. Remove defaults that refer to xata_private schema
			if c.Default != nil && (contains(*c.Default, "xata_private") || contains(*c.Default, "::xata_")) {
				c.Default = nil
			}

			// 3. Handle Sequences (nextval)
			if c.Default != nil && contains(*c.Default, "nextval(") {
				// With pg_catalog, format_type should return proper types like 'integer' or 'bigint' or 'text[]'
				// But we still want to convert auto-incrementing ints to SERIAL for simplicity on destination.
				if strings.HasPrefix(c.DataType, "integer") || c.DataType == "int4" {
					c.DataType = "SERIAL"
					c.Default = nil
				} else if strings.HasPrefix(c.DataType, "bigint") || c.DataType == "int8" {
					c.DataType = "BIGSERIAL"
					c.Default = nil
				}
			}

			t.Columns = append(t.Columns, c)
		}
		cRows.Close()

		// Primary Keys
		pkRows, err := conn.Query(ctx, `
			SELECT kcu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
			  ON tc.constraint_name = kcu.constraint_name
			  AND tc.table_schema = kcu.table_schema
			WHERE tc.constraint_type = 'PRIMARY KEY'
			  AND tc.table_schema = 'public'
			  AND tc.table_name = $1
			ORDER BY kcu.ordinal_position
		`, t.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get PK for table %s: %w", t.Name, err)
		}

		for pkRows.Next() {
			var pkCol string
			if err := pkRows.Scan(&pkCol); err != nil {
				pkRows.Close()
				return nil, err
			}
			t.PrimaryKey = append(t.PrimaryKey, pkCol)
		}
		pkRows.Close()
	}

	return tables, nil
}

func createSchema(ctx context.Context, conn *pgx.Conn, tables []Table) error {
	for _, t := range tables {
		// Drop existing table
		_, err := conn.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS "%s" CASCADE`, t.Name))
		if err != nil {
			return fmt.Errorf("failed to drop table %s: %w", t.Name, err)
		}

		// Build Create SQL
		sql := fmt.Sprintf(`CREATE TABLE "%s" (`, t.Name)
		for i, c := range t.Columns {
			sql += fmt.Sprintf(`"%s" %s`, c.Name, c.DataType)

			if c.IsNullable == "NO" {
				sql += " NOT NULL"
			}
			if c.Default != nil {
				sql += fmt.Sprintf(" DEFAULT %s", *c.Default)
			}

			if i < len(t.Columns)-1 {
				sql += ", "
			}
		}

		if len(t.PrimaryKey) > 0 {
			sql += ", PRIMARY KEY ("
			for i, pk := range t.PrimaryKey {
				sql += fmt.Sprintf(`"%s"`, pk)
				if i < len(t.PrimaryKey)-1 {
					sql += ", "
				}
			}
			sql += ")"
		}

		sql += ")"

		_, err = conn.Exec(ctx, sql)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", t.Name, err)
		}
	}
	return nil
}

func copyData(ctx context.Context, source, dest *pgx.Conn, tables []Table) error {
	for _, t := range tables {
		fmt.Printf("Migrating table: %s\n", t.Name)

		// 1. Get row count
		var count int
		err := source.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s"`, t.Name)).Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to get count for table %s: %w", t.Name, err)
		}

		if count == 0 {
			fmt.Println("  Skipping empty table")
			continue
		}

		bar := progressbar.Default(int64(count), "  Copying")

		// 2. Select data
		// Build column list to ensure order
		colNames := make([]string, len(t.Columns))
		escapedColNames := make([]string, len(t.Columns))
		for i, c := range t.Columns {
			colNames[i] = c.Name
			escapedColNames[i] = fmt.Sprintf(`"%s"`, c.Name)
		}

		rows, err := source.Query(ctx, fmt.Sprintf(`SELECT %s FROM "%s"`,
			joinStrings(escapedColNames, ", "), t.Name))
		if err != nil {
			return fmt.Errorf("failed to query rows from %s: %w", t.Name, err)
		}

		// Wrap rows for progress
		pbRows := &ProgressBarRows{Rows: rows, Bar: bar}

		// 3. Copy to destination
		_, err = dest.CopyFrom(
			ctx,
			pgx.Identifier{t.Name},
			colNames,
			pbRows,
		)
		rows.Close() // Close original rows
		if err != nil {
			return fmt.Errorf("failed to copy data for table %s: %w", t.Name, err)
		}
		bar.Finish()
		fmt.Println()
	}
	return nil
}

type ProgressBarRows struct {
	pgx.Rows
	Bar *progressbar.ProgressBar
}

func (r *ProgressBarRows) Next() bool {
	if r.Rows.Next() {
		r.Bar.Add(1)
		return true
	}
	return false
}

func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	res := strs[0]
	for i := 1; i < len(strs); i++ {
		res += sep + strs[i]
	}
	return res
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
