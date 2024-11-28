package sqlrunner_test

import (
	"context"
	"math/rand"
	"strconv"
	"testing"

	sqlrunner "github.com/database-playground/sqlrunner/lib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDateFunction(t *testing.T) {
	t.Parallel()

	runner, err := sqlrunner.NewSQLRunner(`
		CREATE TABLE datetest (
			date DATE
		);

		INSERT INTO datetest (date) VALUES ('2021-01-01');
		INSERT INTO datetest (date) VALUES ('2021-02-01 00:00:00');
	`)
	require.NoError(t, err)

	t.Run("YEAR", func(t *testing.T) {
		t.Parallel()

		result, err := runner.Query(context.TODO(), "SELECT YEAR(date) FROM datetest")
		require.NoError(t, err)

		assert.Len(t, result.Rows, 2)
		assert.Equal(t, []string{"YEAR(date)"}, result.Columns)
		assert.Equal(t, "2021", result.Rows[0][0])
		assert.Equal(t, "2021", result.Rows[1][0])
	})

	t.Run("MONTH", func(t *testing.T) {
		t.Parallel()

		result, err := runner.Query(context.TODO(), "SELECT MONTH(date) FROM datetest")
		require.NoError(t, err)

		assert.Len(t, result.Rows, 2)
		assert.Equal(t, []string{"MONTH(date)"}, result.Columns)
		assert.Equal(t, "1", result.Rows[0][0])
		assert.Equal(t, "2", result.Rows[1][0])
	})

	t.Run("DAY", func(t *testing.T) {
		t.Parallel()

		result, err := runner.Query(context.TODO(), "SELECT DAY(date) FROM datetest")
		require.NoError(t, err)

		assert.Len(t, result.Rows, 2)
		assert.Equal(t, []string{"DAY(date)"}, result.Columns)
		assert.Equal(t, "1", result.Rows[0][0])
		assert.Equal(t, "1", result.Rows[1][0])
	})
}

func TestIfFunction(t *testing.T) {
	t.Parallel()

	runner, err := sqlrunner.NewSQLRunner(`
		CREATE TABLE iftest (
			value INT
		);

		INSERT INTO iftest (value) VALUES (1);
		INSERT INTO iftest (value) VALUES (2);
	`)
	require.NoError(t, err)

	t.Run("IF", func(t *testing.T) {
		t.Parallel()

		result, err := runner.Query(context.TODO(), "SELECT IF(value = 1, 'yes', 'no') FROM iftest")
		require.NoError(t, err)

		assert.Len(t, result.Rows, 2)
		assert.Equal(t, []string{"IF(value = 1, 'yes', 'no')"}, result.Columns)
		assert.Equal(t, "yes", result.Rows[0][0])
		assert.Equal(t, "no", result.Rows[1][0])
	})
}

func TestLeftFunction(t *testing.T) {
	t.Parallel()

	runner, err := sqlrunner.NewSQLRunner(`
		CREATE TABLE lefttest (
			value TEXT
		);

		INSERT INTO lefttest (value) VALUES ('hello');
		INSERT INTO lefttest (value) VALUES ('world');
	`)

	require.NoError(t, err)

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		result, err := runner.Query(context.TODO(), "SELECT LEFT(value, 3) FROM lefttest")
		require.NoError(t, err)

		assert.Len(t, result.Rows, 2)
		assert.Equal(t, []string{"LEFT(value, 3)"}, result.Columns)
		assert.Equal(t, "hel", result.Rows[0][0])
		assert.Equal(t, "wor", result.Rows[1][0])
	})

	t.Run("Over Length", func(t *testing.T) {
		t.Parallel()

		result, err := runner.Query(context.TODO(), "SELECT LEFT(value, 10) FROM lefttest")
		require.NoError(t, err)

		assert.Len(t, result.Rows, 2)
		assert.Equal(t, []string{"LEFT(value, 10)"}, result.Columns)
		assert.Equal(t, "hello", result.Rows[0][0])
		assert.Equal(t, "world", result.Rows[1][0])
	})

	t.Run("Negative Length", func(t *testing.T) {
		t.Parallel()

		result, err := runner.Query(context.TODO(), "SELECT LEFT(value, -1) FROM lefttest")
		require.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestNewDbrunner(t *testing.T) {
	t.Parallel()

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		_, err := sqlrunner.NewSQLRunner(`
			CREATE TABLE dbrunnertest (
				value TEXT
			);

			INSERT INTO dbrunnertest (value) VALUES ('hello');
		`)

		require.NoError(t, err)
	})

	t.Run("Invalid", func(t *testing.T) {
		t.Parallel()

		_, err := sqlrunner.NewSQLRunner(`
			CREATE TABLE dbrunnertest (
				value TEXT
			);

			INSERT INTO d:)
		`)

		require.ErrorAs(t, err, &sqlrunner.SchemaError{})
	})
}

func TestDbRunnerQuery(t *testing.T) {
	t.Parallel()

	runner, err := sqlrunner.NewSQLRunner(`
		CREATE TABLE dbquerytest (
			value TEXT
		);

		INSERT INTO dbquerytest (value) VALUES ('hello');
		INSERT INTO dbquerytest (value) VALUES ('world');
	`)
	require.NoError(t, err)

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()

		result, err := runner.Query(context.TODO(), "SELECT value FROM dbquerytest")
		require.NoError(t, err)

		assert.Len(t, result.Rows, 2)
		assert.Equal(t, []string{"value"}, result.Columns)
		assert.Equal(t, "hello", result.Rows[0][0])
		assert.Equal(t, "world", result.Rows[1][0])
	})

	t.Run("Invalid", func(t *testing.T) {
		t.Parallel()

		_, err := runner.Query(context.TODO(), "SELECT value FROM dbquerytest WHERE value = ?")
		require.ErrorAs(t, err, &sqlrunner.QueryError{})
	})
}

func TestDbRunnerQueryTimeout(t *testing.T) {
	t.Parallel()

	runner, err := sqlrunner.NewSQLRunner(`
		CREATE TABLE dbquerytest (
			value TEXT
		);

		INSERT INTO dbquerytest (value) VALUES ('hello');
		INSERT INTO dbquerytest (value) VALUES ('world');
	`)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	_, err = runner.Query(ctx, "SELECT value FROM dbquerytest")

	var queryError sqlrunner.QueryError
	require.ErrorAs(t, err, &queryError)
	assert.Equal(t, context.DeadlineExceeded, queryError.Parent)
}

func TestDbRunnerReadonly(t *testing.T) {
	t.Parallel()

	runner, err := sqlrunner.NewSQLRunner(`
		CREATE TABLE readonlytest (
			value TEXT
		);

		INSERT INTO readonlytest (value) VALUES ('hello');
		INSERT INTO readonlytest (value) VALUES ('world');
	`)
	require.NoError(t, err)

	_, err = runner.Query(context.TODO(), "INSERT INTO readonlytest (value) VALUES ('test')")
	t.Log(err)
	require.ErrorAs(t, err, &sqlrunner.QueryError{})
}

func TestDbRunnerNoScientificNotation(t *testing.T) {
	t.Parallel()

	runner, err := sqlrunner.NewSQLRunner(`
		CREATE TABLE notationtest (
			value REAL
		);

		INSERT INTO notationtest (value) VALUES (1.0);
		INSERT INTO notationtest (value) VALUES (1145141919.810)
	`)

	require.NoError(t, err)

	result, err := runner.Query(context.TODO(), "SELECT value FROM notationtest")
	require.NoError(t, err)

	assert.Len(t, result.Rows, 2)
	assert.Equal(t, []string{"value"}, result.Columns)
	assert.Equal(t, "1", result.Rows[0][0])
	assert.Equal(t, "1145141919.81", result.Rows[1][0])
}

func TestDbRunnerEmptyQuery(t *testing.T) {
	t.Parallel()

	runner, err := sqlrunner.NewSQLRunner(`
		CREATE TABLE emptytest (
			value TEXT
		);

		INSERT INTO emptytest (value) VALUES ('hello');
		INSERT INTO emptytest (value) VALUES ('world');
	`)
	require.NoError(t, err)

	result, err := runner.Query(context.TODO(), "")
	require.NoError(t, err)

	assert.Len(t, result.Rows, 0)
	assert.Len(t, result.Columns, 0)
}

func BenchmarkDbrunner(b *testing.B) {
	b.ReportAllocs()

	b.Run("Query on same instance", func(b *testing.B) {
		runner, err := sqlrunner.NewSQLRunner(`
		CREATE TABLE benchtest (
			value TEXT
		);

		INSERT INTO benchtest (value) VALUES ('hello');
		INSERT INTO benchtest (value) VALUES ('world');
	`)
		require.NoError(b, err)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = runner.Query(context.TODO(), "SELECT value FROM benchtest")
		}
	})

	b.Run("Query on same instance, different query", func(b *testing.B) {
		runner, err := sqlrunner.NewSQLRunner(`
		CREATE TABLE benchtest (
			value TEXT
		);

		INSERT INTO benchtest (value) VALUES ('hello');
		INSERT INTO benchtest (value) VALUES ('world');
	`)
		require.NoError(b, err)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = runner.Query(context.TODO(), "SELECT value FROM benchtest WHERE value != '"+strconv.FormatInt(int64(i), 10)+"'")
		}
	})

	b.Run("Query on different instance, same schema", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			runner, err := sqlrunner.NewSQLRunner(`
			CREATE TABLE benchtest (
				value TEXT
			);

			INSERT INTO benchtest (value) VALUES ('hello');
			INSERT INTO benchtest (value) VALUES ('world');
		`)
			require.NoError(b, err)

			_, _ = runner.Query(context.TODO(), "SELECT value FROM benchtest")
		}
	})

	b.Run("Query on different instance, different schema", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// generate a unique schema for each iteration
			t := strconv.FormatInt(int64(rand.Int()*i), 10)

			runner, err := sqlrunner.NewSQLRunner(`
			CREATE TABLE benchtest (
				value TEXT
			);

			INSERT INTO benchtest (value) VALUES ('` + t + `');
		`)
			require.NoError(b, err)

			_, _ = runner.Query(context.TODO(), "SELECT value FROM benchtest")
		}
	})
}
