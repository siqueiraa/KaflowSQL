package pipeline

import (
	"reflect"
	"testing"
)

func TestNewQueryPlan(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		expectError  bool
		expectTables []string
		expectJoins  int
	}{
		{
			name:         "simple select",
			query:        "SELECT * FROM events",
			expectError:  false,
			expectTables: []string{"events"},
			expectJoins:  0,
		},
		{
			name:         "simple join",
			query:        "SELECT e.*, u.name FROM events e LEFT JOIN users u ON e.user_id = u.user_id",
			expectError:  false,
			expectTables: []string{"events", "users"},
			expectJoins:  1,
		},
		{
			name: "multiple joins",
			query: "SELECT e.*, u.name, p.title FROM events e " +
				"LEFT JOIN users u ON e.user_id = u.user_id " +
				"LEFT JOIN products p ON e.product_id = p.product_id",
			expectError:  false,
			expectTables: []string{"events", "products", "users"}, // Alphabetical order
			expectJoins:  2,
		},
		{
			name:         "join with where clause",
			query:        "SELECT e.*, u.name FROM events e LEFT JOIN users u ON e.user_id = u.user_id WHERE e.amount > 100",
			expectError:  false,
			expectTables: []string{"events", "users"},
			expectJoins:  1,
		},
		{
			name:        "invalid sql",
			query:       "INVALID SQL QUERY",
			expectError: true,
		},
		{
			name:        "empty query",
			query:       "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := NewQueryPlan(tt.query)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for query: %s", tt.query)
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if !reflect.DeepEqual(plan.Tables, tt.expectTables) {
				t.Errorf("Tables mismatch: got %v, want %v", plan.Tables, tt.expectTables)
			}

			if len(plan.JoinClauses) != tt.expectJoins {
				t.Errorf("Join count mismatch: got %d, want %d", len(plan.JoinClauses), tt.expectJoins)
			}

			if plan.SQL == "" {
				t.Errorf("SQL should not be empty")
			}
		})
	}
}

func TestJoinClause(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		expectJoin JoinClause
	}{
		{
			name:  "simple join",
			query: "SELECT * FROM events e LEFT JOIN users u ON e.user_id = u.user_id",
			expectJoin: JoinClause{
				LeftTable:  "events",
				RightTable: "users",
				LeftKeys:   []string{"user_id"},
				RightKeys:  []string{"user_id"},
			},
		},
		{
			name:  "multiple key join",
			query: "SELECT * FROM events e LEFT JOIN sessions s ON e.user_id = s.user_id AND e.session_id = s.session_id",
			expectJoin: JoinClause{
				LeftTable:  "events",
				RightTable: "sessions",
				LeftKeys:   []string{"user_id", "session_id"},
				RightKeys:  []string{"user_id", "session_id"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := NewQueryPlan(tt.query)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(plan.JoinClauses) == 0 {
				t.Errorf("Expected at least one join clause")
				return
			}

			join := plan.JoinClauses[0]
			if join.LeftTable != tt.expectJoin.LeftTable {
				t.Errorf("LeftTable mismatch: got %s, want %s", join.LeftTable, tt.expectJoin.LeftTable)
			}
			if join.RightTable != tt.expectJoin.RightTable {
				t.Errorf("RightTable mismatch: got %s, want %s", join.RightTable, tt.expectJoin.RightTable)
			}
			if !reflect.DeepEqual(join.LeftKeys, tt.expectJoin.LeftKeys) {
				t.Errorf("LeftKeys mismatch: got %v, want %v", join.LeftKeys, tt.expectJoin.LeftKeys)
			}
			if !reflect.DeepEqual(join.RightKeys, tt.expectJoin.RightKeys) {
				t.Errorf("RightKeys mismatch: got %v, want %v", join.RightKeys, tt.expectJoin.RightKeys)
			}
		})
	}
}

func TestQueryPlanEdgeCases(t *testing.T) {
	t.Run("NoFromClause", func(t *testing.T) {
		_, err := NewQueryPlan("SELECT 1")
		if err == nil {
			t.Errorf("Expected error for query without FROM clause")
		}
	})

	t.Run("SelfJoin", func(t *testing.T) {
		query := "SELECT e1.*, e2.* FROM events e1 LEFT JOIN events e2 ON e1.user_id = e2.parent_user_id"
		plan, err := NewQueryPlan(query)
		if err != nil {
			t.Errorf("Unexpected error for self join: %v", err)
			return
		}

		if len(plan.Tables) != 1 {
			t.Errorf("Self join should result in one unique table, got %d", len(plan.Tables))
		}

		if plan.Tables[0] != "events" {
			t.Errorf("Expected 'events' table, got %s", plan.Tables[0])
		}
	})

	t.Run("ComplexWhere", func(t *testing.T) {
		query := "SELECT * FROM events e LEFT JOIN users u ON e.user_id = u.user_id " +
			"WHERE e.amount > 100 AND u.status = 'active' ORDER BY e.timestamp DESC"
		plan, err := NewQueryPlan(query)
		if err != nil {
			t.Errorf("Unexpected error for complex query: %v", err)
			return
		}

		expectedTables := []string{"events", "users"}
		if !reflect.DeepEqual(plan.Tables, expectedTables) {
			t.Errorf("Tables mismatch: got %v, want %v", plan.Tables, expectedTables)
		}
	})

	t.Run("NestedQuery", func(t *testing.T) {
		query := "SELECT * FROM (SELECT user_id, amount FROM events WHERE amount > 100) e LEFT JOIN users u ON e.user_id = u.user_id"
		_, err := NewQueryPlan(query)
		// This might or might not be supported depending on implementation
		// Just ensure it doesn't panic
		if err != nil {
			t.Logf("Nested query not supported (expected): %v", err)
		}
	})
}

func TestQueryPlanValidation(t *testing.T) {
	t.Run("ValidPlan", func(t *testing.T) {
		plan := QueryPlan{
			SQL:         "SELECT * FROM events",
			Tables:      []string{"events"},
			JoinClauses: []JoinClause{},
		}

		if !isValidQueryPlan(plan) {
			t.Errorf("Valid plan should pass validation")
		}
	})

	t.Run("InvalidPlanEmptySQL", func(t *testing.T) {
		plan := QueryPlan{
			SQL:         "",
			Tables:      []string{"events"},
			JoinClauses: []JoinClause{},
		}

		if isValidQueryPlan(plan) {
			t.Errorf("Plan with empty SQL should fail validation")
		}
	})

	t.Run("InvalidPlanNoTables", func(t *testing.T) {
		plan := QueryPlan{
			SQL:         "SELECT * FROM events",
			Tables:      []string{},
			JoinClauses: []JoinClause{},
		}

		if isValidQueryPlan(plan) {
			t.Errorf("Plan with no tables should fail validation")
		}
	})
}

// Helper function for validation (would be implemented in the actual code)
func isValidQueryPlan(plan QueryPlan) bool {
	if plan.SQL == "" {
		return false
	}
	if len(plan.Tables) == 0 {
		return false
	}
	return true
}

func TestJoinClauseValidation(t *testing.T) {
	tests := []struct {
		name        string
		join        JoinClause
		expectValid bool
	}{
		{
			name: "valid join",
			join: JoinClause{
				LeftTable:  "events",
				RightTable: "users",
				LeftKeys:   []string{"user_id"},
				RightKeys:  []string{"user_id"},
			},
			expectValid: true,
		},
		{
			name: "missing left table",
			join: JoinClause{
				LeftTable:  "",
				RightTable: "users",
				LeftKeys:   []string{"user_id"},
				RightKeys:  []string{"user_id"},
			},
			expectValid: false,
		},
		{
			name: "missing right table",
			join: JoinClause{
				LeftTable:  "events",
				RightTable: "",
				LeftKeys:   []string{"user_id"},
				RightKeys:  []string{"user_id"},
			},
			expectValid: false,
		},
		{
			name: "mismatched key counts",
			join: JoinClause{
				LeftTable:  "events",
				RightTable: "users",
				LeftKeys:   []string{"user_id", "session_id"},
				RightKeys:  []string{"user_id"},
			},
			expectValid: false,
		},
		{
			name: "empty keys",
			join: JoinClause{
				LeftTable:  "events",
				RightTable: "users",
				LeftKeys:   []string{},
				RightKeys:  []string{},
			},
			expectValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := isValidJoinClause(&tt.join)
			if valid != tt.expectValid {
				t.Errorf("Join validation mismatch: got %v, want %v", valid, tt.expectValid)
			}
		})
	}
}

// Helper function for join validation (would be implemented in the actual code)
func isValidJoinClause(join *JoinClause) bool {
	if join.LeftTable == "" || join.RightTable == "" {
		return false
	}
	if len(join.LeftKeys) == 0 || len(join.RightKeys) == 0 {
		return false
	}
	if len(join.LeftKeys) != len(join.RightKeys) {
		return false
	}
	return true
}

func BenchmarkQueryPlanParsing(b *testing.B) {
	query := "SELECT e.user_id, e.event_type, e.timestamp, u.name, u.email, p.title, p.price " +
		"FROM events e LEFT JOIN users u ON e.user_id = u.user_id " +
		"LEFT JOIN products p ON e.product_id = p.product_id " +
		"WHERE e.amount > 100 AND u.status = 'active'"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewQueryPlan(query)
		if err != nil {
			b.Errorf("Query parsing failed: %v", err)
		}
	}
}
