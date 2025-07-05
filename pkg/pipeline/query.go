// Package pipeline implements SQL parsing utilities that discover tables and JOIN
// relationships in incoming SQL statements.
package pipeline

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"slices"

	pg "github.com/pganalyze/pg_query_go/v6"
)

const (
	qualifiedColumnFields = 2 // Number of fields in a qualified column reference (table.column)
)

// ============================================================================
// Public data‑structures
// ============================================================================

type QueryPlan struct {
	SQL         string
	Tables      []string
	JoinClauses []JoinClause
	JoinGraph   map[string][]string
}

type JoinClause struct {
	LeftTable  string
	RightTable string
	LeftKeys   []string
	RightKeys  []string
}

// ============================================================================
// Quoting helpers (identical to Vitess version)
// ============================================================================

var re = regexp.MustCompile(`(?i)\b(FROM|JOIN)\s+([A-Za-z0-9_.]+)`)

// func quoteTableNames(sql string) string   { return re.ReplaceAllString(sql, "$1 `"+`$2`+"`") }
func quoteTableNamesPG(sql string) string { return re.ReplaceAllString(sql, `$1 "$2"`) }

// ============================================================================
// Entry point
// ============================================================================

func NewQueryPlan(sql string) (*QueryPlan, error) {
	// Validate input
	if strings.TrimSpace(sql) == "" {
		return nil, fmt.Errorf("empty query not allowed")
	}

	backticked := quoteTableNamesPG(sql)
	parsedSQL := quoteTableNamesPG(sql)

	tree, err := pg.Parse(parsedSQL)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	tables, raw := extractFromAST(tree)

	// Validate that we have tables (FROM clause is required for streaming queries)
	if len(tables) == 0 {
		return nil, fmt.Errorf("query must have a FROM clause with at least one table")
	}

	clauses := CollapseSameKeyClauses(raw)
	graph := BuildJoinGraph(clauses)

	return &QueryPlan{SQL: backticked, Tables: tables, JoinClauses: clauses, JoinGraph: graph}, nil
}

// ============================================================================
// AST helpers (manual traversal)
// ============================================================================

func extractFromAST(tree *pg.ParseResult) ([]string, []JoinClause) {
	seenTables := map[string]struct{}{}
	aliasToTable := map[string]string{}
	tables := []string{}
	joins := []JoinClause{}

	// helper to guarantee uniqueness and deterministic order
	addTable := func(name string) {
		if _, ok := seenTables[name]; !ok {
			seenTables[name] = struct{}{}
			tables = append(tables, name)
		}
	}

	// top‑level statements
	for _, raw := range tree.GetStmts() {
		stmt := raw.GetStmt()
		if sel := stmt.GetSelectStmt(); sel != nil {
			// walk FROM clause recursively
			walkFromClause(sel.GetFromClause(), aliasToTable, addTable, &joins)
			// CTEs, sub‑selects, etc. not needed for our use‑case
		}
	}

	sort.Strings(tables)
	return tables, joins
}

// walkFromClause walks a list of pg.Node that appear in a SELECT's FROM clause.
func walkFromClause(list []*pg.Node, aliasTo map[string]string, addTbl func(string), joins *[]JoinClause) {
	for _, n := range list {
		if rv := n.GetRangeVar(); rv != nil {
			// simple table reference
			tableName := rv.GetRelname()
			addTbl(tableName)

			alias := tableName
			if rv.GetAlias() != nil && rv.GetAlias().GetAliasname() != "" {
				alias = rv.GetAlias().GetAliasname()
			}
			aliasTo[alias] = tableName
		} else if j := n.GetJoinExpr(); j != nil {
			// JOIN – recurse on both sides first
			if j.GetLarg() != nil {
				walkFromClause([]*pg.Node{j.GetLarg()}, aliasTo, addTbl, joins)
			}
			if j.GetRarg() != nil {
				walkFromClause([]*pg.Node{j.GetRarg()}, aliasTo, addTbl, joins)
			}
			// then analyze predicates
			extractJoinFromJoinExpr(j, aliasTo, joins)
		}
	}
}

func extractJoinFromJoinExpr(j *pg.JoinExpr, aliasTo map[string]string, out *[]JoinClause) {
	if j.GetQuals() == nil {
		return // NATURAL JOIN etc.
	}

	var jc JoinClause

	// Quals may be nested boolean expressions; we only care about top‑level ANDs
	scanQuals(j.GetQuals(), aliasTo, &jc)

	if jc.LeftTable != "" && jc.RightTable != "" {
		*out = append(*out, jc)
	}
}

// scanQuals drills into expressions and collects equality predicates.
func scanQuals(node *pg.Node, aliasTo map[string]string, jc *JoinClause) {
	if node == nil {
		return
	}

	if boolExp := node.GetBoolExpr(); boolExp != nil {
		for _, arg := range boolExp.GetArgs() {
			scanQuals(arg, aliasTo, jc)
		}
		return
	}

	if aexpr := node.GetAExpr(); aexpr != nil {
		// operator must be '='
		if len(aexpr.GetName()) == 0 || aexpr.GetName()[0].GetString_().GetSval() != "=" {
			return
		}

		lcol := aexpr.GetLexpr().GetColumnRef()
		rcol := aexpr.GetRexpr().GetColumnRef()
		if lcol == nil || rcol == nil {
			return
		}

		aliasL, colL := splitColumnRef(lcol)
		aliasR, colR := splitColumnRef(rcol)
		tblL, okL := aliasTo[aliasL]
		tblR, okR := aliasTo[aliasR]
		if !okL || !okR {
			return
		}

		if jc.LeftTable == "" {
			jc.LeftTable, jc.RightTable = tblL, tblR
		}
		jc.LeftKeys = appendIfMissing(jc.LeftKeys, colL)
		jc.RightKeys = appendIfMissing(jc.RightKeys, colR)
	}
}

func splitColumnRef(cr *pg.ColumnRef) (alias, col string) {
	fields := cr.GetFields()
	if len(fields) == qualifiedColumnFields {
		alias = fields[0].GetString_().GetSval()
		col = fields[1].GetString_().GetSval()
	} else if len(fields) == 1 {
		col = fields[0].GetString_().GetSval()
	}
	return
}

// ============================================================================
// Legacy helper exported for old callers
// ============================================================================

func ExtractJoinClauses(sql string) ([]JoinClause, error) {
	tree, err := pg.Parse(quoteTableNamesPG(sql))
	if err != nil {
		return nil, err
	}
	_, raw := extractFromAST(tree)
	return raw, nil
}

// ============================================================================
// Original graph/key utilities
// ============================================================================

func BuildJoinGraph(clauses []JoinClause) map[string][]string {
	tmp := map[string]map[string]struct{}{}
	add := func(a, b string) {
		if tmp[a] == nil {
			tmp[a] = map[string]struct{}{}
		}
		tmp[a][b] = struct{}{}
	}
	for _, jc := range clauses {
		for _, rt := range strings.Split(jc.RightTable, ",") {
			add(jc.LeftTable, rt)
			add(rt, jc.LeftTable)
		}
	}
	graph := make(map[string][]string, len(tmp))
	for k, v := range tmp {
		ns := make([]string, 0, len(v))
		for n := range v {
			ns = append(ns, n)
		}
		sort.Strings(ns)
		graph[k] = ns
	}
	return graph
}

func CollapseSameKeyClauses(cs []JoinClause) []JoinClause {
	m := map[string]JoinClause{}
	for _, c := range cs {
		key := c.LeftTable + "|" + strings.Join(c.LeftKeys, "|")
		if agg, ok := m[key]; !ok {
			m[key] = c
		} else {
			agg.RightTable = strings.Join(appendIfMissing(strings.Split(agg.RightTable, ","), c.RightTable), ",")
			for _, rk := range c.RightKeys {
				agg.RightKeys = appendIfMissing(agg.RightKeys, rk)
			}
			m[key] = agg
		}
	}
	out := make([]JoinClause, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LeftTable < out[j].LeftTable })
	return out
}

func appendIfMissing(slice []string, val string) []string {
	if slices.Contains(slice, val) {
		return slice
	}
	return append(slice, val)
}

func BuildJoinKey(event map[string]any, cols []string) string {
	var b strings.Builder
	for i, c := range cols {
		v, ok := event[c]
		if !ok {
			return ""
		}
		if i > 0 {
			b.WriteByte('|')
		}
		fmt.Fprint(&b, v)
	}
	return b.String()
}
