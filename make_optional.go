package bq

import (
	"strings"

	"cloud.google.com/go/bigquery"
)

// MakeOptional transforms the given schema, marking all fields as optional
// except for the given field names, which will be left untouched. Names are
// compared ignoring case.
func MakeOptional(schema bigquery.Schema, except ...string) {
	set := make(map[string]struct{})
	for _, k := range except {
		set[strings.ToLower(k)] = struct{}{}
	}
	stack := []bigquery.Schema{schema}
	for len(stack) > 0 {
		pop := stack[len(stack)-1]
		stack = stack[:(len(stack) - 1)]
		for i := range pop {
			_, ok := set[strings.ToLower(pop[i].Name)]
			pop[i].Required = ok && pop[i].Required
			if pop[i].Schema != nil {
				stack = append(stack, pop[i].Schema)
			}
		}
	}
}
