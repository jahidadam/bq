package bq

import "cloud.google.com/go/bigquery"

// MergeSchemas merges two schemas by taking the union of all fields. If
// either schema marks a field as optional, then the resulting schema will mark
// the field as optional. If either schema marks a field as repeated, then the
// resulting schema will mark the field as repeated. If the two schemas are
// conflicting, then this function prefers the schema from the right hand side.
func MergeSchemas(left, right bigquery.Schema) bigquery.Schema {
	var ret bigquery.Schema
	type node struct {
		L bigquery.Schema
		R bigquery.Schema
		N *bigquery.Schema
	}
	stack := []node{{left, right, &ret}}
	for len(stack) > 0 {
		pop := stack[len(stack)-1]
		stack = stack[:(len(stack) - 1)]

		*pop.N = nil
		nameToIndex := make(map[string]int)
		for i, fs := range pop.L {
			*pop.N = append(*pop.N, fs)
			nameToIndex[fs.Name] = i
		}

		// Add all new RHS fields first. Then, pop.N will be a stable pointer to
		// a slice, so we will be able to pass the pointer reliably.
		for _, fs := range pop.R {
			_, ok := nameToIndex[fs.Name]
			if !ok {
				*pop.N = append(*pop.N, fs)
			}
		}

		for _, fs := range pop.R {
			i, ok := nameToIndex[fs.Name]
			if ok {
				// Take RHS values, and then take the more permissive union of the two schemas.
				(*pop.N)[i].Description = fs.Description
				(*pop.N)[i].Repeated = fs.Repeated || pop.L[i].Repeated
				(*pop.N)[i].Required = fs.Required && pop.L[i].Required
				(*pop.N)[i].Type = fs.Type
				stack = append(stack, node{pop.L[i].Schema, fs.Schema, &((*pop.N)[i].Schema)})
			}
		}
	}
	return ret
}
