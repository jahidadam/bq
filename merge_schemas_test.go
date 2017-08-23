package bq

import (
	"encoding/json"
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestMergeSchemas(t *testing.T) {
	type BarLeft struct {
		I int
	}
	type FooLeft struct {
		S string
		B BarLeft
	}

	type BarRight struct {
		I int
		F float32
	}
	type FooRight struct {
		S string
		I int
		B BarRight
	}

	left, err := bigquery.InferSchema(&FooLeft{})
	if err != nil {
		t.Fatal(err)
	}
	right, err := bigquery.InferSchema(&FooRight{})
	if err != nil {
		t.Fatal(err)
	}
	merged := MergeSchemas(left, right)
	want := []*bigquery.FieldSchema{
		&bigquery.FieldSchema{
			Name:     "S",
			Required: true,
			Type:     bigquery.StringFieldType,
		},
		&bigquery.FieldSchema{
			Name:     "B",
			Required: true,
			Type:     bigquery.RecordFieldType,
			Schema: []*bigquery.FieldSchema{
				&bigquery.FieldSchema{
					Name:     "I",
					Required: true,
					Type:     bigquery.IntegerFieldType,
				},
				&bigquery.FieldSchema{
					Name:     "F",
					Required: true,
					Type:     bigquery.FloatFieldType,
				},
			},
		},
		&bigquery.FieldSchema{
			Name:     "I",
			Required: true,
			Type:     bigquery.IntegerFieldType,
		},
	}
	ms, _ := json.MarshalIndent(merged, "", "  ")
	ws, _ := json.MarshalIndent(want, "", "  ")
	if string(ms) != string(ws) {
		t.Errorf("MergeSchemas = %s; want %s", ms, ws)
	}
}
