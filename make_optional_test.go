package bq

import (
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestMakeOptional(t *testing.T) {
	got := []*bigquery.FieldSchema{
		&bigquery.FieldSchema{
			Name:     "S",
			Required: true,
			Type:     bigquery.StringFieldType,
		},
		&bigquery.FieldSchema{
			Name:     "B",
			Required: false,
			Type:     bigquery.RecordFieldType,
			Schema: []*bigquery.FieldSchema{
				&bigquery.FieldSchema{
					Name:     "I",
					Required: true,
					Type:     bigquery.IntegerFieldType,
				},
				&bigquery.FieldSchema{
					Name:     "F",
					Required: false,
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

	MakeOptional(got, "I")

	want := []*bigquery.FieldSchema{
		&bigquery.FieldSchema{
			Name:     "S",
			Required: false,
			Type:     bigquery.StringFieldType,
		},
		&bigquery.FieldSchema{
			Name:     "B",
			Required: false,
			Type:     bigquery.RecordFieldType,
			Schema: []*bigquery.FieldSchema{
				&bigquery.FieldSchema{
					Name:     "I",
					Required: true,
					Type:     bigquery.IntegerFieldType,
				},
				&bigquery.FieldSchema{
					Name:     "F",
					Required: false,
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

	if !reflect.DeepEqual(got, want) {
		t.Errorf("MakeOptional = %v; want %v", got, want)
	}

}
