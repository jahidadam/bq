package bq

import (
	"context"
	"fmt"
	"reflect"

	"cloud.google.com/go/bigquery"
	"github.com/ggqq/reflection"
)

// InsertStructs uses the given bigquery client to insert the slice of
// uniformly-typed structs to the given table in the given dataset. The field
// corresponding to the given ID is used to de-duplicate insertions. If the
// dataset and/or table are missing, then they will be created.
func InsertStructs(ctx context.Context, client *bigquery.Client, dataset, table, id string, structs interface{}) error {
	rv := reflect.ValueOf(structs)
	if rv.Kind() != reflect.Slice {
		return fmt.Errorf("InsertStructs can insert slice of structs; got %v", rv.Type())
	}
	if rv.Len() == 0 {
		return nil
	}
	first := rv.Index(0).Interface()
	schema, err := bigquery.InferSchema(first)
	if err != nil {
		return err
	}
	MakeOptional(schema, id)

	ds := client.Dataset(dataset)
	ds.Create(ctx) // Ignore error, which is returned if the dataset already exists.
	t := ds.Table(table)
	t.Create(ctx, bigquery.UseStandardSQL()) // Ignore error, which is returned if the table already exists.
	md, err := t.Metadata(ctx)
	if err != nil {
		return err
	}
	schema = MergeSchemas(md.Schema, schema)
	tm := bigquery.TableMetadataToUpdate{
		Schema: schema,
	}
	_, err = t.Update(ctx, tm, "")
	if err != nil {
		return err
	}
	ul := t.Uploader()
	savers := []*bigquery.StructSaver{}
	for i := 0; i < rv.Len(); i++ {
		val := rv.Index(i).Interface()
		insertIDGeneric, ok := reflection.StructFieldIgnoreCase(val, id)
		if !ok {
			return fmt.Errorf("InsertStructs: value %v does not have field %s", val, id)
		}
		insertID, ok := insertIDGeneric.(string)
		if !ok {
			return fmt.Errorf("InsertStructs: value %v does not have string field %s", val, id)
		}
		savers = append(savers, &bigquery.StructSaver{
			Struct:   val,
			Schema:   schema,
			InsertID: insertID,
		})
	}
	return ul.Put(ctx, savers)
}
