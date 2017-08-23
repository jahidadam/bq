package bq

import (
	"context"
	"fmt"
	"reflect"

	"cloud.google.com/go/bigquery"
)

// InsertStructs uses the given bigquery client to insert the slice of
// uniformly-typed structs to the given table in the given dataset. If the
// dataset and/or table are missing, then they will be created. The ider
// function must take a struct from the passed slice of structs, and return a
// string that uniquely identifies that element. The ID is used for
// de-duplication.
func InsertStructs(ctx context.Context, client *bigquery.Client, dataset, table string, ider interface{}, structs interface{}) error {
	rv := reflect.ValueOf(structs)
	if rv.Kind() != reflect.Slice {
		return fmt.Errorf("InsertStructs can insert slice of structs; got %v", rv.Type())
	}
	fv := reflect.ValueOf(ider)
	if fv.Kind() != reflect.Func || fv.Type().NumIn() != 1 || fv.Type().NumOut() != 1 || fv.Type().Out(0) != reflect.TypeOf("") ||
		(fv.Type().In(0) != rv.Elem().Type() &&
			(fv.Type().In(0).Kind() == reflect.Interface &&
				!rv.Elem().Type().Implements(fv.Type().In(0)))) {
		return fmt.Errorf("InsertStructs requires an ider that takes %v and returns string; got %v", rv.Elem().Type(), fv.Type())
	}

	if rv.Len() == 0 {
		return nil
	}
	first := rv.Index(0).Interface()
	schema, err := bigquery.InferSchema(first)
	if err != nil {
		return err
	}
	MakeOptional(schema)

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
		id := fv.Call([]reflect.Value{rv.Index(i)})[0].Interface().(string)

		savers = append(savers, &bigquery.StructSaver{
			Struct:   val,
			Schema:   schema,
			InsertID: id,
		})
	}
	return ul.Put(ctx, savers)
}
