package store

import "meteor/internal/datatable"

type ImmutableStore struct {
	table datatable.DataTable
}
