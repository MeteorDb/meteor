package store

import "meteor/internal/datatable"

type BufferStore struct {
	tableShards []datatable.DataTable
}
