package snapshotmanager

type Snapshot int

type SnapshotManager struct {
	activeSnapshots []Snapshot
}
