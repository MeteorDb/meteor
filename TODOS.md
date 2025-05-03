## TODO

### Bihan

- [ ] Implement WAL manager
  - [ ] Finalise WAL file format (headers, rows etc)
  - [ ] Implement WAL file creation with format
  - [ ] Load WAL file with format and store headers in struct
  - [ ] Implement WAL file appending
  - [ ] Implement WAL file reading each row into struct
- [ ] Implement transaction manager
  - [ ] Implement transaction id creation
  - [ ] Implement transaction start
  - [ ] Implement transaction commit
  - [ ] Implement transaction abort
- [ ] Implement store manager
  - [ ] Implement buffer store with go map
  - [ ] Implement store manager for get, put, delete operations
- [ ] Implement snapshot manager
  - [ ] Implement snapshot creation
  - [ ] Implement snapshot loading
  - [ ] Implement snapshot deletion
