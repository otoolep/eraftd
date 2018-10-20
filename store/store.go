package store

type Store struct {
}

func New() *Store {
	return &Store{}
}

func (s *Store) Open() error {
	return nil
}

func (s *Store) Close() error {
	return nil
}

func (s *Store) Get(key string) (string, error) {
	return "", nil
}

func (s *Store) Set(key, value string) error {
	return nil
}

func (s *Store) Delete(key string) error {
	return nil
}
