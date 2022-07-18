package iterator

type Iterator[T any] interface {
	Iter() (T, error)
	HasElement() (bool, error)
}
