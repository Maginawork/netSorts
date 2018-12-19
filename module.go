package lib

type MultiReader interface{
	ReaderFrom(rc []chan int)
}

type InternalSorter interface{
	SortsN(in []chan int, out []chan int)
}



type ExternalSorterN interface{
	MergeN(in []chan int, out chan<- int)
}

type Writer interface {
	Write(in  <-chan int )
}


