package main

import (
	"os"
	"fmt"
	"bufio"
	"encoding/binary"
	"io"
	"sort"
	"SortPro/lib"
	"time"
)
var StartTime time.Time
func init(){
	StartTime = time.Now()
}
/**type MultiReader interface{
	ReaderFrom(rc []chan int)
}*/
type ReaderN struct{
	n int
	filename string
	size int
}

func (r *ReaderN) ReaderFrom(rc []chan int){
	chunkSize := r.size / r.n
	for j := 0 ; j< r.n; j++ {
		file, err := os.Open(r.filename)
		if err != nil {
			panic(fmt.Sprintf("ReaderFrom error : %s ", err.Error()))
		}
		file.Seek(int64(chunkSize*j*8), 0)
		f := bufio.NewReader(file)
		go func(f *bufio.Reader, j int) {
			b := make([]byte, 8)

			for i := 0; i < chunkSize; i++ {
				_, err = f.Read(b)
				if err != nil {
					if err == io.EOF {
						break
					}
					panic(fmt.Sprintf("ReaderFrom chunsize: %s", err.Error()))
				}

				rc[j]  <- int(binary.BigEndian.Uint64(b))
			}
			fmt.Printf("Read done : %s \n", time.Now().Sub(StartTime))
			close(rc[j])


		}(f,j)
	}
}


/**type InternalSorter interface{
	SortsN(in []<-chan int, out []chan<- int)
}*/
type SorterN struct {
	n int
}

func (s *SorterN) SortsN(in []chan int, out []chan int){
	for i := 0; i < s.n; i++{
		go func(i int){
			a := make([]int,0)
			fmt.Println("start SortsN : i ",i)
			for v := range in[i]{
				a = append(a, v)
			}

			sort.Ints(a)

			for _,e := range a{
				out[i] <- e
			}
			close(out[i])

		}(i)
	}
}

/**type ExternalSorterN interface{
	MergeN(in []<-chan int, out chan<- int)
}*/
type MergerN struct{
	n int
}

func (m *MergerN) MergeN(in []chan int, out chan<- int){
	a := mergeN(in...)

	go func() {

		for v := range a {

			out <- v

		}
		close(out)
	}()
}

func mergeN(in ...chan int) chan int{
	if len(in) == 1{
		return in[0]
	}

	n := len(in)/2
	return merge(mergeN(in[:n]...),mergeN(in[n:]...))
}

func merge(a,b chan int) chan int {
	a1, ok1 := <-a
	b1, ok2 := <-b
	out := make(chan int, 1024)
	go func() {
		for {
			if ok1 && a1 <= b1 || (ok1 && !ok2) {

				out <- a1
				a1, ok1 = <-a
			} else {
				if !ok1 && !ok2 {

					break
				} else {

					out <- b1
					b1, ok2 = <-b
				}
			}
		}
		close(out)

		fmt.Printf("merge done ,cost time: %s", time.Now().Sub(StartTime))

	}()
	return out
}


/**type Writer interface {
	Write(in  <-chan int )
}*/
type WriterTo struct{
	filename string
}

func (w *WriterTo)Write(in <-chan int){
	file, err := os.Create(w.filename)
	defer file.Close()
	if err != nil {
		panic(fmt.Sprintf("Write error : %s", err.Error()))
	}
	flag := make(chan struct{})
	go func(){
		f := bufio.NewWriter(file)
		defer f.Flush()
		b := make([]byte, 8)
		fmt.Println("start to write data into data.out")

		count := 0
		for v := range in{

			count ++
			if count < 30{
				fmt.Println(v)
			}
			binary.BigEndian.PutUint64(b,uint64(v))
			_,err = f.Write(b)
			if err!=nil {
				panic(fmt.Sprintf("Write error : %s ", err.Error()))
			}
		}
		flag <- struct{}{}
	}()
	<- flag
	fmt.Println("cost time : %s", time.Now().Sub(StartTime))
}


type dataProcess struct {
	reader lib.MultiReader
	interSorter lib.InternalSorter
	exterSorter lib.ExternalSorterN
	writer lib.Writer
	rc []chan int
	interSortOut []chan int
	exterSortOut chan int
}

func main() {
	filename := "data.in"
	n := 4
	size := 80000000
	reader := ReaderN{n,filename,size}
	interSorter := SorterN{n}
	exterSorter := MergerN{n}
	filenameOut := "data.out"
	writer := WriterTo{filenameOut}


	rc := make([]chan int, n)
	interSortOut := make([]chan int, n)
	for i:=0; i< n; i++{
		rc[i] = make(chan int, 1024)
		interSortOut[i] = make(chan int, 1024)
	}
	exterSortOut := make(chan int, 1024)

	dp := dataProcess{&reader,&interSorter, &exterSorter, &writer, rc, interSortOut, exterSortOut}
	dp.reader.ReaderFrom(dp.rc)
	dp.interSorter.SortsN(dp.rc,dp.interSortOut)
	dp.exterSorter.MergeN(dp.interSortOut,dp.exterSortOut)


	dp.writer.Write(dp.exterSortOut)


}
