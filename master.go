package main

import (
	"fmt"
	"os"
	"bufio"
	"encoding/binary"
	"net"
	"io"
	"time"
	"strconv"
	"runtime"
)

/**type MultiReader interface{
	ReaderFrom(rc []chan int)
}*/
var StartTime time.Time
var log func(n int)
func init(){
	StartTime = time.Now()
	log = getLog()
}
type ReaderN struct{
	n int
	filename string
	size int
}

func (r *ReaderN) ReaderFrom(rc []chan int){
	chunkSize := r.size / r.n + 1
	for j := 0 ; j< r.n; j++ {
		file, err := os.Open(r.filename)
		if err != nil {
			panic(fmt.Sprintf("ReaderFrom error : %s ", err.Error()))
		}
		file.Seek(int64(chunkSize*j*8), 0)        //一定不能忘了乘以8
		f := bufio.NewReader(file)
		count := 0
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
				c := int(binary.BigEndian.Uint64(b))

				rc[j]  <- c
				count ++
			}
			//fmt.Printf("Read done : %s \n", time.Now().Sub(StartTime))
			close(rc[j])
			fmt.Println("shhhhssss: total number:",count)


		}(f,j)

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

		//fmt.Printf("merge done ,cost time: %s", time.Now().Sub(StartTime))

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
		defer func() {
			flag <- struct{}{}
		}()
		f := bufio.NewWriter(file)
		defer f.Flush()
		b := make([]byte, 8)
		fmt.Println("start to write data into data.out")

		count := 0
		for v := range in{

			count ++
			if count%10000 == 0{
				fmt.Println(v)
			}
			binary.BigEndian.PutUint64(b,uint64(v))
			_,err = f.Write(b)
			if err!=nil {
				panic(fmt.Sprintf("Write error : %s ", err.Error()))
			}
		}

	}()
	<- flag

	fmt.Println("cost time : %s", time.Now().Sub(StartTime))
}



func main() {

	n := 3       //node个数
	size := 80000000  //文件大小
	filename := "data.in"
	rederN := ReaderN{n,filename,size	}

	dataN := []chan int{}
	for j := 0; j< n; j++{
		s := make(chan int, 1024)
		dataN = append(dataN,s)
	}

	rederN.ReaderFrom(dataN)

	broadcastData(dataN, n)

	out := make(chan int, 1024)

	mergeData(out, n)
	filename = "data.out"

	writer := WriterTo{filename}

	writer.Write(out)

}

func broadcastData(dataN []chan int, n int){
	listener, err := net.Listen("tcp",":33333")
	if err != nil {
		panic(fmt.Sprintf("master main error : %s", err.Error()))
	}

	go func() {
		i := 0
		for {
			conn, err := listener.Accept()
			if err != nil {
				panic(fmt.Sprintf("master main error: %s ", err.Error()))
			}

			go func(i int, conn net.Conn) {
				defer conn.Close()


				f := bufio.NewWriter(conn)
				defer f.Flush()

				b := make([]byte, 8)

				for v := range dataN[i] {

					binary.BigEndian.PutUint64(b, uint64(v))
					f.Write(b)

				}
			}(i, conn)

			i++
			if i >= n {
				break
			}
		}

	}()
}

func mergeData(out chan int, n int){
	listener, err := net.Listen("tcp",":44444")
	if err!= nil {
		panic(fmt.Sprintf("mergeData error: %s", err.Error()))
	}

	dataN := []chan int{}
	for i := 0; i< n; i++{
		ch := make(chan int, 1024)
		dataN = append(dataN, ch)
	}

	go func() {
		i := 0
		for {
			conn, err := listener.Accept()

			if err != nil {
				panic(fmt.Sprintf("mergeData conn error: %s",err.Error()))
			}

			go func(conn net.Conn, i int) {
				defer conn.Close()


				b := make([]byte, 8)
				f :=bufio.NewReader(conn)
				//count := 0
				for {
					for{
						n, err := f.Read(b)     //n是bug的元凶所在，切莫忽略，因为读少了，还是会读，但是数据却不对了
						if err != nil {
							if err == io.EOF{
								close(dataN[i])
								runtime.Goexit()
							}else{
								panic(fmt.Sprintf("mergeData error : %s", err.Error()))
							}
						}else{
							if n != 8{
								fmt.Println("read n bytes: n=",n,"i=",i)
								for {
									b[n], err = f.ReadByte()
									if err != nil {
										panic(fmt.Sprintf("read byte error : %s", err.Error()))
									}

									n++
									if n == 8{
										break
									}
								}
								break
							}else {
								break
							}
						}
					}

					c := int(binary.BigEndian.Uint64(b))
					dataN[i] <- c
				/**	if count%10000 == 0{
						fmt.Println(c)
					}
					count ++
					go func(c int) {
						log(c)
					}(c) */
				}


			}(conn, i)

			i++
			if i >=n {
				break
			}
		}
	}()


	a := mergeN(dataN...)

	go func() {

		for v := range a {

			out <- v

		}
		close(out)
	}()
}


func  getLog() func(n int){
	filename := "log"
	file, err := os.Create(filename)
	if err != nil {
		panic(fmt.Sprintf("log error : %s", err.Error()))
	}
	count := 0
	return func(n int){
		count ++
		if count %10000 == 0{
			f := bufio.NewWriter(file)
			defer f.Flush()
			f.WriteString(strconv.Itoa(n))
			f.WriteByte('\n')
		}


}

}