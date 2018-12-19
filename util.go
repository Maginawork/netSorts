package lib

import (
	"math/rand"
	"os"
	"fmt"
	"encoding/binary"
	"time"
	"bufio"
)
var TimeStart time.Time
func init(){
	TimeStart = time.Now()
}

func RandCreateInt(filename string, n int) {
	file,err := os.Create(filename)
	if err != nil {
		panic(fmt.Sprintf("RandCreateInt openfile error: %s",err.Error()))
	}
	defer file.Close()
	b := make([]byte, 8)
	mark := make(chan struct{})
	go func() {
		w := bufio.NewWriter(file)
		defer w.Flush()
		for i := 0; i < n; i++ {
			a := rand.Intn(1000000000)
			binary.BigEndian.PutUint64(b, uint64(a))
			_, err = w.Write(b)
			if err != nil {
				panic(fmt.Sprintf("filewrite error : %s", err.Error()))
			}

		}

		d := time.Now().Sub(TimeStart)
		fmt.Printf("create data cost time: %s\n", d)
		mark <- struct{}{}
	}()
	<- mark
}
/**
func ReadAndSort(file *os.File, size int) <-chan int{
	a := make([]int,0)
	b := make([]byte, 8)
	out := make(chan int, 1024)
	go func() {
		f := bufio.NewReader(file)
		for i:=0; i < size; i++{
			_, err := f.Read(b)
			if err != nil {
				panic(fmt.Sprintf("readAndSort error : %s", err.Error()))
			}
			c := int(binary.BigEndian.Uint64(b))
			a = append(a,c)
		}
		sort.Ints(a)
		for _, r := range a {
			out <- r
		}
		close(out)
	}()
	return out

}



func DataSort(filename string, total int, n int) <- chan int {
	chunkSize := total/n
	nline := make([]<-chan int, n)
	for i := 0; i < n; i++{
		file, err := os.Open(filename)
		if err != nil {
			panic(fmt.Sprintf("DataSort error : %s", err.Error()))
		}

		file.Seek(int64(chunkSize * i*8), 0)
		nline[i] = ReadAndSort(file,chunkSize)
	}
	out := MergeN(nline...)
	fmt.Printf("data sort cost time: %s \n", time.Now().Sub(TimeStart))
	return out
}

func  Merge(a <-chan int, b <-chan int) <-chan int{
	out := make(chan int, 1024)
	a1, ok1 := <- a
	b1, ok2 := <-b
	go func(){
		for {
			if (ok1 && a1 <= b1) || (ok1 && !ok2) {
				out <- a1
				a1, ok1 = <- a
			}else {
				if (!ok1 && !ok2){
					break
				}else{
					out <- b1
					b1, ok2 = <- b
				}
			}
		}
		close(out)
	}()
	return out
}


func MergeN(a ...chan int) <- chan int{
	if len(a) == 1{
		return a[0]
	}
	n := len(a)/2
	out := Merge(MergeN(a[:n]...),MergeN(a[n:]...))
	return out
}

func WriteData(filename string, ch <-chan int) error {
	file, err := os.Create(filename)
	defer file.Close()
	if err != nil {
		//panic(fmt.Sprintf("WriteData error : %s", err.Error()))
		return err
	}
	b := make([]byte, 8)
	count := 0
	f := bufio.NewWriter(file)
	defer f.Flush()
	for v := range ch {

		binary.BigEndian.PutUint64(b, uint64(v))
		_, err = f.Write(b)
		if err != nil {
			break
		}
		if count<10{
			fmt.Println(v)
			count++
		}
	}
	fmt.Printf("writedata cost time: %s\n",time.Now().Sub(TimeStart))
	return err
}
*/