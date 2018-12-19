package main

import (
	"net"
	"fmt"
	"bufio"
	"encoding/binary"
	"io"
	"sort"
)

type sorter interface{
	dialData()
	sortAndSpread()
}


type sorterNode struct{
	addr1 string
	addr2 string
	ch chan int
}
func (s *sorterNode) dialData(){
	conn, err := net.Dial("tcp",s.addr1)

	if err != nil {
		panic(fmt.Sprintf("dialData error: %s", err.Error()))
	}

	go func() {
		defer  close(s.ch)

		f := bufio.NewReader(conn)
		b := make([]byte, 8)
	//	count := 0
		for{
			_, err := f.Read(b)
			if err != nil {
				if err == io.EOF{
					break
				}else {
					panic(fmt.Sprintf("dialData error : %s ", err.Error()))
				}
			}
		/**	if count %10000 == 0{
				fmt.Println(binary.BigEndian.Uint64(b))

			}
			count ++
*/
			s.ch <- int(binary.BigEndian.Uint64(b))
		}

	}()
}

func (s *sorterNode) sortAndspread(){
	flag := make(chan struct{})
	go func() {
		a := []int{}
		for v := range s.ch{
			a = append(a, v)
		}
		sort.Ints(a)
		conn, err := net.Dial("tcp", s.addr2)

		if err != nil {
			panic(fmt.Sprintf("sortAndspread error : %s", err.Error()))
		}
		defer conn.Close()
		f := bufio.NewWriter(conn)
		defer f.Flush()
		b:= make([]byte, 8)
		count := 0
		for _, v := range a {
			if count%10000 == 0 {
				fmt.Println(v)

			}

			binary.BigEndian.PutUint64(b, uint64(v))
			if count%10000 == 0{
				fmt.Println(binary.BigEndian.Uint64(b))
			}

			count++
			f.Write(b)
		}
		flag<- struct{}{}
	}()
	<-flag
}

type node struct{
	nodeProcessor sorter
}

func main() {
	addr1 := ":33333"
	addr2 := ":44444"
	ch := make(chan int, 1024)
	s := sorterNode{addr1,addr2, ch}

	s.dialData()

	s.sortAndspread()


}
