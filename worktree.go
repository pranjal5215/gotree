package main

import (
	"fmt"
	"time"
)

type aggStruct struct{
	index	int
	data	interface{}
}

type channelStruct struct{
	quitChan chan bool
	size int
	aggregator chan aggStruct
	FetchFunctions       []func(inp interface{}) interface{}
	FetchFunctionsInput  []interface{}
	Combiner	func(index int, inp interface{}) interface{}
}

func (cS *channelStruct) AddFetcher (f func(inp interface{}) interface{}, input interface{}) {
	cS.FetchFunctions = append(cS.FetchFunctions, f)
	cS.FetchFunctionsInput = append(cS.FetchFunctionsInput, input)
	cS.size = cS.size + 1
}

func (cS *channelStruct) AddCombiner (f func(index int, inp interface{}) interface{}) {
	cS.Combiner = f
}

func (cS *channelStruct) Do (f func(inp []interface{}) interface{}) {
	//I keep the channels in this slice, and want to "loop" over them in the select statement

	cS.aggregator = make(chan aggStruct, cS.size)
	cS.quitChan = make(chan bool, cS.size)

	for index, function := range cS.FetchFunctions {
		go cS.workAndQuit(index, function, cS.FetchFunctionsInput[index])
	}

	breakFlag := false
	received := 0
	for received < cS.size {
		select {
			case msg := <-cS.aggregator:
				fmt.Println("received ", msg.data)
				fmt.Println("index ", msg.index)
				cS.Combiner(msg.index, msg.data)
			case <- cS.quitChan:
				received = received + 1
			case <-time.After(2 * time.Second):
				fmt.Println("timimg out")
				breakFlag = true
				break
		}
		if breakFlag{
			break
		}
	}
}

func (cS *channelStruct) work(index int, function func(inp interface{}) interface{}, payload interface{}) {
	//time.Sleep(3*time.Second)
	cS.aggregator <- aggStruct{index, function(payload)}
}

func (cS *channelStruct) workAndQuit(index int, function func(inp interface{}) interface{}, payload interface{}){
	// wrapped user defined function
	cS.work(index, function, payload)
	cS.quitChan <- true
}

func main() {

	cS := channelStruct{}
	cS.AddFetcher()

	/*for i := 0; i < numChans; i++ {
		msg := <-agg
		fmt.Println("message", msg)
	}*/
}
