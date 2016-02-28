package main

import (
	"fmt"
	"time"
)

// Struct channel which is used to send and receive data
// Index in mapper index
// Data in actual data interface
// Sentinal is used to signal end of response for one mapper
type AggStruct struct{
	Index	int
	Data	interface{}
	Sentinal	bool
}

// Channel struct which will be invoked by client
type channelStruct struct{
	size	int
	aggregator	chan AggStruct
	parent	chan AggStruct
	FetchFunctions	[]func(inp interface{}, agg chan AggStruct) interface{}
	FetchFunctionsInput	[]interface{}
	Combiner	func(index int, inp interface{}, allR interface{}) (interface{}, bool)
}

// Add fetcher(mapper)
func (cS *channelStruct) AddFetcher (f func(inp interface{}, agg chan AggStruct) interface{}, input interface{}) {
	cS.FetchFunctions = append(cS.FetchFunctions, f)
	cS.FetchFunctionsInput = append(cS.FetchFunctionsInput, input)
	cS.size = cS.size + 1
}

// Add combiner(reducer)
func (cS *channelStruct) AddCombiner (f func(index int, inp interface{}, allR interface{}) (interface{}, bool)) {
	cS.Combiner = f
}

func retNil() interface{} {
	return nil
}

// Every run is async, combiner is given current response state and 
// data_recv along with index at which mapped data is received 
// new_resp_instance = data_recv + old resp_instance 
// data returned from combiner is updated to new_resp_instance
// Finally after all receives on aggregator resp is returned.
func (cS *channelStruct) Do () interface{} {
	//I keep the channels in this slice, and want to "loop" over them in the select statement
	for index, function := range cS.FetchFunctions {
		go cS.workAndQuit(index, function, cS.FetchFunctionsInput[index])
	}

	cS.aggregator = make(chan AggStruct, cS.size)
	cS.parent = make(chan AggStruct, cS.size)

	breakFlag := false
	received := 0
	resp := retNil()
	sendToParent := false
	for received < cS.size {
		select {
			case msg := <-cS.aggregator:
				if msg.Sentinal == true{
					received = received + 1
					break
				}
				resp, sendToParent = cS.Combiner(msg.Index, msg.Data, resp)
				if sendToParent == true{
					agg := AggStruct{msg.Index, resp, false}
					cS.parent <- agg
				}
				// cs.Parent <- resp ***
			case <-time.After(2 * time.Second):
				breakFlag = true
				break
		}
		if breakFlag{
			break
		}
	}
	return resp
}

// work; called when mapper is invoked
// If you create a new work tree in child mapper; function(payload); 
// assign cs.parent=that channel and send resp to cs.Parent ***
func (cS *channelStruct) work(index int, function func(inp interface{}, agg chan AggStruct) interface{}, payload interface{}) {
	agg := AggStruct{index, function(payload, cS.aggregator), false}
	cS.aggregator <- agg
}

// do work and then send sentinal channel to exit
// wrap function call and send as Aggstruct instance to agg channel
func (cS *channelStruct) workAndQuit(index int, function func(inp interface{}, agg chan AggStruct) interface{}, payload interface{}){
	cS.work(index, function, payload)
	agg := AggStruct{0, nil, true}
	cS.aggregator <- agg
}

func fetcher(inp interface{}, parent chan AggStruct) interface{} {
	i := inp.(int)
	if i == 20{
		cS := channelStruct{parent:parent}
		cS.AddFetcher(fetcher1, 10)
		cS.AddFetcher(fetcher1, 20)
		cS.AddCombiner(combiner)
		resp := cS.Do()
		return i + resp.(int)
	}
	return i
}
func fetcher1(inp interface{}, _ chan AggStruct) interface{} {
	i := inp.(int)
	return i
}

func combiner(index int, data interface{}, resp interface{}) (interface{}, bool) {
	out := 0
	if resp != nil{
		out = data.(int) + resp.(int)
	}else{
		out = data.(int)
	}
	return out, false
}

func main() {
	// simple tree
	cS := channelStruct{}
	cS.AddFetcher(fetcher1, 20)
	cS.AddFetcher(fetcher1, 33)
	cS.AddFetcher(fetcher1, 16)
	cS.AddFetcher(fetcher1, 3)
	cS.AddCombiner(combiner)
	resp := cS.Do()
	fmt.Println(resp)


	// tree behind a tree
	cS1 := channelStruct{}
	cS1.AddFetcher(fetcher, 20)
	cS1.AddFetcher(fetcher, 30)
	cS1.AddFetcher(fetcher, 40)
	cS1.AddFetcher(fetcher, 50)
	cS1.AddCombiner(combiner)
	resp1 := cS1.Do()
	fmt.Println(resp1)


	/*for i := 0; i < numChans; i++ {
		msg := <-agg
		fmt.Println("message", msg)
	}*/
}
