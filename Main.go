package main

import (
	"fmt"
	"time"
)
type Sensor struct {
	id int
	topicsS map[int]string
	value int
}

type Actuator struct {
	id int
	topicsA map[int][]string
}
type Message struct {

	topic string
	value int
}
const numactuators = 3
const numsensors   = 3
const numtopics    = 2

var csensor   [numsensors]chan Sensor
var cactuator [numactuators]chan Actuator
var cbroker   chan Message
var attuatore  [numactuators]Actuator
var sensore    [numsensors]Sensor

func main() {

	sensors(true)
	actuators(true,nil,0)
	sensors(false)

}

func actuators(registration bool, message chan Message, id int){

	numact  := numactuators

	ida := 0

	if registration {
		for i := range cactuator {
			cactuator[i] = make(chan Actuator)
		}

		topicsA := &Actuator{
			topicsA: make(map[int][]string),
		}

		for (numact > 0) {
			topicsA.topicsA[ida] = []string{"topic1", "topic4"}
			go broker(nil, cactuator[numact-1], true)
			cactuator[numact-1] <- Actuator{ida, topicsA.topicsA}
			ida++
			time.Sleep(1 * time.Second)
			<-cactuator[numact-1]
			numact--

		} 
	}else{
		select{
			case x := <-message:
				fmt.Println("Acutator",id,"received",x.topic,"with value",x.value)
		}
	}
}
func sensors(registration bool){

	numsens := numsensors
	ids := 0

	if registration {
		for i := range csensor {
			csensor[i] = make(chan Sensor)
		}

		for numsens > 0 {
			go broker(csensor[numsens-1], nil, true)
			csensor[numsens-1] <- Sensor{ids, nil, 0}
			ids++
			time.Sleep(1 * time.Second)
			<-csensor[numsens-1]
			numsens--
		}
	}else{
		topicsS := &Sensor{
			topicsS: make(map[int]string),
		}

		for numsens > 0 {
			go broker(csensor[numsens-1], nil, false)
			topicsS.topicsS[ids] = "topic1"
			csensor[numsens-1] <- Sensor{ids, topicsS.topicsS, 50}
			ids++
			time.Sleep(1 * time.Second)
			<-cbroker
			<-csensor[numsens-1]
			numsens--
		}
	}
}
var i = 0
var j = 0
func broker(sensor chan Sensor, actuator chan Actuator, registration bool) {

	if registration {
		select{
			case connectS := <-sensor:
				fmt.Println("Sensor",connectS.id," registered")
				time.Sleep(100 * time.Millisecond)
				sensor <- connectS
			case x :=<-actuator:
				attuatore[i] = Actuator{x.id,x.topicsA}
				fmt.Println("Actuator",attuatore[i].id," registered with topics",attuatore[i].topicsA[attuatore[i].id])
				i++
				time.Sleep(100 * time.Millisecond)
				actuator <- attuatore[i-1]

		}
	}else{
		cbroker = make(chan Message)
		select{
			case connectS := <-sensor:
				sensore[j]  = Sensor{connectS.id,connectS.topicsS,connectS.value}
				j++
				fmt.Println("Broker : received",connectS.topicsS[connectS.id],"with value", connectS.value,"From Sensor",connectS.id)
				time.Sleep(100 * time.Millisecond)
				for i := range attuatore {
					for j :=0; j<numtopics; j++{
						if attuatore[i].topicsA[i][j] == connectS.topicsS[connectS.id] {
							go actuators(false,cbroker,i)
							cbroker <- Message{connectS.topicsS[connectS.id],connectS.value}
							time.Sleep(1 * time.Second)

						}
					}
				}
				sensor <- connectS

		}

	}

}
