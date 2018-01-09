package main

import (
	"fmt"
	"time"
	"math/rand"
	"os"
)

type Sensor struct {
	id      int
	topicsS map[int]string
	value   int
}

type Actuator struct {
	id      int
	topicsA map[int][]string
}

type Message struct {
	topic string
	value int
}
func (m Message) MessageToString() string{

	str := fmt.Sprintln("Topic: ",m.topic," Value: ",m.value)
	return str

}



var numtopics int

var csensor []chan Sensor
var cactuator []chan Actuator
var cbroker chan Message
var ackch []chan string
var attuatorArchive []Actuator
var sensorArchive []Sensor
var path string
var receivedMessage []string
var topicList  []string
var ackNack = [2]string{"ack", "fault"}//fault messo come test

var frequency time.Duration

func main() {
	//Prendo il path per poter scrivere nel percorso di esecuzione
	path = definePath()


	var nsensor   int
	var nactuator int
	var topictmp  string

	fmt.Print("Number of Sensors: ")
	fmt.Scan(&nsensor)
	fmt.Print("Number of topics: ")
	fmt.Scan(&numtopics)
	topicList = make([]string,numtopics)
	for numtopics > 0{
		fmt.Print("Inserisci topic: ")
		fmt.Scan(&topictmp)
		numtopics--
		topicList[numtopics] = topictmp
	}
	fmt.Print("Number of Actuators: ")
	fmt.Scan(&nactuator)
	fmt.Print("Frequency for sending messages :")
	fmt.Scan(&frequency)
	frequency = frequency * time.Second


	csensor   = make([]chan Sensor,nsensor)
	cactuator = make([]chan Actuator,nactuator)
	ackch     = make([]chan string,nactuator)

	attuatorArchive = make([]Actuator,nactuator)
	sensorArchive   = make([]Sensor,nsensor)

	var tickChan = time.NewTicker(frequency).C
	fmt.Println("Freque : ", frequency)
	sensors(true,nsensor,csensor)
	actuators(true, nil, 0,nactuator,cactuator)

	for {
		select {

		case <-tickChan:
			go sensors(false,nsensor,csensor)

		}
	}

}

func actuators(registration bool, message chan Message, id int,numact int, cactuator []chan Actuator) {

	ida := 0

	if registration {

		for i := range cactuator {
			cactuator[i] = make(chan Actuator)
		}

		topicsA := &Actuator{
			topicsA: make(map[int][]string),
		}

		//al momento, sto usando gli stessi topic per ogni attuatore, poi ovviamente si possono cambiare
		for numact > 0 {
			n := len(topicList)
			pickrand := rand.Intn(n)
			for t :=0;t<=pickrand;t++{

				topicsA.topicsA[ida] = append(topicsA.topicsA[ida],topicList[t])
			}
			//topicsA.topicsA[ida] = []string{"topic1", "topic4"}
			go broker(nil, cactuator[numact-1], true)
			cactuator[numact-1] <- Actuator{ida, topicsA.topicsA}
			ida++
			//	time.Sleep(1 * time.Second)
			<-cactuator[numact-1]
			numact--

		}
	} else {
		select {
		case x := <-message:
			fmt.Println("Acutator", id, "received", x.topic, "with value", x.value)
			//time.Sleep(time.Second*1)

			ackch[id] = make(chan string)
			fmt.Println("Actuator:", id, " sending ack")


			go waiting(ackch[id], id, x)
			//testing timeout with time.sleep
			time.Sleep(time.Second * 5)
			//Spedisco nel canale un valore casuale tra fault e ack, insieme al messaggio ricevuto(da rivedere)
			ackch[id] <- ackNack[0]
			writeFile(x)


		}
	}
}

func sensors(registration bool,numsens int,csensor []chan Sensor) {

	ids := 0

	if registration {
		for i := range csensor {
			csensor[i] = make(chan Sensor)
		}

		for numsens > 0 {
			go broker(csensor[numsens-1], nil, true)
			csensor[numsens-1] <- Sensor{ids, nil, 0}
			ids++
			//time.Sleep(1 * time.Second)
			<-csensor[numsens-1]
			numsens--
		}
	} else {
		topicsS := &Sensor{
			topicsS: make(map[int]string),
		}

		for numsens > 0 {
			go broker(csensor[numsens-1], nil, false)
			n := len(topicList)
			topicsS.topicsS[ids] = topicList[rand.Intn(n)]
			csensor[numsens-1] <- Sensor{ids, topicsS.topicsS, rand.Intn(50)}
			ids++
			//time.Sleep(1 * time.Second)
			//ho dovuto commentare altrimenti si blocca<-cbroker//questo serve per non far andare in deadlock il broker così non si aspettano a vicenda con csensor
			<-csensor[numsens-1]
			numsens--
		}
	}
}

var i = 0
var j = 0

func broker(sensor chan Sensor, actuator chan Actuator, registration bool) {

	//registrazione di sensori e attuatori e negli attuatori salvo su array di struct Actuator e stampo un verbose ovunque.
	if registration {
		select {
		case connectS := <-sensor:
			fmt.Println("Sensor", connectS.id, " registered")
			//time.Sleep(100 * time.Millisecond)
			receivedMessage = append(receivedMessage, fmt.Sprintln("Sensor ", connectS.id, " registered (archived)"))
			//fmt.Println(receivedMessage)
			sensor <- connectS
		case x := <-actuator:
			attuatorArchive[i] = Actuator{x.id, x.topicsA}
			fmt.Println("Actuator", attuatorArchive[i].id, " registered with topics", attuatorArchive[i].topicsA[attuatorArchive[i].id])

			receivedMessage = append(receivedMessage, fmt.Sprintln("Actuator", attuatorArchive[i].id, " registered with topics",
				attuatorArchive[i].topicsA[attuatorArchive[i].id], "(archived)"))
			//fmt.Println(receivedMessage)
			i++
			//	time.Sleep(100 * time.Millisecond)
			actuator <- attuatorArchive[i-1]

		}
	} else {

		cbroker = make(chan Message)

		select {
		case connectS := <-sensor:

			//senza il controllo if si avrebbe out of bound, in quanto j viene sempre incrementata. Per cui una volta che
			//nel main chiamo il tick, devo pure controllare che lo slice non abbia sforato con la dimensione
			if len(sensorArchive) == j {

				j = 0
				sensorArchive[j] = Sensor{connectS.id, connectS.topicsS, connectS.value}
			}

			sensorArchive[j] = Sensor{connectS.id, connectS.topicsS, connectS.value}
			j++
			fmt.Println("Broker : received", connectS.topicsS[connectS.id], "with value", connectS.value, "From Sensor", connectS.id)

			receivedMessage = append(receivedMessage, fmt.Sprintln("Broker : received", connectS.topicsS[connectS.id], "with value",
				connectS.value, "From Sensor", connectS.id))

			//time.Sleep(100 * time.Millisecond)
			//scorro sugli attuatori e vedo se il topic serve a qualcuno e lo mando nel caso.

			for i := range attuatorArchive {
				for j := 0; j < numtopics; j++ {
					if attuatorArchive[i].topicsA[i][j] == connectS.topicsS[connectS.id] {
						go actuators(false, cbroker, i,0,cactuator)
						cbroker <- Message{connectS.topicsS[connectS.id], connectS.value}

						//	time.Sleep(2 * time.Second)

					}
				}
			}
			sensor <- connectS




		}

	}
}
//ackch: canale di riferimento broker-actuator;ackIndex: indice dell'attuatore (serve per capire chi ha mandato ack),message: messaggio da confermare
func waiting(ackch chan string, ackIndex int, message Message) {
	select {
	case s := <-ackch:

		if s == "ack" {
			fmt.Println("Received: ", s, " from actuator:", ackIndex, " from topic with value:", message.value)
			ackch <- s
			return
			//non si verifica attualmente
		} else if s == "fault" {
			fmt.Println(" ", s, " from actuator: ", ackIndex)

			//se ho riscontrato un fault spedisco nuovamente il messaggio
			go actuators(false, cbroker, ackIndex,0,cactuator)
			cbroker <- message
			ackch <- s

			return
		} else {
			fmt.Println("unexpected message")
			ackch <- s

			return
		}
		//timeout: scaduto il tempo ritrasmetto il messaggio
	case <-time.After(time.Second * 1):
		fmt.Println("timeout for topic with value: ", message.value, "Retransmitting for actuator ",
			ackIndex)
		go actuators(false, cbroker, ackIndex,0,cactuator)
		cbroker <- message
		<-ackch
		

	}

}
//ritorna la path in cui si esegue il simulatore
func definePath() string {


	var 	path,errore = os.Getwd()
	if errore != nil {
		fmt.Println(errore.Error())
	}
	return path+"/GoFile.txt"
}

func createFile() {
	// detect if file exists
	var _, err = os.Stat(path)

	// create file if not exists
	if os.IsNotExist(err) {
		var file, err = os.Create(path)
		if isError(err) {
			return
		}
		defer file.Close()
	}

	//fmt.Println("File Created", path)
}

func writeFile(message Message) {
	createFile()
	// open file using READ & WRITE permission
	//O_APPEND string
	var file, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if isError(err) {
		return
	}
	defer file.Close()



	_, err = file.WriteString(message.MessageToString())

	if isError(err) {
		return
	}

	// save changes
	err = file.Sync()
	if isError(err) {
		return
	}

	//	fmt.Println("HO SCRITTO")
}

func isError(err error) bool {
	if err != nil {
		fmt.Println(err.Error())
	}

	return err != nil
}
