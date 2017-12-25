package main

import (
	"fmt"
	"time"

	"math/rand"
	"os"
)

//Non costruisco la struct di broker, poichè è solo colui che smista i messaggi nella mia config
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
//mi serve per scrivere su file, convertendo i field dei messaggi struct
func (m Message) MessageToString() string{

	str := fmt.Sprintln("Topic: ",m.topic," Value: ",m.value)
	return str

}



//al momento tutti gli attori e i topic sono costanti, poi si potrebbero mettere interattivi
const numactuators = 4
const numsensors = 3
const numtopics = 2

//uso delle variabili globali cosicchè istanzio solo una volta la memoria per le funzioni che vengono richiamate più volte
//alla fine, li usiamo sempre nel nostro progetto
var csensor [numsensors]chan Sensor
var cactuator [numactuators]chan Actuator
var cbroker chan Message
var ackch [numactuators]chan string //canale usato per spedire ack dall'attuatore
var attuatorArchive [numactuators]Actuator
var sensorArchive [numsensors]Sensor
var path string
var receivedMessage []string
var ackNack = [2]string{"ack", "fault"}//fault messo come test
//frequenza di invio messaggi da parte di sensor
var frequency time.Duration = 4 * time.Second
//la funzione main non fa altro che le routine normali SEQUENZIALI alle quali sono associate go routine "multithread"
func main() {
	//Prendo il path per poter scrivere nel percorso di esecuzione
	path = definePath()

	var tickChan = time.NewTicker(frequency).C
	fmt.Println("Frequenza invio messaggi : ", frequency)
	sensors(true)
	actuators(true, nil, 0)

	for {
		select {

		case <-tickChan:
			go sensors(false)


		}
	}

}

//la booleana registration, serve per la connect e per il make dei channel e dei topics,
//il message è il tipo di channel che prende per poi processaarlo
//l'id lo userò per la funzione del broker per stampare a schermo(e poi eventualmente si userà per altro..tipo liste ecc)
func actuators(registration bool, message chan Message, id int) {

	numact := numactuators

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
			topicsA.topicsA[ida] = []string{"topic1", "topic4"}
			go broker(nil, cactuator[numact-1], true)
			cactuator[numact-1] <- Actuator{ida, topicsA.topicsA}
			ida++
			//	time.Sleep(1 * time.Second)
			<-cactuator[numact-1]
			numact--

		} //l'else serve ad accogliere i messaggi del broker e a stampare su schermo ciò che riceve l'attuatore
	} else {
		select {
		case x := <-message:
			fmt.Println("Acutator", id, "received", x.topic, "with value", x.value)
			//time.Sleep(time.Second*1)

			//ackch è un canale in cui l'attuatore manda l'ack di riferimento al broker
			ackch[id] = make(chan string)
			fmt.Println("Attuatore:", id, " manda ack")
			//ritardo per testare timeout
			//time.Sleep(time.Second * 1)

			go broker(nil, cactuator[id], false)
			//Spedisco nel canale un valore casuale tra fault e ack, insieme al messaggio ricevuto(da rivedere)
			ackch[id] <- ackNack[0]
			writeFile(x)


		}
	}
}

//stesso discorso dell'attuatore per il bool
func sensors(registration bool) {

	numsens := numsensors
	ids := 0

	if registration {
		for i := range csensor {
			csensor[i] = make(chan Sensor)
		}

		for numsens > 0 {
			go broker(csensor[numsens-1], nil, true)
			csensor[numsens-1] <- Sensor{ids, nil, 0} //non servono nè topic nè value per la registrazione
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
			topicsS.topicsS[ids] = "topic1"                                   // per ora, topic uguale per tutti i sensori
			csensor[numsens-1] <- Sensor{ids, topicsS.topicsS, rand.Intn(50)} //poi il value sarà random
			ids++
			//time.Sleep(1 * time.Second)
			//ho dovuto commentare altrimenti si blocca<-cbroker//questo serve per non far andare in deadlock il broker così non si aspettano a vicenda con csensor
			<-csensor[numsens-1]
			numsens--
		}
	}
}

//variabili di loop per riempire gli array di struct actuator e di struct Sensor (potrebbero anche essere provvisori, mi interessava la communication al momento)
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
						go actuators(false, cbroker, i)
						cbroker <- Message{connectS.topicsS[connectS.id], connectS.value}
						go waiting(ackch[i], i, Message{connectS.topicsS[connectS.id], connectS.value})

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
			fmt.Println("Ho ricevuto : ", s, " da ", ackIndex, "con ", message.value)
			ackch <- s
			return
			//non si verifica attualmente
		} else if s == "fault" {
			fmt.Println("Ho riscontrato un : ", s, " da ", ackIndex)

			//se ho riscontrato un fault spedisco nuovamente il messaggio
			go actuators(false, cbroker, ackIndex)
			cbroker <- message
			ackch <- s

			return
		} else {
			fmt.Println("Messaggio inaspettato")
			ackch <- s

			return
		}
		//timeout: scaduto il tempo ritrasmetto il messaggio
	case <-time.After(time.Second * 4):
		fmt.Println("timeout ", message.value, "Attuatore ", ackIndex, ", ritrasmetto")
		go actuators(false, cbroker, ackIndex)
		cbroker <- message
		<-ackch

		return


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
