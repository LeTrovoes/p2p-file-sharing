package main

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/LeTrovoes/p2p-file-sharing/message"
)

const UDP_BUFFER_SIZE = 1024       // max message size in bytes
const LIVENESS_PROBE_INTERVAL = 30 // interval in seconds between checks
const LIVENESS_PROBE_TOLERANCE = 0 // how many checks a peer can fail before considered dead

var (
	mutex       sync.RWMutex
	peerToFiles map[string][]string // [ip:port]: [file1.txt, file2.txt]
	fileToPeers map[string][]string // [file.txt]: [ip:port1, ip:port2]

	aliveMutex sync.RWMutex
	peerAlive  map[string]int // [ip:port]: int

	udpConn *net.UDPConn
)

func main() {

	// REF 4g: pede endereço ip via terminal
	var serverIp string
	fmt.Print("ip: ")
	fmt.Scan(&serverIp)

	// resolve endereço
	udpAddr, err := net.ResolveUDPAddr("udp", serverIp+":10098")

	if err != nil {
		fmt.Println("ERRO: endereço inválido")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// abre socket
	udpConn, err = net.ListenUDP("udp", udpAddr)

	if err != nil {
		fmt.Println("ERRO: não foi possível abrir o socket")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// cria maps
	fileToPeers = make(map[string][]string)
	peerToFiles = make(map[string][]string)
	peerAlive = make(map[string]int)

	// REF 4f inicia goroutine que gerencia a prova de vida dos peers
	go checkPeers()

	// recebe requisições na goroutine principal
	waitForUDP(udpConn)
}

// REF 4a: recebe requisições UDP dos peers e encaminho para os handler
func waitForUDP(udpConn *net.UDPConn) {
	for {
		// loop infinito

		// recebe mensagem
		buffer := make([]byte, UDP_BUFFER_SIZE)
		n, peerAddr, _ := udpConn.ReadFromUDP(buffer) // bloqueia até receber uma mensagem

		// decodifica mensagem
		req, _ := message.MessageFromByteArray(buffer[:n])

		var res *message.Message // resposta

		// encaminha mensagem para o handler correspondente
		switch req.Command {
		case "JOIN":
			res = join(&req, peerAddr.String())

		case "LEAVE":
			res = leave(&req, peerAddr.String())

		case "SEARCH":
			res = search(&req, peerAddr.String())

		case "UPDATE":
			res = update(&req, peerAddr.String())

		case "ALIVE_OK":
			aliveOk(&req, peerAddr.String())
			continue
		}

		res.Id = req.Id                      // atribui id da requisição à resposta
		encRes, _ := res.ToByteArray()       // codifica resposta
		udpConn.WriteToUDP(encRes, peerAddr) // envia resposta
	}
}

/********************
 * Request Handlers *
 ********************/

// REF 4b
func join(req *message.Message, peerId string) (res *message.Message) {
	addPeer(peerId, req.Files)

	fmt.Printf("Peer %s adicionado com arquivos", peerId)
	for _, file := range req.Files {
		fmt.Print(" ", file)
	}
	fmt.Println()

	return &message.Message{Command: "JOIN_OK"}
}

// REF 4c
func leave(req *message.Message, peerId string) (res *message.Message) {
	removePeer(peerId)
	return &message.Message{Command: "LEAVE_OK"}
}

// REF 4d
func search(req *message.Message, peerId string) (res *message.Message) {
	mutex.RLock()
	defer mutex.RUnlock()

	fmt.Printf("Peer %s solicitou arquivo %s\n", peerId, req.File)

	return &message.Message{Command: "SEARCH_OK", Files: fileToPeers[req.File]}
}

// REF 4e
func update(req *message.Message, peerId string) (res *message.Message) {
	addFileToPeer(peerId, req.File)
	return req
}

// REF 4f: recebe resposta do ALIVE
func aliveOk(req *message.Message, peerId string) {
	aliveMutex.Lock()
	defer aliveMutex.Unlock()

	peerAlive[peerId] = LIVENESS_PROBE_TOLERANCE + 1
}

/*********************
 * Application Logic *
 ********************/

// adiciona um peer e seus arquivos às estruturas de dados (REF 4b)
func addPeer(peerId string, fileNames []string) {
	mutex.Lock()
	defer mutex.Unlock()

	_, ok := peerToFiles[peerId]

	if ok {
		return // already registered, skip
	}

	// adiciona arquivos ao peer
	peerToFiles[peerId] = fileNames

	// adiciona o peer em cada arquivo
	for _, fileName := range fileNames {
		peersA, ok := fileToPeers[fileName]

		if ok {
			fileToPeers[fileName] = append(peersA, peerId)
		} else {
			fileToPeers[fileName] = []string{peerId}
		}
	}

	aliveMutex.Lock()
	defer aliveMutex.Unlock()

	// adicona o peer à prova de vida
	peerAlive[peerId] = LIVENESS_PROBE_TOLERANCE + 1
}

// remove um peer e seus arquivos das estruturas de dados (REF 4c)
func removePeer(peerId string) {
	mutex.Lock()
	defer mutex.Unlock()

	files, ok := peerToFiles[peerId]

	if !ok {
		return // peer not registered, skip
	}

	// remove o peer de cada arquivo, apagando o arquivo se nenhum outro peer o tiver
	for _, file := range files {
		peers := fileToPeers[file]
		for i, peer := range peers {
			if peer == peerId {
				peers[i] = peers[len(peers)-1]
				fileToPeers[file] = peers[:len(peers)-1]
				break
			}
		}

		if len(fileToPeers[file]) == 0 {
			delete(fileToPeers, file)
		}
	}

	// remove o peer
	delete(peerToFiles, peerId)

	aliveMutex.Lock()
	defer aliveMutex.Unlock()

	// remove o peer da prova de vida
	delete(peerAlive, peerId)
}

// adiciona um arquivo a um peer já existente. É chamada pelo UPDATE (REF 4e)
func addFileToPeer(peerId string, fileName string) {
	mutex.Lock()
	defer mutex.Unlock()

	files, ok := peerToFiles[peerId]

	if !ok {
		return // peer not registered, ignore
	}

	if findInSlice(files, fileName) >= 0 {
		return // file already in peer, ignore
	}

	peerToFiles[peerId] = append(peerToFiles[peerId], fileName)

	peers, ok := fileToPeers[fileName]
	if ok {
		fileToPeers[fileName] = append(peers, peerId)
	} else {
		fileToPeers[fileName] = []string{peerId}
	}
}

// REF 4f: faz a prova de vida dos peers
func checkPeers() {
	for {
		// envia a requisição ALIVE a todos os peers
		aliveMutex.RLock()
		for key, value := range peerAlive {
			peerAlive[key] = value - 1
			pingPeer(key)
		}
		aliveMutex.RUnlock()

		// espera o intervalo entre as provas
		time.Sleep(LIVENESS_PROBE_INTERVAL * time.Second)

		// elimina peers que não responderam à requisição
		mutex.RLock()
		aliveMutex.Lock()
		for key, value := range peerAlive {
			if value == 0 {
				fmt.Printf("Peer %s morto. Eliminando seus arquivos", key)
				for _, file := range peerToFiles[key] {
					fmt.Printf(" %s", file)
				}
				fmt.Println()

				go removePeer(key)
			}
		}
		aliveMutex.Unlock()
		mutex.RUnlock()
	}
}

// envia requisição ALIVE a um peer
func pingPeer(peer string) {
	req := message.Message{Command: "ALIVE"}
	encoded, _ := req.ToByteArray()
	udpAddr, _ := net.ResolveUDPAddr("udp", peer)
	udpConn.WriteToUDP(encoded, udpAddr)
}

/***********
 * Helpers *
 **********/

func findInSlice(slice []string, value string) int {
	for i, v := range slice {
		if v == value {
			return i
		}
	}
	return -1
}
