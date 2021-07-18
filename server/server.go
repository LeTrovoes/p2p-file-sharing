package main

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/LeTrovoes/p2p-file-sharing/message"
)

const LIVENESS_PROBE_INTERVAL = 30
const LIVENESS_PROBE_TOLERANCE = 0 // how many checks a peer can fail before considered dead

var (
	mutex       sync.RWMutex
	peerToFiles map[string][]string // [ip:port]: [file1.txt, file2.txt]
	fileToPeers map[string][]string // [file.txt]: [ip:port1, ip:port2]

	peerAlive  map[string]int
	aliveMutex sync.RWMutex

	udpConn *net.UDPConn
)

func main() {

	var serverIp string
	fmt.Print("ip: ")
	fmt.Scan(&serverIp)

	udpAddr, err := net.ResolveUDPAddr("udp", serverIp+":10098")

	if err != nil {
		fmt.Println("ERRO: endereço inválido")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	udpConn, err = net.ListenUDP("udp", udpAddr)

	if err != nil {
		fmt.Println("ERRO: não foi possível abrir o socket")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	fileToPeers = make(map[string][]string)
	peerToFiles = make(map[string][]string)
	peerAlive = make(map[string]int)

	go checkPeers()

	waitForUDP(udpConn)
}

func waitForUDP(udpConn *net.UDPConn) {
	for {
		buff := make([]byte, 1024)
		n, peerAddr, _ := udpConn.ReadFromUDP(buff)

		req, _ := message.MessageFromByteArray(buff[:n])

		var res *message.Message

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
		res.Id = req.Id

		encRes, _ := res.ToByteArray()
		udpConn.WriteToUDP(encRes, peerAddr)
	}
}

/********************
 * Request Handlers *
 ********************/

func join(req *message.Message, peerId string) (res *message.Message) {
	addPeer(peerId, req.Files)

	fmt.Printf("Peer %s adicionado com arquivos", peerId)
	for _, file := range req.Files {
		fmt.Print(" ", file)
	}
	fmt.Println()

	return &message.Message{Command: "JOIN_OK"}
}

func leave(req *message.Message, peerId string) (res *message.Message) {
	removePeer(peerId)
	return &message.Message{Command: "LEAVE_OK"}
}

func search(req *message.Message, peerId string) (res *message.Message) {
	mutex.RLock()
	defer mutex.RUnlock()

	fmt.Printf("Peer %s solicitou arquivo %s\n", peerId, req.File)

	return &message.Message{Command: "SEARCH_OK", Files: fileToPeers[req.File]}
}

func update(req *message.Message, peerId string) (res *message.Message) {
	addFileToPeer(peerId, req.File)
	return req
}

func aliveOk(req *message.Message, peerId string) {
	aliveMutex.Lock()
	defer aliveMutex.Unlock()

	peerAlive[peerId] = LIVENESS_PROBE_TOLERANCE + 1
}

/*********************
 * Application Logic *
 ********************/

func addPeer(peerId string, fileNames []string) {
	mutex.Lock()
	defer mutex.Unlock()

	_, ok := peerToFiles[peerId]

	if ok {
		return // already registered, skip
	}

	peerToFiles[peerId] = fileNames

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

	peerAlive[peerId] = LIVENESS_PROBE_TOLERANCE + 1
}

func removePeer(peerId string) {
	mutex.Lock()
	defer mutex.Unlock()

	files, ok := peerToFiles[peerId]

	if !ok {
		return // peer not registered, skip
	}

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

	delete(peerToFiles, peerId)

	aliveMutex.Lock()
	defer aliveMutex.Unlock()

	delete(peerAlive, peerId)
}

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

func checkPeers() {
	for {
		aliveMutex.RLock()
		for key, value := range peerAlive {
			peerAlive[key] = value - 1
			pingPeer(key)
		}
		aliveMutex.RUnlock()

		time.Sleep(LIVENESS_PROBE_INTERVAL * time.Second)

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
