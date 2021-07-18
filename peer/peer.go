package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path"
	"time"

	"github.com/LeTrovoes/p2p-file-sharing/message"
)

const REQUEST_RETRIES = 3
const REQUEST_RETRY_DELAY = 10
const DOWNLOAD_RETRIES = 3
const DOWNLOAD_RETRY_DELAY = 10
const SERVER_ADDR_STR = "127.0.0.1:10098"

var (
	udpConn     *net.UDPConn
	peerFolder  string
	initialized bool

	fileToDownload string
	peersWithFile  []string

	SERVER_ADDR *net.UDPAddr

	messageCounter int
)

func main() {
	initialized = false
	messageCounter = 1
	rand.Seed(time.Now().UnixNano())

	SERVER_ADDR, _ = net.ResolveUDPAddr("udp", SERVER_ADDR_STR)

	inbox := make(chan message.Message)

	// menu in main thread
	menu(inbox)
}

func menu(inbox chan message.Message) {
	join(inbox)

	for {
		var command string

		fmt.Print("Comando: ")
		fmt.Scan(&command)

		switch command {
		case "JOIN":
			join(inbox)

		case "SEARCH":
			search(inbox)

		case "DOWNLOAD":
			download(inbox)

		case "LEAVE":
			leave(inbox)
		}
	}
}

/********************
 * Console Commands *
 *******************/

func join(inbox chan message.Message) {

	if initialized {
		fmt.Println("  peer já inicializado")
		return
	}

	var peerIp string
	fmt.Print("  ip: ")
	fmt.Scan(&peerIp)

	var port string
	fmt.Print("  porta: ")
	fmt.Scan(&port)

	fmt.Print("  pasta: ")
	fmt.Scan(&peerFolder)

	// validate folder
	files, err := listFilesInDir(peerFolder)

	if err != nil {
		fmt.Println("ERRO: pasta invalida")
		return
	}

	udpAddr, err := net.ResolveUDPAddr("udp", peerIp+":"+port)

	if err != nil {
		fmt.Println("ERRO: endereço inválido")
		fmt.Println(err.Error())
		return
	}

	udpConn, err = net.ListenUDP("udp", udpAddr)

	if err != nil {
		fmt.Println("ERRO: não foi possível abrir o socket UDP")
		fmt.Println(err.Error())
		return
	}

	tcpAddr, _ := net.ResolveTCPAddr("tcp", udpAddr.String())
	tcpListener, err := net.ListenTCP("tcp", tcpAddr)

	if err != nil {
		fmt.Println("ERRO: não foi possível abrir o socket TCP")
		fmt.Println(err.Error())
		udpConn.Close()
		return
	}

	// UDP thread
	go handleUDPMessages(inbox)

	// TCP thread
	go waitForTCP(tcpListener)

	msg := message.Message{
		Command: "JOIN",
		Files:   files,
	}

	_, err = makeRequest(msg, inbox)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Printf("Sou peer %s com arquivos", udpAddr.String())
	for _, file := range files {
		fmt.Print(" ", file)
	}
	fmt.Println()

	initialized = true
}

func search(inbox chan message.Message) {
	fmt.Print("  arquivo: ")
	fmt.Scan(&fileToDownload)

	msg := message.Message{
		Command: "SEARCH",
		File:    fileToDownload,
	}

	res, err := makeRequest(msg, inbox)

	if err != nil {
		return
	}

	peersWithFile = res.Files

	fmt.Printf("  peers com arquivo solicitado:")
	for _, peer := range res.Files {
		fmt.Print(" ", peer)
	}
	fmt.Println()
}

func download(inbox chan message.Message) {

	if fileToDownload == "" {
		fmt.Println("Use o comando SEARCH para escolher um arquivo")
		return
	}

	var remotePeer string

	fmt.Print("  peer (ip:port): ")
	fmt.Scan(&remotePeer)

	success := downloadFile(remotePeer, fileToDownload)

	if success {
		fmt.Println("Arquivo", fileToDownload, "baixado com sucesso na pasta", peerFolder)
		update(fileToDownload, inbox)
	}
}

func leave(inbox chan message.Message) {
	makeRequest(message.Message{Command: "LEAVE"}, inbox)
	os.Exit(0)
}

/****************
 * UDP Handling *
 ***************/

func update(newFile string, inbox chan message.Message) {
	makeRequest(message.Message{Command: "UPDATE", File: newFile}, inbox)
}

func handleUDPMessages(inbox chan message.Message) {
	for {
		buffer := make([]byte, 1024)
		n, addr, _ := udpConn.ReadFromUDP(buffer)
		req, err := message.MessageFromByteArray(buffer[:n])

		if err != nil {
			continue
		}

		if req.Command == "ALIVE" {
			res := message.Message{Command: "ALIVE_OK"}
			encRes, _ := res.ToByteArray()
			udpConn.WriteToUDP(encRes, addr)
		} else {
			select {
			case inbox <- req:
			default:
			}
		}
	}
}

// makeRequest sends a Message to the server, waits and return the response
func makeRequest(req message.Message, inbox chan message.Message) (message.Message, error) {

	sendId := messageCounter
	req.Id = messageCounter
	messageCounter++

	success := make(chan bool)
	timeout := make(chan bool)
	var timedOut bool

	go sendAndWaitForReply(req, success, timeout)

	var res message.Message

	for {
		select {
		case res = <-inbox:
		case timedOut = <-timeout:
		}

		if res.Id == sendId {
			success <- true
			return res, nil
		}

		if timedOut {
			break
		}
	}

	return message.Message{}, errors.New("falha de comunicação")
}

func sendAndWaitForReply(req message.Message, success chan bool, timeout chan bool) {
	for i := REQUEST_RETRIES; i > 0; i-- {
		encodedReq, _ := req.ToByteArray()
		udpConn.WriteToUDP(encodedReq, SERVER_ADDR)

		var ok bool
		delay := make(chan bool)

		go func() {
			time.Sleep(REQUEST_RETRY_DELAY * time.Second)
			delay <- true
		}()

		if i > 1 {
			select {
			case ok = <-success:
			case <-delay:
			}

			if ok {
				return
			}
		}
	}

	timeout <- true
}

/****************
 * TCP Handling *
 ***************/

func waitForTCP(listener net.Listener) {
	for {
		tcpConn, _ := listener.Accept()
		go handleTCPConn(tcpConn)
	}
}

func downloadFile(remotePeer string, fileName string) (success bool) {
	initialPeer := findInSlice(peersWithFile, remotePeer)
	if initialPeer == -1 {
		fmt.Printf("Peer %s não possui o arquivo %s\n", remotePeer, fileName)
		return
	}

	var err error
	success, err = loopDownload(initialPeer, fileName)
	if err != nil {
		fmt.Println(err.Error())
	}

	return
}

func loopDownload(initialPeer int, fileName string) (success bool, err error) {
	for i := 0; i < DOWNLOAD_RETRIES; i++ {
		currentPeer := initialPeer
		for {
			success, err := downloadFileFromPeer(peersWithFile[currentPeer], fileName)
			if err != nil {
				return false, err
			}

			if success {
				return true, nil
			}

			nextPeer := currentPeer + 1
			if nextPeer == len(peersWithFile) {
				nextPeer = 0
			}

			fmt.Printf("peer %s negou o download, pedindo agora para o peer %s\n", peersWithFile[currentPeer], peersWithFile[nextPeer])

			if nextPeer == initialPeer {
				break
			}

			currentPeer = nextPeer
		}

		if i < DOWNLOAD_RETRIES-1 {
			time.Sleep(DOWNLOAD_RETRY_DELAY * time.Second)
		}
	}
	return false, nil
}

func downloadFileFromPeer(peerAddr string, fileName string) (success bool, err error) {
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return false, nil // treat connection error as rejection
	}
	defer conn.Close()

	req := message.Message{Command: "DOWNLOAD", File: fileName}
	encodedReq, _ := req.ToByteArray()
	conn.Write(encodedReq)

	buffer := make([]byte, 1024)
	n, _ := conn.Read(buffer)

	res, err := message.MessageFromByteArray(buffer[:n])

	if err == nil {
		if res.Command == "DOWNLOAD_NEGADO" {
			return false, nil
		}
		return false, errors.New("resposta inesperada")
	}

	file, err := os.Create(path.Join(peerFolder, fileName)) // cria arquivo
	if err != nil {
		fmt.Println(err.Error())
		return false, err
	}
	defer file.Close()

	_, err = file.Write(buffer[:n])
	if err != nil {
		return false, err
	}

	for {
		n, err := conn.Read(buffer)

		if err != nil {
			if err == io.EOF {
				break // termina se receber EOF
			}
			return false, err
		}

		_, err = file.Write(buffer[:n])
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func handleTCPConn(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 1024)

	n, _ := conn.Read(buffer)
	req, _ := message.MessageFromByteArray(buffer[:n])
	fileName := req.File

	if rand.Intn(2) == 0 {
		res := message.Message{Command: "DOWNLOAD_NEGADO"}
		encodedRes, _ := res.ToByteArray()
		conn.Write(encodedRes)
		return
	}

	file, err := os.Open(path.Join(peerFolder, fileName))
	if err != nil {
		fmt.Println(err.Error())
		res := message.Message{Command: "DOWNLOAD_NEGADO"}
		encodedRes, _ := res.ToByteArray()
		conn.Write(encodedRes)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	for {
		n, err := reader.Read(buffer)

		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println(err.Error())
			return
		}

		conn.Write(buffer[:n])
	}
}

/***********
 * Helpers *
 **********/

func listFilesInDir(dir string) (files []string, err error) {
	files = make([]string, 0)
	entries, err := os.ReadDir(dir)

	if err != nil {
		return
	}

	for _, v := range entries {
		if !v.IsDir() {
			files = append(files, v.Name())
		}
	}

	return
}

func findInSlice(slice []string, value string) int {
	for i, v := range slice {
		if v == value {
			return i
		}
	}
	return -1
}
