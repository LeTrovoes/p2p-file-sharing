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

const UDP_BUFFER_SIZE = 1024 // max message size in bytes
const TCP_BUFFER_SIZE = 1024 // max packet size in bytes

const REQUEST_RETRIES = 3       // retries to server
const REQUEST_RETRY_DELAY = 10  // retry interval to server
const DOWNLOAD_RETRIES = 3      // retries to other peers
const DOWNLOAD_RETRY_DELAY = 10 // retry interval to other peers

const SERVER_ADDR_STR = "127.0.0.1:10098"

var (
	udpConn     *net.UDPConn
	peerFolder  string
	initialized bool // whether the peer has already opened sockets and is accepting requests

	fileToDownload string
	peersWithFile  []string

	SERVER_ADDR *net.UDPAddr

	messageCounter int // used to match requests to responses from server
)

func main() {
	initialized = false
	messageCounter = 1
	rand.Seed(time.Now().UnixNano()) // usa o relógio para inicializar rand

	SERVER_ADDR, _ = net.ResolveUDPAddr("udp", SERVER_ADDR_STR)

	// canal para comunicação entre goroutine do UDP e a principal
	inbox := make(chan message.Message)

	// menu no console na goroutine principal
	menu(inbox)
}

// recebe comandos do console e encaminha para o handler correspondente
func menu(inbox chan message.Message) {
	join(inbox) // inicia com um JOIN (REF 5l)

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

// REF 5b
func join(inbox chan message.Message) {

	// ignora se o peer já tiver sido inicializado
	if initialized {
		fmt.Println("  peer já inicializado")
		return
	}

	// recebe ip
	var peerIp string
	fmt.Print("  ip: ")
	fmt.Scan(&peerIp)

	// recebe porta
	var port string
	fmt.Print("  porta: ")
	fmt.Scan(&port)

	// recebe pasta
	fmt.Print("  pasta: ")
	fmt.Scan(&peerFolder)

	// valida pasta
	files, err := listFilesInDir(peerFolder)

	if err != nil {
		fmt.Println("ERRO: pasta invalida")
		return
	}

	// resolve endereço
	udpAddr, err := net.ResolveUDPAddr("udp", peerIp+":"+port)

	if err != nil {
		fmt.Println("ERRO: endereço inválido")
		fmt.Println(err.Error())
		return
	}

	// abre socket UDP
	udpConn, err = net.ListenUDP("udp", udpAddr)

	if err != nil {
		fmt.Println("ERRO: não foi possível abrir o socket UDP")
		fmt.Println(err.Error())
		return
	}

	// abre socket TCP na mesma porta
	tcpAddr, _ := net.ResolveTCPAddr("tcp", udpAddr.String())
	tcpListener, err := net.ListenTCP("tcp", tcpAddr)

	if err != nil {
		fmt.Println("ERRO: não foi possível abrir o socket TCP")
		fmt.Println(err.Error())
		udpConn.Close()
		return
	}

	// inicia go routine para mensagens UDP do servidor (REF 5a)
	go handleUDPMessages(inbox)

	// inicia go routine para mensagens TCP dos peers (REF 5a)
	go waitForTCP(tcpListener)

	// envia JOIN para o servidor e espera a resposta (REF 5b)
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

// REF 5f
func search(inbox chan message.Message) {
	// recebe nome do arquivo do console
	fmt.Print("  arquivo: ")
	fmt.Scan(&fileToDownload)

	// envia SEARCH para o servidor e espera a resposta (REF 5f)
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

// REF 5h
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
		update(fileToDownload, inbox) // envia UPDATE com o novo arquivo (REF 5d)
	}
}

// REF 5c
func leave(inbox chan message.Message) {
	// envia LEAVE para o servidor e espera a resposta (REF 5c)
	makeRequest(message.Message{Command: "LEAVE"}, inbox)
	os.Exit(0)
}

/****************
 * UDP Handling *
 ***************/

// REF 5d
func update(newFile string, inbox chan message.Message) {
	// envia UPDATE para o servidor e espera a resposta (REF 5d)
	makeRequest(message.Message{Command: "UPDATE", File: newFile}, inbox)
}

// recebe mensagens UDP do servidor
func handleUDPMessages(inbox chan message.Message) {
	for {
		buffer := make([]byte, UDP_BUFFER_SIZE)
		n, addr, _ := udpConn.ReadFromUDP(buffer)            // bloqueia até receber uma mensagem
		req, err := message.MessageFromByteArray(buffer[:n]) // decodifica a mensagem

		if err != nil {
			// ignora mensagens que não puderem ser decodificadas
			continue
		}

		if req.Command == "ALIVE" {
			// envia ALIVE_OK para o servidor (REF 5e)
			res := message.Message{Command: "ALIVE_OK"}
			encRes, _ := res.ToByteArray()
			udpConn.WriteToUDP(encRes, addr)
		} else {
			select {
			case inbox <- req: // envia mensagem para o canal apenas se a goroutine principal estiver esperando
			default: // se não estivar esperando, descarta mensagem
			}
		}
	}
}

// REF 5g: envia uma mensagem ao servidor e espera a resposta
func makeRequest(req message.Message, inbox chan message.Message) (message.Message, error) {

	// atribui identificador a mensagem
	sendId := messageCounter
	req.Id = messageCounter
	messageCounter++

	success := make(chan bool) // canal para indicar que a respota esperada chegou
	timeout := make(chan bool) // canal para indicar que o tempo de espera terminou
	var timedOut bool

	// envia e reenvia a mesma mensagem até que a resposta chegue ou o tempo acabe
	go sendAndRetry(req, success, timeout)

	var res message.Message

	for {
		// espera por uma resposta ou pelo esgotamento do tempo, o que acontecer primeiro
		select {
		case res = <-inbox:
		case timedOut = <-timeout:
		}

		// verifica se é a respota correta
		if res.Id == sendId {
			success <- true // avisa a goroutine para parar o reenvio
			return res, nil
		}

		if timedOut {
			break // sai se o tempo tiver esgotado
		}
	}

	return message.Message{}, errors.New("falha de comunicação")
}

// REF 5g: envia a mesma mensagem, entre intervalos de tempo, por uma determinada quantidade de vezes
func sendAndRetry(req message.Message, success chan bool, timeout chan bool) {
	for i := REQUEST_RETRIES; i > 0; i-- {
		// envia a mensagem
		encodedReq, _ := req.ToByteArray()
		udpConn.WriteToUDP(encodedReq, SERVER_ADDR)

		var ok bool
		delay := make(chan bool) // canal para a espera do intervalo

		// goroutine para espera do intervalo
		go func() {
			time.Sleep(REQUEST_RETRY_DELAY * time.Second)
			delay <- true
		}()

		if i > 1 {
			select { // espera o que acontecer primeiro:
			case ok = <-success: // a outra goroutine indicar que a resposta chegou
			case <-delay: // o intervalo de tempo passar
			}

			if ok {
				return // sai se a resposta tiver chegado
			}
		}
	}

	timeout <- true // avisa que as tentativas acabaram
}

/*****************
 * File Download *
 ****************/

// REF 5h: começa operação de download de outro peer
func downloadFile(remotePeer string, fileName string) (success bool) {
	// verifica se o peer informado no console possui o arquivo
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

// REF 5k: tenta encontrar um peer que aceite o download, a começar pelo que foi informado via console
func loopDownload(initialPeer int, fileName string) (success bool, err error) {
	// retenta a operação abaixo por uma quantidade determinada de vezes
	for i := 0; i < DOWNLOAD_RETRIES; i++ {
		currentPeer := initialPeer
		// percorre a lista de peers que possuem o arquivo
		for {

			// tenta baixar do peer atual
			success, err := downloadFileFromPeer(peersWithFile[currentPeer], fileName)
			if err != nil {
				return false, err
			}

			if success {
				return true, nil // retorna sucesso se conseguir baixar
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
			// espera um tempo entre cada percorrida da lista
			time.Sleep(DOWNLOAD_RETRY_DELAY * time.Second)
		}
	}
	return false, nil
}

func downloadFileFromPeer(peerAddr string, fileName string) (success bool, err error) {
	// tenta se conectar ao peer remoto
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return false, nil // trata um erro de conexão como uma rejeição
	}
	defer conn.Close()

	// envia requisição de download
	req := message.Message{Command: "DOWNLOAD", File: fileName}
	encodedReq, _ := req.ToByteArray()
	conn.Write(encodedReq)

	// espera resposta
	buffer := make([]byte, TCP_BUFFER_SIZE)
	n, _ := conn.Read(buffer)

	// tenta decodificar resposta como uma mensagem
	res, err := message.MessageFromByteArray(buffer[:n])

	if err == nil {
		// retorna se receber uma mensagem como resposta
		if res.Command == "DOWNLOAD_NEGADO" {
			return false, nil
		}
		return false, errors.New("resposta inesperada")
	}
	// se a respota não puder ser decodificada, interpreta como parte do arquivo

	// REF 5j: cria arquivo na pasta do socket
	file, err := os.Create(path.Join(peerFolder, fileName))
	if err != nil {
		fmt.Println(err.Error())
		return false, err
	}
	defer file.Close()

	_, err = file.Write(buffer[:n]) // escreve parte já recebida
	if err != nil {
		return false, err
	}

	for {
		n, err := conn.Read(buffer) // le chunk que chega no socket

		if err != nil {
			if err == io.EOF {
				break // termina se receber EOF
			}
			return false, err
		}

		_, err = file.Write(buffer[:n]) // escreve chunk no arquivo
		if err != nil {
			return false, err
		}
	}

	return true, nil // retorna sucesso
}

/****************
 * TCP Handling *
 ***************/

// espera por conexões de outros peers
func waitForTCP(listener net.Listener) {
	for {
		tcpConn, _ := listener.Accept()
		go handleTCPConn(tcpConn) // goroutine para tratar requisição
	}
}

// REF 5i: trata requisições de outros peers
func handleTCPConn(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, TCP_BUFFER_SIZE)

	n, _ := conn.Read(buffer)                          // bloqueia até ler mensagem
	req, _ := message.MessageFromByteArray(buffer[:n]) // decodifica mensagem
	fileName := req.File

	if rand.Intn(2) == 0 {
		// rejeita download aleatóriamente
		res := message.Message{Command: "DOWNLOAD_NEGADO"}
		encodedRes, _ := res.ToByteArray()
		conn.Write(encodedRes)
		return
	}

	file, err := os.Open(path.Join(peerFolder, fileName))
	if err != nil {
		// rejeita download se não tiver (ou não conseguir abrir) o arquivo
		res := message.Message{Command: "DOWNLOAD_NEGADO"}
		encodedRes, _ := res.ToByteArray()
		conn.Write(encodedRes)
		return
	}
	defer file.Close()

	// envia o arquivo em pedaços via TCP
	// ordem e integridade são garantidas pelo TCP
	reader := bufio.NewReader(file)
	for {
		n, err := reader.Read(buffer) // le um chunk do arquivo

		if err != nil {
			if err == io.EOF {
				break // encerra o envio quando a leitura do arquivo terminar (EOF)
			}
			fmt.Println(err.Error())
			return // retorna caso ocorra algum erro inesperado na leitura
		}

		conn.Write(buffer[:n]) // envia o chunk lido
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
