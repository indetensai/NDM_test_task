package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type queue struct {
	input  chan string
	output chan string
}

var queues map[string]queue
var mu sync.RWMutex

func GetQueue(queueName string) (input chan string, output chan string) {
	mu.RLock()
	temp, ok := queues[queueName]
	if ok {
		defer mu.RUnlock()
		return temp.input, temp.output
	} else {
		mu.RUnlock()
		mu.Lock()
		defer mu.Unlock()
		temp, ok := queues[queueName]
		if ok {
			return temp.input, temp.output
		}
		temp = queue{input: make(chan string), output: make(chan string)}
		go func() {
			messages := make([]string, 0, 16)
			defer close(temp.output)
			for {
				var s string
				if len(messages) > 0 {
					s = messages[0]
				inner:
					for {
						select {
						case s, ok := <-temp.input:
							if !ok {
								temp.input = nil
								panic("da")
							}
							messages = append(messages, s)
						case temp.output <- s:
							messages = messages[1:]
							break inner
						}
					}
				} else {
					s, ok := <-temp.input
					if !ok {
						temp.input = nil
						panic("da")
					}
					messages = append(messages, s)
				}

			}
		}()
		queues[queueName] = temp
		return temp.input, temp.output
	}
}

func PutInQueueHandler(w http.ResponseWriter, r *http.Request) {
	queueName := strings.TrimPrefix(r.URL.Path, "/")
	message := r.URL.Query().Get("v")
	if message == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	input, _ := GetQueue(queueName)
	input <- message
	w.WriteHeader(http.StatusOK)
}

func GetFromQueueHandler(w http.ResponseWriter, r *http.Request) {
	queueName := strings.TrimPrefix(r.URL.Path, "/")
	timeout := r.URL.Query().Get("timeout")
	duration, err := strconv.Atoi(timeout)
	if timeout != "" {
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}
	_, output := GetQueue(queueName)
	select {
	case result := <-output:
		data, err := json.Marshal(result)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(data)
		return
	case <-time.After(time.Duration(duration) * time.Second):
		w.WriteHeader(http.StatusNotFound)
		return
	}
}

func main() {
	queues = make(map[string]queue)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			PutInQueueHandler(w, r)
		case http.MethodGet:
			GetFromQueueHandler(w, r)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	var port string
	if len(os.Args) > 1 {
		port = os.Args[1]
	} else {
		port = "8080"
	}
	fmt.Printf("server listening at %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
