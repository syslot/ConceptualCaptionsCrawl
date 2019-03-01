package main

import (
	"os"
	"bufio"
	"log"
	"strings"
	"flag"
	"net/http"
	"io"
	"github.com/google/uuid"
	"encoding/json"
	"sync"
	"crypto/tls"
	"gopkg.in/cheggaaa/pb.v1"
)

type Record struct{
	Caption string `json:"caption"`
	ImageUrl string `json:"image_url"`
	Uuid string `json:"tag"`
	//Image   []byte `json:"-"`
}


var bar *pb.ProgressBar = nil

func taskBuilder(srcPath string, ch chan *Record, wg * sync.WaitGroup, wg1 *sync.WaitGroup){
	file, err := os.Open(srcPath)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()
	defer wg.Done()

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        cmds:=strings.Split(scanner.Text(), "\t")
        r := &Record{Caption: cmds[0], ImageUrl:cmds[1]}

        wg1.Add(1)

        ch <- r
    }
    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }
}

func getImage(tgtPath string, ch chan * Record, stop chan interface{}, rs io.Writer, wg1 *sync.WaitGroup){

	tr := &http.Transport{
        TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
    }
    client := &http.Client{Transport: tr}

	for {
		select {
		case task := <-ch:
			bar.Increment()
			wg1.Done()
			response, err := client.Get(task.ImageUrl)
			if err != nil {
				log.Printf("get image %v failed at %v", task.ImageUrl, err)
				continue
			}
			task.Uuid = uuid.New().String()
			file, err := os.Create(tgtPath + "/" + task.Uuid + ".jpg")
			if err != nil {
				log.Print(err)
				file.Close()
				continue
			}

			_, err = io.Copy(file, response.Body)
			response.Body.Close()
			if err != nil {
				log.Print(err)
				file.Close()
			}
			strs,_ := json.Marshal(task)
			rs.Write(strs)
			rs.Write([]byte("\n"))
			file.Close()

			case <- stop:
				break
		}
	}
}


func main(){
	var srcPath = flag.String("srcPath", "Validation_GCC-1.1.0-Validation.tsv", "source tsv path")
	var ImagePath = flag.String("imagePath", "val", "image save path")
	var recPath = flag.String("recordPath",  "val.txt", "record save path")
	var worker = flag.Int("num", 1, "parallel number")
	var logPath = flag.String("log", "scral.log", "log2file")
	flag.Parse()

	if *ImagePath == "val"{
		bar = pb.StartNew(15840)
	}else{
		bar = pb.StartNew(3318333)
	}
	//bar.Callback = func(s string) { fmt.Println(s) }

	f, err := os.OpenFile(*logPath, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
	if err != nil {
    	log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)

	var chRecord = make(chan *Record, 100)
	var chStop = make(chan interface{})

	wg := &sync.WaitGroup{}
	wg.Add(1)
	wg1 := &sync.WaitGroup{}

	fout, _ := os.Create(*recPath)
	defer fout.Close()

	for i:=0;i< *worker;i++ {
		go getImage(*ImagePath, chRecord, chStop, fout, wg1)
	}
	go taskBuilder(*srcPath, chRecord, wg, wg1)
	wg.Wait()
	wg1.Wait()
}
