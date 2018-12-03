package main

import (
	"conf"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go/bson"
	"github.com/wysstartgo/go-mongo-to-es/elastic"
	"flag"
	"fmt"
	"gopkg.in/mgo.v2"
	"io/ioutil"
	"github.com/wysstartgo/go-mongo-to-es/mongoClient"
	"sync"
	"time"
)

var host = flag.String("host", "192.168.1.38", "Elasticsearch host")
var port = flag.Int("port", 9200, "Elasticsearch port")
var c *elastic.Client
//批处理的数量
const BatchSize  = 500
var (
	configFile string
	sourceCollection *mgo.Collection
)
//创建缓冲区
var ch = make(chan [] interface{},1000)

const WorkerCount  = 5        //worker数量


func main(){
	//索引重建操作
	cfg := new(elastic.ClientConfig)
	cfg.Addr = fmt.Sprintf("%s:%d", *host, *port)
	cfg.User = ""
	cfg.Password = ""
	c = elastic.NewClient(cfg)

	index := "rt_group"
	docType :=  "blog"
	mapingMap,err := ioutil.ReadFile("F:\\goworkspace\\go-zm-dev\\src\\cc\\group.mapping")
	if err != nil{
		fmt.Println(err)
	}
	err = c.CreateMappingByFile(index, docType, mapingMap)
	if err != nil {
		fmt.Println(err)
	}

	//从mongodb取数据来建索引
	_, sourceCollection = mongoClient.InitDB(conf.GConf.SourceMongoUrl, conf.GConf.SourceCollection)
	var controlWaitGroup sync.WaitGroup

	for i := 1 ; i <= WorkerCount; i++{
		controlWaitGroup.Add(1)
		//初始化几个worker
		go work(ch,&controlWaitGroup)
	}

	//fmt.Println(sourceCollection,"***********************")
	pipe := sourceCollection.Pipe([]bson.M{{"$count": "count"}})
	resp := []bson.M{}
	err = pipe.All(&resp)
	if err != nil {
		fmt.Println("pipe control error!")
	}
	count := resp[0]["count"].(int)
	//count, error := sourceCollection.Count()
	//if error != nil {
	//	panic(error)
	//}
	//分页获取
	//获取方式为两个协程，一个从前往后获取，一个从后向前获取，每个协程负责一半的工作量
	if count == 0 {
		log.Println("no data in this collection , please check it!")
	}
	//queue := make(chan[] int,2)
	var waitGroup sync.WaitGroup
	half := count / 2
	waitGroup.Add(1)
	log.Println("start from first position!")
	//time.Sleep(time.Second * 2)
	go mongoClient.StartFromFirstPosition(half, &waitGroup)
	log.Println("start from end position!")
	waitGroup.Add(1)
	if count % 2 == 0 {
		go mongoClient.StartFromEndPosition(half, &waitGroup)
	} else {
		//从后向前多查询一个
		go mongoClient.StartFromEndPosition(half+1, &waitGroup)
	}
	waitGroup.Wait()
	fmt.Println("**************************")
	controlWaitGroup.Wait()
}

/**
  注意：在这里需要传指针，不能传变量
 */
func work(ch chan []interface{},workWaitGroup *sync.WaitGroup){
	var isStop = false
	for{
		if isStop{
			break
		}
		//接收任务
		select {
		case task := <- ch:
			writeTargetData(task)
		case <-time.After(time.Second * 5):
			fmt.Println("数据已经处理完毕，关闭协程!")
			workWaitGroup.Done()
			isStop = true
			break
		}

	}
}
