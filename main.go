package main

import (
	"conf"
	"elastic"
	"flag"
	"fmt"
	"gopkg.in/mgo.v2"
	"io/ioutil"
	"mongoClient"
)

var host = flag.String("host", "192.168.1.38", "Elasticsearch host")
var port = flag.Int("port", 9200, "Elasticsearch port")
var c *elastic.Client
//批处理的数量
const BatchSize  = 500
var (
	configFile string
	sourceCollection *mgo.Collection
	targetCollection *mgo.Collection
)


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
	_, sourceCollection = mongoClient.initDB(conf.GConf.SourceMongoUrl, conf.GConf.SourceCollection)








}
