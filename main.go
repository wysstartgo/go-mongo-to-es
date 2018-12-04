package main

import (
	"flag"
	"fmt"
	"github.com/wysstartgo/go-mongo-to-es/elastic"
	"github.com/wysstartgo/go-mongo-to-es/mongoClient"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"reflect"
	"strconv"
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

var index = ""
var docType = ""


func main(){
	//time.Now().Nanosecond();

	//索引重建操作
	cfg := new(elastic.ClientConfig)
	cfg.Addr = fmt.Sprintf("%s:%d", *host, *port)
	cfg.User = ""
	cfg.Password = ""
	c = elastic.NewClient(cfg)

	index = "rt_group"
	docType =  "group"
	//mapingMap,err := ioutil.ReadFile("F:\\goworkspace\\go-mongo-to-es\\group.mapping")
	//if err != nil{
	//	fmt.Println(err)
	//}
	//err = c.CreateMappingByFile(index, docType, mapingMap)
	//if err != nil {
	//	fmt.Println("-----------------------------")
	//	fmt.Println(err)
	//	fmt.Println("-----------------------------")
	//}

	//从mongodb取数据来建索引
	retriveDataAndIndex(nil)
}

func retriveDataAndIndex(err error) {
	_, sourceCollection = mongoClient.InitDB("mongodb://zx:zx123456@192.168.1.133:20001,192.168.1.134:20001,192.168.1.135:20001/zx", "business_group_base_info")
	var controlWaitGroup sync.WaitGroup
	for i := 1; i <= WorkerCount; i++ {
		controlWaitGroup.Add(1)
		//初始化几个worker
		go work(ch, &controlWaitGroup)
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
	go mongoClient.StartFromFirstPosition(half, &waitGroup, sourceCollection, BatchSize, ch)
	log.Println("start from end position!")
	waitGroup.Add(1)
	if count%2 == 0 {
		go mongoClient.StartFromEndPosition(half, &waitGroup, sourceCollection, BatchSize, ch)
	} else {
		//从后向前多查询一个
		go mongoClient.StartFromEndPosition(half+1, &waitGroup, sourceCollection, BatchSize, ch)
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
			buildGroupIndex(task)
		case <- time.After(time.Second * 5):
			fmt.Println("数据已经处理完毕，关闭协程!")
			workWaitGroup.Done()
			isStop = true
			break
		}
	}
}



/**
	创建圈子索引
 */
func buildGroupIndex(data []interface{}){
	length := len(data)
	items := make([]*elastic.BulkRequest,length)
	for i := 0; i < length; i++ {
		bulkRequest := new(elastic.BulkRequest)
		bulkRequest.Action = elastic.ActionCreate
		rowData := data[i]
		//log.Println("==============",rowData)
		//获取id
		groupId := rowData.(bson.M)["groupId"].(int64)
		bulkRequest.ID = strconv.FormatInt(groupId,10)
		//bulkRequest.Index = index
		//bulkRequest.Type = docType
		groupData := make(map[string]interface{})
		groupData["id"] = groupId
		groupData["title"] = rowData.(bson.M)["title"]
		groupData["brief"] = rowData.(bson.M)["brief"]
		groupData["tags_ids"] = rowData.(bson.M)["tagIds"]
		groupData["tags_names"] = rowData.(bson.M)["tagNames"]
		groupData["is_free"] = rowData.(bson.M)["isFree"].(int)
		//groupData["price"] = rowData.(bson.M)["price"]
		//groupData["author"] = rowData.(bson.M)["author"]
		//createTime := time.Now().Format(rowData.(bson.M)["createTime"].(string))
		groupData["publish_time"] = rowData.(bson.M)["createTime"].(time.Time).UnixNano()/1e6
		region := rowData.(bson.M)["region"]
		if region != nil{
			groupData["region"] = region.(int)
		}

		groupData["category_id"] = rowData.(bson.M)["categoryId"]
		groupData["category_name"] = rowData.(bson.M)["categoryName"]
		totalMember := rowData.(bson.M)["totalMember"]

		groupData["total_members"] = totalMember

		//switch totalMember.(type) {
		//case int:
		//
		//}
		fmt.Println(reflect.TypeOf(totalMember))
		//groupData["attr1"] = rowData.(bson.M)["attr1"]
		//groupData["attr2"] = rowData.(bson.M)["attr2"]
		//groupData["score"] = rowData.(bson.M)["score"]
		bulkRequest.Data = groupData
		items[i] = bulkRequest
	}
	//保存
	resp,err := c.IndexTypeBulk(index,docType,items)
	if err != nil {
		log.Println(err,resp.Code,":",resp.Errors)
	}else{
		log.Println(resp.Code)
	}
}

