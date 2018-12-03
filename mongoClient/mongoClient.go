package mongoClient

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"sync"
)


func StartFromFirstPosition(count int,waitGroup * sync.WaitGroup,sourceCollection *mgo.Collection,batchSize int,ch chan []interface{}){
	fmt.Println("FromFirstPosition===============count:",count)
	var lastObjId bson.ObjectId
	runFromStartPosition(count,lastObjId,batchSize,sourceCollection,ch)
	waitGroup.Done()
}

func runFromStartPosition(count int,lastObjId bson.ObjectId, batchSize int,sourceCollection *mgo.Collection,ch chan []interface{})  {
	var sourceResult []interface{}
	fmt.Println("FromFirstPosition===============count is:",count)
	fmt.Println("FromFirstPosition=============lastObjId:",lastObjId)
	//从头开始去处理
	firstBatchSize := batchSize
	if count < batchSize {
		firstBatchSize = count
	}
	//lastObjId.Hex()
	if lastObjId.Hex() == ""{
		//证明是第一个
		//fmt.Println(sourceCollection,"**********=======*************")
		err := sourceCollection.Find(bson.M{}).Sort("_id").Limit(firstBatchSize).All(&sourceResult)
		if err != nil {
			log.Fatal("get from start error!")
		}
	}else {
		if count <= 0 {
			return
		}
		err := sourceCollection.Find(bson.M{"_id":bson.M{"$gt":lastObjId}}).Sort("_id").Limit(firstBatchSize).All(&sourceResult)
		if err != nil {
			log.Fatal("get data error!")
		}
	}
	len := len(sourceResult)
	if len <= 0{
		return
	}
	//bson.ObjectIdHex()
	//顺序的最后一条记录
	firstResult := sourceResult[len - 1]
	//fmt.Println("=====FromStart:",firstResult)
	lastObjId = firstResult.(bson.M)["_id"].(bson.ObjectId)
	ch <- sourceResult
	if count -batchSize > 0 {
		runFromStartPosition(count -batchSize,lastObjId,batchSize,sourceCollection,ch)
	}
}

func StartFromEndPosition(count int,waitGroup * sync.WaitGroup,sourceCollection *mgo.Collection,batchSize int,ch chan []interface{}){
	//fmt.Println("FromEndPosition===============count:",count)
	var firstObjId bson.ObjectId
	runFromEndPosition(count,firstObjId,sourceCollection,batchSize,ch)
	waitGroup.Done()
}

func runFromEndPosition(count int,firstObjId bson.ObjectId,sourceCollection *mgo.Collection,batchSize int,ch chan []interface{}){
	//从结束的地方开始查找,是在另一个协程中处理的
	var sourceResult []interface{}
	fmt.Println("FromEndPosition===============count is:",count)
	fmt.Println("=====FromEnd:",firstObjId)
	//从尾开始去处理
	firstBatchSize := batchSize
	if count < batchSize {
		firstBatchSize = count
	}
	if firstObjId.Hex() == "" {
		err := sourceCollection.Find(bson.M{}).Sort("-_id").Limit(firstBatchSize).All(&sourceResult)
		if err != nil {
			log.Fatal(err)
		}
	}else {
		err := sourceCollection.Find(bson.M{"_id":bson.M{"$lt":firstObjId}}).Sort("-_id").Limit(firstBatchSize).All(&sourceResult)
		if err != nil {
			log.Fatal(err)
		}
	}
	len := len(sourceResult)
	if len <= 0 {
		return
	}
	fmt.Println("++++EndPosition:",len)
	ch <- sourceResult
	if count - batchSize > 0 {
		//bson.ObjectIdHex()
		//fmt.Println("=====FromEnd:",sourceResult)
		//顺序的最后一条记录
		firstResult := sourceResult[len - 1]
		//fmt.Println("=====FromEnd:",firstResult)
		firstObjId = firstResult.(bson.M)["_id"].(bson.ObjectId)
		runFromEndPosition(count - batchSize,firstObjId,sourceCollection,batchSize,ch)
	}

}



/**
 panic 官方文档介绍：panic 是用来停止当前程序的执行。当一个方法调用panic。 当函数F调用panic时，F的正常执行立即停止。 但是任何有F推迟的函数都会运行，意思是F定义有defer关键字声明的函数会执行,然后F返回给它的调用者。 对于调用者G来说，F的调用就像调用panic 一样，终止G的执行并运行任何延迟(带有defer 关键字)的函数。 这种情况会持续下去，直到正在执行的goroutine中的所有功能都以相反的顺序停止。 此时，程序终止并报告错误情况，包括panic的参数值。最后这种情况可以通过调用recover 来恢复函数的运行。
函数 recover 介绍: recover内置函数允许一段程序管理一个正在paincing goroutine的行为。

在defer 定义的函数（不是由它调用的任何函数）内部执行一段recover 函数，通过recover函数执行来停止panic 函数的执行，并且可以找出给panic所传递的错误值。 如果在defer 函数之外调用恢复，它不会停止panic的执行。 在这种情况下，或者当goroutine没有panicing时，或者提供给panicing的参数为零时，恢复返回nil。 因此，recover函数的返回值报告协程是否正在遭遇panicing 。

panic函数就是往外扔错误，一层接一层往上扔直到当前程序不能运行为止，不想让panic 函数扔的错误导致程序挂掉，
就得使用recover 函数来接收panic 错误或者说是阻挡panicing ，并且recover 函数可以将错误转化为error 类型。
因为panic 错误不会让defer 关键字定义的函数也停止运行,就是说defer 关键字声明的函数或者代码即使遇到错误也会执行。
一个函数里面有defer 关键字声明一个函数(假设叫catch 函数)和要运行出错的代码，在catch 函数里面调用recover 函数。
recover 会拦截错误，不会让错误往上扔，返回给调用者error（里面有错误的信息）类型 ，从而使goroutine 不挂掉。

 */
func InitDB(url string, c string) (*mgo.Session, *mgo.Collection){
	dialInfo,err := mgo.ParseURL(url)
	if err != nil {
		log.Fatal(err.Error())
	}

	server, err := mgo.Dial(url)
	if err != nil {
		panic(err)
	}

	collection := server.DB(dialInfo.Database).C(c)

	return server, collection
}
