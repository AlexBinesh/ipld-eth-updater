package main

import (
  "database/sql"
  "fmt"
  "encoding/json"
  "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
  _ "github.com/lib/pq"
  //"github.com/kr/pretty"
)

const (
  host     = "localhost"
  port     = 8077
  user     = "vdbm"
  password = "password"
  dbname   = "vulcanize_public"
)
const (
  host_prod     = "localhost"
  prod_port     = 8078
  prod_user     = "vdb_read"
  prod_password = "8Ew0zUdwaHohAwLm"
  dbname_prod   = "vulcanize_public"
)
type Log struct {
	// Consensus fields:
	// address of the contract that generated the event
	Address common.Address `json:"address" gencodec:"required"`
	// list of topics provided by the contract.
	Topics []common.Hash `json:"topics" gencodec:"required"`
	// supplied by the contract, usually ABI-encoded
	Data []byte `json:"data" gencodec:"required"`

	// Derived fields. These fields are filled in by the node
	// but not secured by consensus.
	// block in which the transaction was included
	BlockNumber uint64 `json:"blockNumber"`
	// hash of the transaction
	TxHash common.Hash `json:"transactionHash" gencodec:"required"`
	// index of the transaction in the block
	TxIndex uint `json:"transactionIndex"`
	// hash of the block in which the transaction was included
	BlockHash common.Hash `json:"blockHash"`
	// index of the log in the block
	Index uint `json:"logIndex"`

	// The Removed field is true if this log was reverted due to a chain reorganisation.
	// You must pay attention to this field if you receive logs through a filter query.
	Removed bool `json:"removed"`
}
type ipldResult struct {
	CID  string `db:"cid"`
	Data []byte `db:"data"`
}

func main() {
_, receiptBytes, err := RetrieveReceiptsByBlockHash()
if err != nil {
  // handle this error better than this
  panic(err)
}
//logs := make([][]*Log, len(receiptBytes))

//fmt.Println("len(receiptBytes) " + receiptBytes)

logs := make([][]*types.Log, len(receiptBytes) )
fmt.Print("This is the JSON MSG :", receiptBytes)
  for i, rctBytes := range receiptBytes {
    var rct types.Receipt
    if err := rlp.DecodeBytes(rctBytes, &rct); err != nil {
      //return nil, err    Put me back later when you  make me a function
      panic(err)
    }
    logs[i] = rct.Logs

    json_msg, err := json.MarshalIndent(rct.Logs, "", "  ")
    if err != nil {
        fmt.Println(err)
    }
    fmt.Print("This is the JSON MSG :" , json_msg)

  //  fmt.Println("This is the log "  + json_msg.Topics[0])
  }
}



func RetrieveReceiptsByBlockHash() ([]string, [][]byte, error) {

  psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
    "password=%s dbname=%s sslmode=disable",
    host, port, user, password, dbname)
  db, err := sql.Open("postgres", psqlInfo)
  if err != nil {
    panic(err)
  }
  fmt.Println("Successfully connected To LOCAL!")

  psqlInfo_prod := fmt.Sprintf("host=%s port=%d user=%s "+
    "password=%s dbname=%s sslmode=disable",
    host_prod, prod_port, prod_user, prod_password, dbname_prod)
  db_prod, err := sql.Open("postgres", psqlInfo_prod)
  if err != nil {
    panic(err)
  }
  fmt.Println("Successfully connected TO PROD!")

  receiptBytes, err := db_prod.Query("SELECT receipt_cids.cid, data FROM eth.receipt_cids INNER JOIN public.blocks ON (receipt_cids.mh_key = blocks.key) limit 10")
  if err != nil {
    // handle this error better than this
    panic(err)
  }
  defer receiptBytes.Close()



	var ArrLen int =0 
  //var DecodedData string
  var DecodedData types.Receipt
//  rcts := make([][]byte, len(rctResults))
  for receiptBytes.Next() {
    //fmt.Println("Getting DecodedData")
    ArrLen++
    var cid string
    var data []byte
    err = receiptBytes.Scan(&cid, &data)
    if err != nil {
      // handle this error
      panic(err)
    }
    if err := rlp.DecodeBytes(data, &DecodedData); err != nil {
      //return nil, err    Put me back later when you  make me a function
      panic(err)
    }
    var DataLog []*types.Log //[]*Log
    DataLog = DecodedData.Logs
    //json_msg, err := json.Marshal(DataLog)
    if err != nil {
        fmt.Println(err)
    }

//fmt.Printf("==============This is the DATA:", data," \n\n")
//fmt.Printf("++++++++++++++This is the receiptBytes.cid:", cid," \n\n")
    fmt.Printf("==============This is the DATA: \n\n")
    var Topics_Field [4] common.Hash
    var i =0 
    for _, DL := range DataLog {
      fmt.Printf("This is the address:", DL.Removed, "\n\n")
      //  fmt.Printf("This is the DecodedData.Logs: 0" ,DL, "\n\n")
      for _, Top := range DL.Topics {

      //          fmt.Printf("These are the TOPICS: " ,Top, "\n")
          Topics_Field[i] = Top;
        }
    }

  }
    err = receiptBytes.Err()
  if err != nil {
    panic(err)
  }
  fmt.Println("ArrLen ", ArrLen)
  
  rctResults := make([]ipldResult, 0)
	cids := make([]string, len(rctResults))
	rcts := make([][]byte, len(rctResults))

  defer db.Close()

	return cids, rcts, nil

}

