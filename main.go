package main

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	types "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	// dotenv "github.com/joho/godotenv"
	"reflect"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	ABI = `
	[
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "lotId",
        "type": "uint256"
      },
      {
        "indexed": false,
        "internalType": "address",
        "name": "user",
        "type": "address"
      },
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "amount",
        "type": "uint256"
      }
    ],
    "name": "ClaimWithdrawn",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "amount",
        "type": "uint256"
      }
    ],
    "name": "FeeWithdrawn",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": false,
        "internalType": "uint8",
        "name": "version",
        "type": "uint8"
      }
    ],
    "name": "Initialized",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "lotId",
        "type": "uint256"
      },
      {
        "indexed": false,
        "internalType": "address",
        "name": "from",
        "type": "address"
      },
      {
        "indexed": false,
        "internalType": "address[]",
        "name": "to",
        "type": "address[]"
      }
    ],
    "name": "Invited",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "lotId",
        "type": "uint256"
      },
      {
        "indexed": false,
        "internalType": "string",
        "name": "tokenA",
        "type": "string"
      },
      {
        "indexed": false,
        "internalType": "string[]",
        "name": "tokenBChoices",
        "type": "string[]"
      },
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "startEpoch",
        "type": "uint256"
      },
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "duration",
        "type": "uint256"
      },
      {
        "indexed": false,
        "internalType": "address",
        "name": "creator",
        "type": "address"
      },
      {
        "indexed": false,
        "internalType": "bool",
        "name": "isPrivate",
        "type": "bool"
      },
      {
        "indexed": false,
        "internalType": "bool",
        "name": "isChallenge",
        "type": "bool"
      }
    ],
    "name": "LotCreated",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "lotId",
        "type": "uint256"
      },
      {
        "indexed": false,
        "internalType": "string",
        "name": "token",
        "type": "string"
      },
      {
        "indexed": false,
        "internalType": "address",
        "name": "user",
        "type": "address"
      },
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "size",
        "type": "uint256"
      }
    ],
    "name": "LotJoined",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "lotId",
        "type": "uint256"
      },
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "size",
        "type": "uint256"
      },
      {
        "indexed": false,
        "internalType": "string",
        "name": "winningToken",
        "type": "string"
      },
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "startPriceTokenA",
        "type": "uint256"
      },
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "startPriceTokenB",
        "type": "uint256"
      },
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "resolvePriceTokenA",
        "type": "uint256"
      },
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "resolvePriceTokenB",
        "type": "uint256"
      }
    ],
    "name": "LotResolved",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": true,
        "internalType": "address",
        "name": "previousOwner",
        "type": "address"
      },
      {
        "indexed": true,
        "internalType": "address",
        "name": "newOwner",
        "type": "address"
      }
    ],
    "name": "OwnershipTransferred",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "lotId",
        "type": "uint256"
      },
      {
        "indexed": false,
        "internalType": "address",
        "name": "user",
        "type": "address"
      },
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "amount",
        "type": "uint256"
      }
    ],
    "name": "RefundWithdrawn",
    "type": "event"
  },
  {
    "inputs": [],
    "name": "RATIO_PRECISION",
    "outputs": [{ "internalType": "uint256", "name": "", "type": "uint256" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "collateralToken",
    "outputs": [{ "internalType": "address", "name": "", "type": "address" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "string", "name": "_tokenA", "type": "string" },
      {
        "internalType": "string[]",
        "name": "_tokenBChoices",
        "type": "string[]"
      },
      { "internalType": "uint256", "name": "_size", "type": "uint256" },
      { "internalType": "uint256", "name": "_startEpoch", "type": "uint256" },
      { "internalType": "uint256", "name": "_duration", "type": "uint256" },
      { "internalType": "bool", "name": "_isPrivate", "type": "bool" },
      { "internalType": "bool", "name": "_isChallenge", "type": "bool" }
    ],
    "name": "createLot",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "feePercentage",
    "outputs": [{ "internalType": "uint256", "name": "", "type": "uint256" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "_priceFeed", "type": "address" },
      {
        "internalType": "address",
        "name": "_collateralToken",
        "type": "address"
      },
      { "internalType": "uint256", "name": "_feePercentage", "type": "uint256" }
    ],
    "name": "initialize",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "uint256", "name": "_lotId", "type": "uint256" },
      { "internalType": "address[]", "name": "_addresses", "type": "address[]" }
    ],
    "name": "invite",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "uint256", "name": "_lotId", "type": "uint256" },
      { "internalType": "string", "name": "_token", "type": "string" },
      { "internalType": "uint256", "name": "_size", "type": "uint256" }
    ],
    "name": "joinLot",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "lastLotId",
    "outputs": [{ "internalType": "uint256", "name": "", "type": "uint256" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "owner",
    "outputs": [{ "internalType": "address", "name": "", "type": "address" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "priceFeed",
    "outputs": [{ "internalType": "address", "name": "", "type": "address" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "renounceOwnership",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "uint256", "name": "_lotId", "type": "uint256" }
    ],
    "name": "resolveLot",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "_collateralToken",
        "type": "address"
      }
    ],
    "name": "setCollateralToken",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "_priceFeed", "type": "address" }
    ],
    "name": "setPriceFeed",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "totalFee",
    "outputs": [{ "internalType": "uint256", "name": "", "type": "uint256" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "newOwner", "type": "address" }
    ],
    "name": "transferOwnership",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "uint256", "name": "_lotId", "type": "uint256" }
    ],
    "name": "withdrawClaim",
    "outputs": [
      { "internalType": "bool", "name": "isSuccessful", "type": "bool" }
    ],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "withdrawFee",
    "outputs": [
      { "internalType": "bool", "name": "isSuccessful", "type": "bool" }
    ],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "uint256", "name": "_lotId", "type": "uint256" }
    ],
    "name": "withdrawRefund",
    "outputs": [
      { "internalType": "bool", "name": "isSuccessful", "type": "bool" }
    ],
    "stateMutability": "nonpayable",
    "type": "function"
  }
]

	`
)

type LotCreatedEvent struct {
	ID            uuid.UUID `gorm:"primaryKey"`
	LastLotId     decimal.Decimal
	TokenA        string
	TokenBChoices string
	StartEpoch    decimal.Decimal
	Duration      decimal.Decimal
	Creator       common.Address
	IsPrivate     bool
	IsChallenge   bool
	BlockNumber   uint64
}
type LotJoinedEvent struct {
	ID          uuid.UUID `gorm:"primaryKey"`
	LotId       decimal.Decimal
	Token       string
	User        common.Address
	Size        decimal.Decimal
	BlockNumber uint64
}
type LotResolvedEvent struct {
	ID                 uuid.UUID `gorm:"primaryKey"`
	LotId              decimal.Decimal
	Size               decimal.Decimal
	WinningToken       string
	StartPriceTokenA   decimal.Decimal
	StartPriceTokenB   decimal.Decimal
	ResolvePriceTokenA decimal.Decimal
	ResolvePriceTokenB decimal.Decimal
	BlockNumber        uint64
}

func consumeHistorical(ch chan types.Log) {
	lotCreatedEventHex := "0x7aa0e2508522c6e152ffa3164ff180b4d3a50b99c2d3a682a4a3176d99bcf5f5"
	lotJoinedEventHex := "0x2ef9c5ad696244f79a77d314d32cd61df59551c3eb7e42679ea2f2898784065c"
	lotResolvedEventHex := "0xcb4fe1f5bd157ec52ea58b76dd52ac4ce2d6a5498e3a3fcea3b972c329964735"
	databaseUri := "postgresql://postgres:1234@localhost:5432/postgres"
	db, err := gorm.Open(postgres.Open(databaseUri), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	} else {
		fmt.Println("Connected to database")
	}

	contractABI, err := abi.JSON(strings.NewReader(ABI))
	if err != nil {
		log.Fatal("could not convert JSON ABI string to ABI object")
	}

	for vLog := range ch {
		topicHex := vLog.Topics[0].Hex()
		blockNumber := vLog.BlockNumber
		switch topicHex {
		case lotCreatedEventHex:
			fmt.Println("Inside lotCreatedEvent")
			res, err := contractABI.Unpack("LotCreated", vLog.Data)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("res of unpack ", res)
			lotId := (res[0]).(*big.Int)
			token := (res[1]).(string)
			tokenB := (fmt.Sprint(res[2]))
			start := (res[3]).(*big.Int)
			duration := (res[4]).(*big.Int)
			creator := (res[5]).(common.Address)
			private := (res[6]).(bool)
			challange := (res[7]).(bool)
			//Values to insert
			varLotCreated := LotCreatedEvent{
				ID:            uuid.New(),
				LastLotId:     decimal.NewFromBigInt(lotId, 0),
				TokenA:        token,
				TokenBChoices: string(tokenB),
				StartEpoch:    decimal.NewFromBigInt(start, 0),
				Duration:      decimal.NewFromBigInt(duration, 0),
				Creator:       creator,
				IsPrivate:     private,
				IsChallenge:   challange,
				BlockNumber:   blockNumber,
			}
			db.Clauses(clause.OnConflict{
				UpdateAll: true,
			}).Create(&varLotCreated)

		case lotJoinedEventHex:
			fmt.Println("Inside lotJoinedEvent")
			res, err := contractABI.Unpack("LotJoined", vLog.Data)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("res of unpack ", reflect.TypeOf(res[0]), reflect.TypeOf(res[1]), reflect.TypeOf(res[2]), reflect.TypeOf(res[3]), (res))
			lotId := (res[0]).(*big.Int)
			token := (res[1]).(string)
			user := (res[2]).(common.Address)
			size := (res[3]).(*big.Int)
			//Values to insert
			varLotJoined := LotJoinedEvent{
				ID:          uuid.New(),
				LotId:       decimal.NewFromBigInt(lotId, 0),
				Token:       token,
				User:        user,
				Size:        decimal.NewFromBigInt(size, 0),
				BlockNumber: blockNumber,
			}
			db.Clauses(clause.OnConflict{
				UpdateAll: true,
			}).Create(&varLotJoined)

		case lotResolvedEventHex:
			fmt.Println("lotResolvedEventHex")
			res, err := contractABI.Unpack("LotResolved", vLog.Data)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("res of unpack ", res)
			lotId := (res[0]).(*big.Int)
			size := (res[1]).(*big.Int)
			winToken := (res[2]).(string)
			startPriceTokenA := (res[3]).(*big.Int)
			startPriceTokenB := (res[4]).(*big.Int)
			resolvePriceTokenA := (res[5]).(*big.Int)
			resolvePriceTokenB := (res[6]).(*big.Int)
			//Values to insert
			varLotResolved := LotResolvedEvent{
				ID:                 uuid.New(),
				LotId:              decimal.NewFromBigInt(lotId, 0),
				Size:               decimal.NewFromBigInt(size, 0),
				WinningToken:       winToken,
				StartPriceTokenA:   decimal.NewFromBigInt(startPriceTokenA, 0),
				StartPriceTokenB:   decimal.NewFromBigInt(startPriceTokenB, 0),
				ResolvePriceTokenA: decimal.NewFromBigInt(resolvePriceTokenA, 0),
				ResolvePriceTokenB: decimal.NewFromBigInt(resolvePriceTokenB, 0),
				BlockNumber:        blockNumber,
			}
			db.Clauses(clause.OnConflict{
				UpdateAll: true,
			}).Create(&varLotResolved)

		}
	}
}

func main() {
	//LOAD ENV VARIABLES
	historic := true
	databaseUri := "postgresql://postgres:1234@localhost:5432/postgres"
	infuraWSUri := ""
	httpProviderUri := ""

	lotCreatedEventHex := "0x7aa0e2508522c6e152ffa3164ff180b4d3a50b99c2d3a682a4a3176d99bcf5f5"
	lotJoinedEventHex := "0x2ef9c5ad696244f79a77d314d32cd61df59551c3eb7e42679ea2f2898784065c"
	lotResolvedEventHex := "0xcb4fe1f5bd157ec52ea58b76dd52ac4ce2d6a5498e3a3fcea3b972c329964735"

	//Create connection to postgres
	db, err := gorm.Open(postgres.Open(databaseUri), &gorm.Config{})
	if err != nil {
		panic("Failed to connect with postgres")
	} else {
		fmt.Println("Connected to postgres")
	}
	//Migrate tables
	db.AutoMigrate(LotCreatedEvent{})
	db.AutoMigrate(LotJoinedEvent{})
	db.AutoMigrate(LotResolvedEvent{})

	//Create connection to ws client
	conn, err := ethclient.Dial(infuraWSUri)

	if err != nil {
		log.Fatal("Failed to connect with websocket client")
	}
	//Initialize abi
	contractABI, err := abi.JSON(strings.NewReader(ABI))
	if err != nil {
		log.Fatal("Could not convert json abi string to abi object")
	}
	fmt.Println("=================== contractABI ================", contractABI)

	//TODO: For both part we could get current block and divide task into two parts
	// 1. To listen data after current block through subscription
	// 2. To query historical for the earlier once

	contractAddress := common.HexToAddress("0xdBFC942264f5CebF8C59f4065af2EFfB92D12475")
	currentQuery := ethereum.FilterQuery{
		Addresses: []common.Address{contractAddress},
		Topics: [][]common.Hash{{
			common.HexToHash("0x7aa0e2508522c6e152ffa3164ff180b4d3a50b99c2d3a682a4a3176d99bcf5f5"),
			common.HexToHash("0xcb4fe1f5bd157ec52ea58b76dd52ac4ce2d6a5498e3a3fcea3b972c329964735"),
			common.HexToHash("0x2ef9c5ad696244f79a77d314d32cd61df59551c3eb7e42679ea2f2898784065c"),
		}},
	}
	fmt.Println("=================== current query ================", currentQuery)

	//Historic
	fromBlock := big.NewInt(int64(8585483))
	toBlock := big.NewInt(int64(8585502))

	historicQuery := ethereum.FilterQuery{
		FromBlock: fromBlock,
		ToBlock:   toBlock,
		Addresses: []common.Address{contractAddress},
		//common.HexToHash("0x7aa0e2508522c6e152ffa3164ff180b4d3a50b99c2d3a682a4a3176d99bcf5f5"), common.HexToHash("0xcb4fe1f5bd157ec52ea58b76dd52ac4ce2d6a5498e3a3fcea3b972c329964735"),
		Topics: [][]common.Hash{{
			common.HexToHash("0x7aa0e2508522c6e152ffa3164ff180b4d3a50b99c2d3a682a4a3176d99bcf5f5"),
			common.HexToHash("0xcb4fe1f5bd157ec52ea58b76dd52ac4ce2d6a5498e3a3fcea3b972c329964735"),
			common.HexToHash("0x2ef9c5ad696244f79a77d314d32cd61df59551c3eb7e42679ea2f2898784065c"),
		}},
	}

	//Channels to receive logs
	currentLogs := make(chan types.Log)
	historicLogs := make(chan types.Log)

	if historic {
		//Connect to client
		clientH, err := ethclient.Dial(httpProviderUri)
		if err != nil {
			log.Fatal("Failed to connect rpc ", err)
		} else {
			fmt.Println("successfully connected to the rpc endpoint!")
		}
		historiclogs, err := clientH.FilterLogs(context.Background(), historicQuery)
		if err != nil {
			log.Fatal("Failed to get historical logs ", err)
		} else {
			fmt.Println("successfully got historical logs ")
		}
		for _, vLog := range historiclogs {
			go consumeHistorical(historicLogs)
			historicLogs <- (vLog)
		}
	}
	//Create websocket subscription
	sub, err := conn.SubscribeFilterLogs(context.Background(), currentQuery, currentLogs)
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println("successfully subscribed to the contract events!")
	}

	for {
		select {
		case err := <-sub.Err():
			log.Fatal(err)
		case vLog := <-currentLogs:
			fmt.Println("I am up to here currentLogs", vLog.Data)
			for vLog := range currentLogs {
				topicHex := vLog.Topics[0].Hex()
				blockNumber := vLog.BlockNumber
				switch topicHex {
				case lotCreatedEventHex:
					fmt.Println("Inside lotCreatedEvent")
					res, err := contractABI.Unpack("LotCreated", vLog.Data)
					if err != nil {
						log.Fatal(err)
					}
					fmt.Println("res of unpack ", res)
					lotId := (res[0]).(*big.Int)
					token := (res[1]).(string)
					tokenB := (fmt.Sprint(res[2]))
					start := (res[3]).(*big.Int)
					duration := (res[4]).(*big.Int)
					creator := (res[5]).(common.Address)
					private := (res[6]).(bool)
					challange := (res[7]).(bool)
					//Values to insert
					varLotCreated := LotCreatedEvent{
						ID:            uuid.New(),
						LastLotId:     decimal.NewFromBigInt(lotId, 0),
						TokenA:        token,
						TokenBChoices: string(tokenB),
						StartEpoch:    decimal.NewFromBigInt(start, 0),
						Duration:      decimal.NewFromBigInt(duration, 0),
						Creator:       creator,
						IsPrivate:     private,
						IsChallenge:   challange,
						BlockNumber:   blockNumber,
					}
					db.Clauses(clause.OnConflict{
						UpdateAll: true,
					}).Create(&varLotCreated)

				case lotJoinedEventHex:
					fmt.Println("Inside lotJoinedEvent")
					res, err := contractABI.Unpack("LotJoined", vLog.Data)
					if err != nil {
						log.Fatal(err)
					}
					fmt.Println("res of unpack ", reflect.TypeOf(res[0]), reflect.TypeOf(res[1]), reflect.TypeOf(res[2]), reflect.TypeOf(res[3]), (res))
					lotId := (res[0]).(*big.Int)
					token := (res[1]).(string)
					user := (res[2]).(common.Address)
					size := (res[3]).(*big.Int)
					//Values to insert
					varLotJoined := LotJoinedEvent{
						ID:          uuid.New(),
						LotId:       decimal.NewFromBigInt(lotId, 0),
						Token:       token,
						User:        user,
						Size:        decimal.NewFromBigInt(size, 0),
						BlockNumber: blockNumber,
					}
					db.Clauses(clause.OnConflict{
						UpdateAll: true,
					}).Create(&varLotJoined)

				case lotResolvedEventHex:
					fmt.Println("lotResolvedEventHex")
					res, err := contractABI.Unpack("LotResolved", vLog.Data)
					if err != nil {
						log.Fatal(err)
					}
					fmt.Println("res of unpack ", res)
					lotId := (res[0]).(*big.Int)
					size := (res[1]).(*big.Int)
					winToken := (res[2]).(string)
					startPriceTokenA := (res[3]).(*big.Int)
					startPriceTokenB := (res[4]).(*big.Int)
					resolvePriceTokenA := (res[5]).(*big.Int)
					resolvePriceTokenB := (res[6]).(*big.Int)
					//Values to insert
					varLotResolved := LotResolvedEvent{
						ID:                 uuid.New(),
						LotId:              decimal.NewFromBigInt(lotId, 0),
						Size:               decimal.NewFromBigInt(size, 0),
						WinningToken:       winToken,
						StartPriceTokenA:   decimal.NewFromBigInt(startPriceTokenA, 0),
						StartPriceTokenB:   decimal.NewFromBigInt(startPriceTokenB, 0),
						ResolvePriceTokenA: decimal.NewFromBigInt(resolvePriceTokenA, 0),
						ResolvePriceTokenB: decimal.NewFromBigInt(resolvePriceTokenB, 0),
						BlockNumber:        blockNumber,
					}
					db.Clauses(clause.OnConflict{
						UpdateAll: true,
					}).Create(&varLotResolved)

				}
			}
		}
	}

}
