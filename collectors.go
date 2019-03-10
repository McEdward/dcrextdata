package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

type exchangeDataTick struct {
	High     float64
	Low      float64
	Open     float64
	Close    float64
	Volume   float64
	Time     int64
	Exchange string
}

type poloniexDataTick struct {
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Open   float64 `json:"open"`
	Close  float64 `json:"close"`
	Volume float64 `json:"volume"`
	Time   int64   `json:"date"`
}

type poloniexAPIResponse []poloniexDataTick

type bittrexDataTick struct {
	High   float64 `json:"H"`
	Low    float64 `json:"L"`
	Open   float64 `json:"O"`
	Close  float64 `json:"C"`
	Volume float64 `json:"BV"`
	Time   string  `json:"T"`
}

type bittrexAPIResponse struct {
	Result []bittrexDataTick `json:"result"`
}

type bleutradeDataTick struct {
	High   string `json:"high"`
	Low    string `json:"low"`
	Open   string `json:"open"`
	Close  string `json:"close"`
	Volume string `json:"volume"`
	Time   string `json:"TimeStamp"`
}

type bleutradeAPIResponse struct {
	Result []bleutradeDataTick `json:"result"`
}

type binanceDataTick []string

type binanceAPIResponse []binanceData
type binanceData []interface{}

var dcrlaunchtime int64 = 1454889600
var binanceLimit int64 = 1000

func collectPoloniexData(start int64) ([]exchangeDataTick, error) {
	client := &http.Client{Timeout: 300 * time.Second}

	if start == 0 {
		start = dcrlaunchtime
	}

	res, err := client.Get(fmt.Sprintf("https://poloniex.com/public?command=returnChartData&currencyPair=BTC_DCR&start=%d&end=9999999999&period=1800", start))
	if err != nil {
		return nil, err
	}

	data := new(poloniexAPIResponse)
	err = json.NewDecoder(res.Body).Decode(data)

	if err != nil {
		return nil, err
	}

	res.Body.Close()

	exchangeData := make([]exchangeDataTick, 0)

	for _, v := range []poloniexDataTick(*data) {
		eData := exchangeDataTick{
			High:     v.High,
			Low:      v.Low,
			Open:     v.Open,
			Close:    v.Close,
			Time:     v.Time,
			Exchange: "poloniex",
		}
		exchangeData = append(exchangeData, eData)
	}

	return exchangeData, nil
}

func collectBittrexData(start int64) ([]exchangeDataTick, error) {
	client := &http.Client{Timeout: 300 * time.Second}

	// Bittrex "start" option doesn't work
	res, err := client.Get("https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName=BTC-DCR&tickInterval=thirtyMin")

	if err != nil {
		return nil, err
	}

	data := new(bittrexAPIResponse)
	err = json.NewDecoder(res.Body).Decode(data)

	if err != nil {
		return nil, err
	}

	res.Body.Close()

	exchangeData := make([]exchangeDataTick, 0)

	for _, v := range data.Result {
		t, _ := time.Parse(time.RFC3339[:19], v.Time)

		if t.Unix() < start {
			continue
		}

		eData := exchangeDataTick{
			High:     v.High,
			Low:      v.Low,
			Open:     v.Open,
			Close:    v.Close,
			Time:     t.Unix(),
			Exchange: "bittrex",
		}
		exchangeData = append(exchangeData, eData)
	}

	return exchangeData, nil
}

func collectBleutradeData(start int64) ([]exchangeDataTick, error) {
	client := &http.Client{Timeout: 300 * time.Second}

	// Bleutrade doesn't have a "start" option
	res, err := client.Get("https://bleutrade.com/api/v2/public/getcandles?market=DCR_BTC&count=999999&period=30m")

	if err != nil {
		return nil, err
	}

	data := new(bleutradeAPIResponse)
	err = json.NewDecoder(res.Body).Decode(data)

	if err != nil {
		return nil, err
	}

	res.Body.Close()

	exchangeData := make([]exchangeDataTick, 0)

	for _, v := range data.Result {
		t, _ := time.Parse("2006-01-02 15:04:05", v.Time)

		if t.Unix() < start {
			continue
		}

		// conversion of types to match exchangeDataTick
		high, err := strconv.ParseFloat(v.High, 64)
		if err != nil {
			log.Error("Failed to convert to float: ", err.Error())
			return nil, nil
		}
		low, err := strconv.ParseFloat(v.Low, 64)
		if err != nil {
			log.Error("Failed to convert to float: ", err.Error())
			return nil, nil
		}
		open, err := strconv.ParseFloat(v.Open, 64)
		if err != nil {
			log.Error("Failed to convert to float: ", err.Error())
			return nil, nil
		}
		close, err := strconv.ParseFloat(v.Close, 64)
		if err != nil {
			log.Error("Failed to convert to float: ", err.Error())
			return nil, nil
		}

		eData := exchangeDataTick{
			High:     high,
			Low:      low,
			Open:     open,
			Close:    close,
			Time:     t.Unix(),
			Exchange: "bleutrade",
		}
		exchangeData = append(exchangeData, eData)
	}

	return exchangeData, nil
}

func collectBinanceData(start int64) ([]exchangeDataTick, error) {
	client := &http.Client{Timeout: 300 * time.Second}

	if start == 0 {
		start = dcrlaunchtime
	}

	limit := binanceLimit

	// Converting of unix time from seconds to milliseconds as required by the API
	start = start * 1000

	res, err := client.Get(fmt.Sprintf("https://api.binance.com/api/v1/klines?symbol=DCRBTC&interval=30m&limit=%d&startTime=%d", limit, start))
	if err != nil {
		return nil, err
	}

	data := new(binanceAPIResponse)
	err = json.NewDecoder(res.Body).Decode(&data)

	if err != nil {
		return nil, err
	}

	res.Body.Close()

	exchangeData := make([]exchangeDataTick, 0)
	for _, j := range []binanceData(*data) {

		high, err := strconv.ParseFloat(j[2].(string), 64)
		if err != nil {
			log.Error("Failed to convert to float: ", err.Error())
			return nil, nil
		}
		low, err := strconv.ParseFloat(j[3].(string), 64)
		if err != nil {
			log.Error("Failed to convert to float: ", err.Error())
			return nil, nil
		}
		open, err := strconv.ParseFloat(j[1].(string), 64)
		if err != nil {
			log.Error("Failed to convert to float: ", err.Error())
			return nil, nil
		}
		close, err := strconv.ParseFloat(j[4].(string), 64)
		if err != nil {
			log.Error("Failed to convert to float: ", err.Error())
			return nil, nil
		}

		// Converting unix time from milliseconds to seconds
		time := int64(j[0].(float64)) / 1000

		eData := exchangeDataTick{
			High:     high,
			Low:      low,
			Open:     open,
			Close:    close,
			Time:     time,
			Exchange: "binance",
		}
		exchangeData = append(exchangeData, eData)
	}
	return exchangeData, nil

}

func collectExchangeData(start int64) ([]exchangeDataTick, error) {
	data := make([]exchangeDataTick, 0)

	/*poloniexdata, err := collectPoloniexData(start)
	if err != nil {
		log.Error("Error: ", err)
		return nil, err
	}
	bittrexdata, err := collectBittrexData(start)
	if err != nil {
		log.Error("Error: ", err)
		return nil, err
	}*/
	bleutradedata, err := collectBleutradeData(start)
	if err != nil {
		log.Error("Error: ", err)
		return nil, err
	}
	binancedata, err := collectBinanceData(start)
	if err != nil {
		log.Error("Error: ", err)
		return nil, err
	}
	//data = append(data, poloniexdata...)
	//data = append(data, bittrexdata...)
	data = append(data, bleutradedata...)
	data = append(data, binancedata...)

	// Removing an hour from the current time
	t := time.Now().Unix() - 3600

	// Collecting all previous data available from the binance API
	for true {
		if int64(len(binancedata)) == binanceLimit && t > binancedata[len(binancedata)-1].Time {
			start = binancedata[len(binancedata)-1].Time
			binancedata, err = collectBinanceData(start)
			if err != nil {
				log.Error("Error: ", err)
			}
			data = append(data, binancedata...)
		} else {
			break
		}
	}

	return data, nil
}
