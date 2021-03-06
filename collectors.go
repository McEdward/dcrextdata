package main

import (
	"encoding/json"
	"fmt"
	"net/http"
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

var dcrlaunchtime int64 = 1454889600

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

func collectExchangeData(start int64) ([]exchangeDataTick, error) {
	data := make([]exchangeDataTick, 0)

	poloniexdata, err := collectPoloniexData(start)
	if err != nil {
		log.Error("Error: ", err)
		return nil, err
	}
	bittrexdata, err := collectBittrexData(start)
	if err != nil {
		log.Error("Error: ", err)
		return nil, err
	}
	data = append(data, poloniexdata...)
	data = append(data, bittrexdata...)
	return data, nil
}
