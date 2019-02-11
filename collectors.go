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

type powDataTick struct {
	Time              int64
	NetworkHashrate   int64
	PoolHashrate      float64
	Workers           int64
	NetworkDifficulty float64
	CoinPrice         string
	BtcPrice          string
	source            string
}

type luxorPowDataTick struct {
	Time              string  `json:"time"`
	NetworkHashrate   int64   `json:"network_hashrate"`
	PoolHashrate      float64 `json:"pool_hashrate"`
	Workers           int64   `json:"workers"`
	NetworkDifficulty float64 `json:"network_difficulty"`
	CoinPrice         string  `json:"coin_price"`
	BtcPrice          string  `json:"btc_price"`
}

type luxorAPIResponse struct {
	GlobalStats []luxorPowDataTick `json:"globalStats"`
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

func collectLuxorData(start int64) ([]powDataTick, error) {
	client := &http.Client{Timeout: 300 * time.Second}

	// Luxor "start" option doesn't work
	res, err := client.Get("https://mining.luxor.tech/API/DCR/stats/")

	if err != nil {
		return nil, err
	}

	data := new(luxorAPIResponse)
	err = json.NewDecoder(res.Body).Decode(data)

	if err != nil {
		return nil, err
	}

	res.Body.Close()

	powData := make([]powDataTick, 0)

	for _, v := range data.GlobalStats {
		t, _ := time.Parse(time.RFC3339, v.Time)

		if t.Unix() < start {
			continue
		}

		pData := powDataTick{
			Time:              t.Unix(),
			NetworkHashrate:   v.NetworkHashrate,
			PoolHashrate:      v.PoolHashrate,
			Workers:           v.Workers,
			NetworkDifficulty: v.NetworkDifficulty,
			CoinPrice:         v.CoinPrice,
			BtcPrice:          v.BtcPrice,
			source:            "luxor",
		}
		powData = append(powData, pData)
	}

	return powData, nil
}

func collectPOWData(start int64) ([]powDataTick, error) {
	data := make([]powDataTick, 0)

	luxordata, err := collectLuxorData(start)
	if err != nil {
		log.Error("Error: ", err)
		return nil, err
	}
	data = append(data, luxordata...)
	return data, nil
}
