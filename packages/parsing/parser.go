package parsing

import (
	"regexp"
	"strings"
)

type FlightData struct {
	// Метаданные
	ID           string `bson:"_id,omitempty" json:"id"`
	FlightDataID int    `bson:"flightDataID" json:"flightDataID"`
	// Основная информация (столбец A)
	Region string `bson:"region" json:"region"`
	// Парсинг из столбца B (SHR - основные данные)
	SHRData SHRData `bson:"shr" json:"shr"`
	// Парсинг из столбца C (DEP - вылет)
	Departure DepartureData `bson:"dep" json:"dep"`
	// Парсинг из столбца D (ARR - прибытие)
	Arrival ArrivalData `bson:"arr" json:"arr"`
	// Извлеченные ключевые поля для быстрого поиска
	SearchFields SearchField `bson:"searchFields" json:"searchFields"`
}

// SHRData - данные из столбца B (основная информация о полете)
type SHRData struct {
	RawText          string `bson:"rawText" json:"rawText"`
	SID              string `bson:"sid" json:"sid"`
	AircraftIndex    string `bson:"aircraftIndex" json:"aircraftIndex"`
	AircraftType     string `bson:"aircraftType" json:"aircraftType"`
	AircraftQuantity string `bson:"aircraftQuantity" json:"aircraftQuantity"`
	CoordinatesDep   string `bson:"coordinatesDep" json:"coordinatesDep"`
	CoordinatesArr   string `bson:"coordinatesArr" json:"coordinatesArr"`
	Date             string `bson:"date" json:"date"`
	Operator         string `bson:"operator" json:"operator"`
	Remarks          string `bson:"remarks" json:"remarks"`
}

// DepartureData - данные о вылете из столбца C
type DepartureData struct {
	RawText     string `bson:"rawText" json:"rawText"`
	SID         string `bson:"sid" json:"sid"`
	Date        string `bson:"date" json:"date"`
	Time        string `bson:"time" json:"time"`
	Airport     string `bson:"airport" json:"airport"`
	Coordinates string `bson:"coordinates" json:"coordinates"`
}

// ArrivalData - данные о прибытии из столбца D
type ArrivalData struct {
	RawText     string `bson:"rawText" json:"rawText"`
	SID         string `bson:"sid" json:"sid"`
	Date        string `bson:"date" json:"date"`
	Time        string `bson:"time" json:"time"`
	Airport     string `bson:"airport" json:"airport"`
	Coordinates string `bson:"coordinates" json:"coordinates"`
}

// KeyFields - ключевые поля для индексации и поиска
type SearchField struct {
	SID         string `bson:"sid" json:"sid"`
	Region      string `bson:"region" json:"region"`
	Date        string `bson:"date" json:"date"`
	Operator    string `bson:"operator" json:"operator"`
	Coordinates string `bson:"coordinates" json:"coordinates"`
}

// Безопасное получение элемента из массива
func safeGet(arr []string, index int) string {
	if index < len(arr) {
		return arr[index]
	}
	return ""
}

// Создание структуры FlightData из строки Excel
func CreateFlightData(rowNum int, row []string) FlightData {
	region := safeGet(row, 0)
	shrRaw := safeGet(row, 1)
	depRaw := safeGet(row, 2)
	arrRaw := safeGet(row, 3)

	// Парсим данные
	shrData := parseSHRData(shrRaw)
	depData := parseDepartureData(depRaw)
	arrData := parseArrivalData(arrRaw)

	// Создаем ключевые поля для индексации
	searchField := SearchField{
		Region:   region,
		Date:     shrData.Date,
		Operator: shrData.Operator,
		SID:      shrData.SID,
	}

	return FlightData{
		FlightDataID: rowNum,
		Region:       region,
		SHRData:      shrData,
		Departure:    depData,
		Arrival:      arrData,
		SearchFields: searchField,
	}
}

func extractData(expression string, rawText string) string {

	if matches := regexp.MustCompile(expression).FindStringSubmatch(rawText); len(matches) > 1 {
		return matches[1]
	}
	return ""
}

// Парсинг SHR данных из столбца B
func parseSHRData(rawText string) SHRData {
	shr := SHRData{
		RawText: rawText,
	}

	shr.AircraftIndex = extractData(`SHR-([A-Z0-9]+)`, rawText)
	shr.AircraftType = extractData(`TYP/\d*([A-Z]+)`, rawText)
	shr.AircraftQuantity = extractData(`TYP/([0-9]+)`, rawText)
	shr.CoordinatesDep = extractData(`DEP/([0-9]+[NS][0-9]+[EW])`, rawText)
	shr.CoordinatesArr = extractData(`DEST/([0-9]+[NS][0-9]+[EW])`, rawText)
	shr.Date = extractData(`DOF/([0-9]+)`, rawText)
	shr.SID = extractData(`SID/([0-9]+)`, rawText)

	// Парсинг OPR
	if idx := strings.Index(rawText, "OPR/"); idx != -1 {
		shr.Operator = "todo operator..."
	}

	// Парсинг RMK
	if idx := strings.Index(rawText, "RMK/"); idx != -1 {
		shr.Remarks = "todo remarks..."
	}

	return shr
}

// Парсинг данных о вылете из столбца C
func parseDepartureData(rawText string) DepartureData {
	dep := DepartureData{
		RawText: rawText,
	}

	dep.SID = extractData(`-SID ([0-9]+)`, rawText)
	dep.Date = extractData(`-ADD ([0-9]+)`, rawText)
	dep.Time = extractData(`-ATD ([0-9]+)`, rawText)
	dep.Airport = extractData(`-ADEP ([A-Z0-9]+)`, rawText)
	dep.Coordinates = extractData(`-ADEPZ ([0-9]+[NS][0-9]+[EW])`, rawText)

	return dep
}

// Парсинг данных о прибытии из столбца D
func parseArrivalData(rawText string) ArrivalData {
	arr := ArrivalData{
		RawText: rawText,
	}

	arr.SID = extractData(`-SID ([0-9]+)`, rawText)
	arr.Date = extractData(`-ADA ([0-9]+)`, rawText)
	arr.Time = extractData(`-ATA ([0-9]+)`, rawText)
	arr.Airport = extractData(`-ADARR ([A-Z0-9]+)`, rawText)
	arr.Coordinates = extractData(`-ADARRZ ([0-9]+[NS][0-9]+[EW])`, rawText)

	return arr
}
