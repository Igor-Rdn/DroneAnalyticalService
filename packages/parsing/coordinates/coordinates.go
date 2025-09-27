package coorinates

import (
	"fmt"
	"math"
	"strconv"
)

type Coordinate struct {
	Lat float64 `bson:"lat"`
	Lon float64 `bson:"lon"`
}

// ParseAviationCoordinate универсальный парсер для разных форматов
func ParseAviationCoordinate(coord string) (*Coordinate, error) {

	// Определяем формат по длине строки
	switch len(coord) {
	case 11, 12: // Базовый формат: DDMMN/SDDDMME
		return parseBasicFormat(coord)
	case 15, 16: // Высокоточный формат: DDMMSSN/SDDDMMSSE
		return parseHighPrecisionFormat(coord)
	default:
		return nil, fmt.Errorf("unknown coordinate format: %s (length: %d)", coord, len(coord))
	}
}

// Парсер базового формата (DDMMN/SDDDMME)
func parseBasicFormat(coord string) (*Coordinate, error) {

	// Широта: DDMMN
	latDegrees, err := strconv.Atoi(coord[:2])
	if err != nil {
		return nil, err
	}
	latMinutes, err := strconv.Atoi(coord[2:4])
	if err != nil {
		return nil, err
	}
	latHemisphere := coord[4:5]

	// Долгота: DDDMME (может начинаться с 3 цифр градусов)
	lonStart := 5
	// Определяем, где начинаются градусы долготы (3 или 2 цифры)
	if coord[5] >= '0' && coord[5] <= '9' {
		lonStart = 5 // DDDMM format
	} else {
		return nil, fmt.Errorf("invalid longitude format: %s", coord)
	}

	lonDegrees, err := strconv.Atoi(coord[lonStart : lonStart+3])
	if err != nil {
		// Пробуем 2 цифры для долготы
		lonDegrees, err = strconv.Atoi(coord[lonStart : lonStart+2])
		if err != nil {
			return nil, err
		}
		lonMinutes, err := strconv.Atoi(coord[lonStart+2 : lonStart+4])
		if err != nil {
			return nil, err
		}
		lonHemisphere := coord[lonStart+4 : lonStart+5]

		lat := float64(latDegrees) + float64(latMinutes)/60.0
		if latHemisphere == "S" {
			lat = -lat
		}

		lon := float64(lonDegrees) + float64(lonMinutes)/60.0
		if lonHemisphere == "W" {
			lon = -lon
		}

		return &Coordinate{Lat: lat, Lon: lon}, nil
	}

	lonMinutes, err := strconv.Atoi(coord[lonStart+3 : lonStart+5])
	if err != nil {
		return nil, err
	}
	lonHemisphere := coord[lonStart+5 : lonStart+6]

	lat := float64(latDegrees) + float64(latMinutes)/60.0
	if latHemisphere == "S" {
		lat = -lat
	}

	lon := float64(lonDegrees) + float64(lonMinutes)/60.0
	if lonHemisphere == "W" {
		lon = -lon
	}

	lat = roundOptimal(lat)
	lon = roundOptimal(lon)

	return &Coordinate{Lat: lat, Lon: lon}, nil
}

// Парсер высокоточного формата (DDMMSSN/SDDDMMSSE)
func parseHighPrecisionFormat(coord string) (*Coordinate, error) {

	// Широта: DDMMSSN
	latDegrees, err := strconv.Atoi(coord[:2])
	if err != nil {
		return nil, err
	}
	latMinutes, err := strconv.Atoi(coord[2:4])
	if err != nil {
		return nil, err
	}
	latSeconds, err := strconv.Atoi(coord[4:6])
	if err != nil {
		return nil, err
	}
	latHemisphere := coord[6:7]

	// Долгота: DDDMMSSE
	lonDegrees, err := strconv.Atoi(coord[7:10])
	if err != nil {
		return nil, err
	}
	lonMinutes, err := strconv.Atoi(coord[10:12])
	if err != nil {
		return nil, err
	}
	lonSeconds, err := strconv.Atoi(coord[12:14])
	if err != nil {
		return nil, err
	}
	lonHemisphere := coord[14:15]

	lat := float64(latDegrees) + float64(latMinutes)/60.0 + float64(latSeconds)/3600.0
	if latHemisphere == "S" {
		lat = -lat
	}

	lon := float64(lonDegrees) + float64(lonMinutes)/60.0 + float64(lonSeconds)/3600.0
	if lonHemisphere == "W" {
		lon = -lon
	}

	lat = roundOptimal(lat)
	lon = roundOptimal(lon)

	return &Coordinate{Lat: lat, Lon: lon}, nil
}

func roundOptimal(value float64) float64 {
	return math.Round(value*1000000) / 1000000
}
