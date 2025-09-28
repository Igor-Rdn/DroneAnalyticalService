package datetime

import "time"

func ParseDate(dateStr, timeStr string) *time.Time {

	var d time.Time
	var t time.Time
	var err error
	var dateTime time.Time

	if dateStr == "" {
		return nil
	}

	// Парсим дату
	dateLayout := "060102" // формат ddmmyy
	d, err = time.Parse(dateLayout, dateStr)
	if err != nil {
		panic(err)
	}

	if timeStr == "2400" {
		timeStr = "2359"
	}

	// Парсим время
	if timeStr != "" {
		timeLayout := "1504" // формат hhmm
		t, err = time.Parse(timeLayout, timeStr)
		if err != nil {
			panic(err)
		}
	}

	// Объединяем дату и время
	dateTime = time.Date(
		d.Year(), d.Month(), d.Day(),
		t.Hour(), t.Minute(), 0, 0,
		time.UTC,
	)

	if dateTime.IsZero() {
		return nil
	}

	return &dateTime

}
