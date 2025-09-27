package cors

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// CORS возвращает настроенный CORS middleware
func CORS() gin.HandlerFunc {

	// Проверяем, что базовые значения окружения присутствуют
	issuer := os.Getenv("KEYCLOAK_URL")
	if issuer == "" {
		panic("KEYCLOAK_URL is not set")
	}

	// FRONT_URL(S) — можно указать один или несколько через запятую
	// Примеры:
	// FRONT_URL=http://localhost:3000
	// FRONT_URLS=http://localhost:3000,http://localhost:5173,https://dev.example.com
	var allowedOrigins []string

	raw := os.Getenv("FRONT_URLS")
	if raw == "" {
		// fallback на FRONT_URL
		frontURL := strings.TrimRight(os.Getenv("FRONT_URL"), "/")
		if frontURL != "" {
			allowedOrigins = []string{frontURL}
		} else {
			log.Println("⚠️  FRONT_URL(S) не задан — используем http://localhost:3000")
			allowedOrigins = []string{"http://localhost:3000"}
		}
	} else {
		parts := strings.Split(raw, ",")
		for _, p := range parts {
			p = strings.TrimSpace(strings.TrimRight(p, "/"))
			if p != "" {
				allowedOrigins = append(allowedOrigins, p)
			}
		}
	}

	// Расширяем AllowHeaders под типичные случаи upload/form и кастомные заголовки
	allowHeaders := []string{
		"Origin",
		"Content-Type",
		"Authorization",
		"Accept",
		"X-Requested-With",
		"X-CSRF-Token",
		"Content-Disposition",
		// Добавьте сюда свои кастомные заголовки, если используете:
		// "X-File-Name",
		// "X-Upload-Token",
	}

	return cors.New(cors.Config{
		AllowOrigins:     allowedOrigins,
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"},
		AllowHeaders:     allowHeaders,
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	})
}
