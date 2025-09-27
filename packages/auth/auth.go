package auth

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/gin-gonic/gin"
)

var (
	provider     *oidc.Provider
	providerOnce sync.Once
	providerErr  error
	verifier     *oidc.IDTokenVerifier
)

func isDev() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("IS_DEV")))
	return v == "true" || v == "1" || v == "yes"
}

// getIssuerURL возвращает правильный URL в зависимости от среды
func getIssuerURL() string {
	issuer := os.Getenv("KEYCLOAK_URL")
	if issuer == "" {
		panic("KEYCLOAK_URL is not set")
	}

	// В development среде используем localhost вместо имени контейнера
	if isDev() && strings.Contains(issuer, "keycloak:") {
		issuer = strings.Replace(issuer, "keycloak:", "localhost:", 1)
	}

	return issuer
}

// initProvider инициализирует OIDC провайдер с retry логикой
func initProvider() (*oidc.Provider, error) {
	providerOnce.Do(func() {
		issuer := getIssuerURL()

		// Пытаемся инициализировать провайдер с несколькими попытками
		var err error
		for i := 0; i < 5; i++ {
			provider, err = oidc.NewProvider(context.Background(), issuer)
			if err == nil {
				// Успешно инициализировали провайдер
				clientID := os.Getenv("KEYCLOAK_CLIENT_ID")
				keycloakDevMode := strings.ToLower(os.Getenv("KEYCLOAK_DEV_MODE")) == "true"

				verifier = provider.Verifier(&oidc.Config{
					ClientID:          clientID,
					SkipIssuerCheck:   keycloakDevMode,
					SkipClientIDCheck: keycloakDevMode,
				})
				return
			}

			fmt.Printf("Attempt %d failed to create OIDC provider: %v\n", i+1, err)
			if i < 4 { // Не ждем после последней попытки
				time.Sleep(2 * time.Second)
			}
		}
		providerErr = fmt.Errorf("failed to create OIDC provider after 5 attempts: %w", err)
	})

	return provider, providerErr
}

// getVerifier возвращает верификатор токенов
func getVerifier() (*oidc.IDTokenVerifier, error) {
	if _, err := initProvider(); err != nil {
		return nil, err
	}
	return verifier, nil
}

func JWTAuth() gin.HandlerFunc {

	return func(c *gin.Context) {
		// Пропускаем аутентификацию для эндпоинта /ping
		if c.Request.URL.Path == "/ping" {
			c.Next()
			return
		}

		// В development режиме устанавливаем mock claims и пропускаем
		if isDev() {
			c.Set("jwt_claims", map[string]any{
				"sub":                "dev-user",
				"preferred_username": "developer",
				"email":              "dev@example.com",
				"realm_access": map[string]any{
					"roles": []string{"admin", "user"},
				},
			})
			c.Next()
			return
		}

		// Инициализируем верификатор
		verifier, err := getVerifier()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"error":   "authentication service unavailable",
				"details": err.Error(),
			})
			return
		}

		// Проверяем Authorization header
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "missing authorization header",
			})
			return
		}

		// Извлекаем токен
		if !strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid authorization format, expected 'Bearer <token>'",
			})
			return
		}

		tokenString := strings.TrimSpace(authHeader[len("Bearer "):])
		if tokenString == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "empty bearer token",
			})
			return
		}

		// Верифицируем токен
		idToken, err := verifier.Verify(c.Request.Context(), tokenString)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error":   "invalid token",
				"details": err.Error(),
			})
			return
		}

		// Извлекаем claims
		var claims map[string]any
		if err := idToken.Claims(&claims); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"error":   "failed to parse token claims",
				"details": err.Error(),
			})
			return
		}

		// Сохраняем claims в контексте
		c.Set("jwt_claims", claims)
		c.Next()

	}

}

func RequireRealmRole(role string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// В development режиме пропускаем проверку ролей
		if isDev() {
			c.Next()
			return
		}

		val, exists := c.Get("jwt_claims")
		if !exists {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "missing token claims",
			})
			return
		}

		claims, ok := val.(map[string]any)
		if !ok {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": "invalid claims structure",
			})
			return
		}

		if !hasRealmRole(claims, role) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error":    "insufficient permissions",
				"required": role,
			})
			return
		}

		c.Next()
	}
}

// RequireAnyRealmRole требует хотя бы одну из указанных ролей
func RequireAnyRealmRole(roles ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if isDev() {
			c.Next()
			return
		}

		val, exists := c.Get("jwt_claims")
		if !exists {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing token claims"})
			return
		}

		claims, ok := val.(map[string]any)
		if !ok {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "invalid claims structure"})
			return
		}

		for _, role := range roles {
			if hasRealmRole(claims, role) {
				c.Next()
				return
			}
		}

		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
			"error":    "insufficient permissions",
			"required": roles,
		})
	}
}

func hasRealmRole(claims map[string]any, role string) bool {
	realmAccess, ok := claims["realm_access"].(map[string]any)
	if !ok {
		return false
	}

	rolesInterface, ok := realmAccess["roles"].([]any)
	if !ok {
		return false
	}

	for _, r := range rolesInterface {
		if s, ok := r.(string); ok && s == role {
			return true
		}
	}
	return false
}

// GetUsername извлекает username из claims
func GetUsername(c *gin.Context) (string, bool) {
	val, exists := c.Get("jwt_claims")
	if !exists {
		return "", false
	}

	claims, ok := val.(map[string]any)
	if !ok {
		return "", false
	}

	// Пробуем разные поля, где может быть username
	if username, ok := claims["preferred_username"].(string); ok {
		return username, true
	}
	if email, ok := claims["email"].(string); ok {
		return email, true
	}
	if sub, ok := claims["sub"].(string); ok {
		return sub, true
	}

	return "", false
}

// GetUserID извлекает user ID (sub) из claims
func GetUserID(c *gin.Context) (string, bool) {
	val, exists := c.Get("jwt_claims")
	if !exists {
		return "", false
	}

	claims, ok := val.(map[string]any)
	if !ok {
		return "", false
	}

	if sub, ok := claims["sub"].(string); ok {
		return sub, true
	}

	return "", false
}
