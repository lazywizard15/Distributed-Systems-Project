package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Role represents what a user is allowed to do
type Role string

const (
	RoleAdmin Role = "admin" // Can create accounts, view all balances
	RoleUser  Role = "user"  // Can deposit/withdraw/transfer their own accounts
)

// Secret used to sign JWTs — in production, load from env/secrets manager
var jwtSecret = []byte("super-secret-banking-key-change-in-prod")

// Claims is the payload stored inside the JWT token
type Claims struct {
	UserID    string `json:"user_id"`
	Role      Role   `json:"role"`
	AccountID string `json:"account_id"` // Which account this user owns
	jwt.RegisteredClaims
}

// GenerateToken creates a signed JWT for a user
func GenerateToken(userID string, role Role, accountID string) (string, error) {
	claims := Claims{
		UserID:    userID,
		Role:      role,
		AccountID: accountID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(jwtSecret)
}

// ValidateToken parses and validates a JWT string, returning its claims
func ValidateToken(tokenStr string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return jwtSecret, nil
	})
	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	return claims, nil
}

// CanOperate checks if a user has permission to operate on a given account.
// Rules:
//   - Admins can do anything
//   - Users can only touch their own account
func CanOperate(claims *Claims, targetAccountID string) bool {
	if claims.Role == RoleAdmin {
		return true
	}
	return claims.AccountID == targetAccountID
}
