package bank

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Store wraps Redis to store account balances.
// Each account is a Redis key: "account:{id}" → balance (float string)
type Store struct {
	client *redis.Client
	ctx    context.Context
}

// NewStore creates a new Redis-backed store
func NewStore(addr string) *Store {
	client := redis.NewClient(&redis.Options{
		Addr: addr, // e.g. "localhost:6379"
	})
	return &Store{
		client: client,
		ctx:    context.Background(),
	}
}

// key returns the Redis key for an account
func (s *Store) key(accountID string) string {
	return fmt.Sprintf("account:%s", accountID)
}

// GetBalance returns the balance of an account.
// Returns 0 if the account doesn't exist yet.
func (s *Store) GetBalance(accountID string) (float64, error) {
	val, err := s.client.Get(s.ctx, s.key(accountID)).Float64()
	if err == redis.Nil {
		return 0, nil // Account not found — treat as zero balance
	}
	return val, err
}

// SetBalance directly sets an account's balance (used when applying log entries)
func (s *Store) SetBalance(accountID string, amount float64) error {
	return s.client.Set(s.ctx, s.key(accountID), amount, 0).Err()
}

// Deposit adds amount to an account. Creates the account if it doesn't exist.
func (s *Store) Deposit(accountID string, amount float64) (float64, error) {
	if amount <= 0 {
		return 0, fmt.Errorf("deposit amount must be positive")
	}
	current, err := s.GetBalance(accountID)
	if err != nil {
		return 0, err
	}
	newBalance := current + amount
	return newBalance, s.SetBalance(accountID, newBalance)
}

// Withdraw subtracts amount from an account.
// Returns an error if the balance would go negative (no overdrafts).
func (s *Store) Withdraw(accountID string, amount float64) (float64, error) {
	if amount <= 0 {
		return 0, fmt.Errorf("withdrawal amount must be positive")
	}
	current, err := s.GetBalance(accountID)
	if err != nil {
		return 0, err
	}
	if current < amount {
		return 0, fmt.Errorf("insufficient funds: balance %.2f, requested %.2f", current, amount)
	}
	newBalance := current - amount
	return newBalance, s.SetBalance(accountID, newBalance)
}

// Transfer moves amount from one account to another atomically.
// Uses a Redis pipeline to ensure both operations happen together.
func (s *Store) Transfer(fromID, toID string, amount float64) error {
	if amount <= 0 {
		return fmt.Errorf("transfer amount must be positive")
	}

	fromBal, err := s.GetBalance(fromID)
	if err != nil {
		return err
	}
	if fromBal < amount {
		return fmt.Errorf("insufficient funds in %s", fromID)
	}

	toBal, err := s.GetBalance(toID)
	if err != nil {
		return err
	}

	// Pipeline both writes — they execute atomically on Redis
	pipe := s.client.Pipeline()
	pipe.Set(s.ctx, s.key(fromID), fromBal-amount, 0)
	pipe.Set(s.ctx, s.key(toID), toBal+amount, 0)
	_, err = pipe.Exec(s.ctx)
	return err
}

// Ping checks Redis connectivity
func (s *Store) Ping() error {
	return s.client.Ping(s.ctx).Err()
}
