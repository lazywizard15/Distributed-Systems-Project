package bank

import (
	"fmt"
	"strconv"
	"strings"
)

// Command formats stored in the Raft log:
//   DEPOSIT:{accountID}:{amount}
//   WITHDRAW:{accountID}:{amount}
//   TRANSFER:{fromID}:{toID}:{amount}
//   CREATE:{accountID}

// ApplyCommand parses a log entry command string and applies it to the store.
// This is called whenever a log entry is committed — on ALL nodes.
// This is how all nodes stay in sync: same log → same commands → same state.
func ApplyCommand(store *Store, command string) error {
	parts := strings.Split(command, ":")
	if len(parts) < 2 {
		return fmt.Errorf("invalid command: %s", command)
	}

	op := parts[0]

	switch op {
	case "CREATE":
		// CREATE:{accountID}
		if len(parts) != 2 {
			return fmt.Errorf("invalid CREATE command")
		}
		accountID := parts[1]
		_, err := store.GetBalance(accountID) // Just ensures key exists
		if err != nil {
			return err
		}
		return store.SetBalance(accountID, 0)

	case "DEPOSIT":
		// DEPOSIT:{accountID}:{amount}
		if len(parts) != 3 {
			return fmt.Errorf("invalid DEPOSIT command")
		}
		accountID := parts[1]
		amount, err := strconv.ParseFloat(parts[2], 64)
		if err != nil {
			return fmt.Errorf("invalid amount: %s", parts[2])
		}
		_, err = store.Deposit(accountID, amount)
		return err

	case "WITHDRAW":
		// WITHDRAW:{accountID}:{amount}
		if len(parts) != 3 {
			return fmt.Errorf("invalid WITHDRAW command")
		}
		accountID := parts[1]
		amount, err := strconv.ParseFloat(parts[2], 64)
		if err != nil {
			return fmt.Errorf("invalid amount: %s", parts[2])
		}
		_, err = store.Withdraw(accountID, amount)
		return err

	case "TRANSFER":
		// TRANSFER:{fromID}:{toID}:{amount}
		if len(parts) != 4 {
			return fmt.Errorf("invalid TRANSFER command")
		}
		fromID := parts[1]
		toID := parts[2]
		amount, err := strconv.ParseFloat(parts[3], 64)
		if err != nil {
			return fmt.Errorf("invalid amount: %s", parts[3])
		}
		return store.Transfer(fromID, toID, amount)

	default:
		return fmt.Errorf("unknown operation: %s", op)
	}
}

// BuildDepositCmd creates the log command string for a deposit
func BuildDepositCmd(accountID string, amount float64) string {
	return fmt.Sprintf("DEPOSIT:%s:%.2f", accountID, amount)
}

// BuildWithdrawCmd creates the log command string for a withdrawal
func BuildWithdrawCmd(accountID string, amount float64) string {
	return fmt.Sprintf("WITHDRAW:%s:%.2f", accountID, amount)
}

// BuildTransferCmd creates the log command string for a transfer
func BuildTransferCmd(fromID, toID string, amount float64) string {
	return fmt.Sprintf("TRANSFER:%s:%s:%.2f", fromID, toID, amount)
}

// BuildCreateCmd creates the log command string for account creation
func BuildCreateCmd(accountID string) string {
	return fmt.Sprintf("CREATE:%s", accountID)
}
