package api

import (
	"net/http"
	"strings"

	"distributed-banking/internal/auth"
	"distributed-banking/internal/bank"
	"distributed-banking/internal/raft"

	"github.com/gin-gonic/gin"
)

// Server wires together the Raft node, bank store, and HTTP routes
type Server struct {
	node  *raft.Node
	store *bank.Store
}

// NewServer creates the HTTP server
func NewServer(node *raft.Node, store *bank.Store) *Server {
	return &Server{node: node, store: store}
}

// SetupRoutes registers all HTTP endpoints on the given Gin router
func (s *Server) SetupRoutes(r *gin.Engine) {
	// ── Raft internal RPC endpoints (node-to-node only) ──
	raft := r.Group("/raft")
	{
		raft.POST("/vote", s.handleRequestVote)
		raft.POST("/append", s.handleAppendEntries)
	}

	// ── Auth endpoints (public) ──
	r.POST("/login", s.handleLogin)

	// ── Banking endpoints (require JWT) ──
	api := r.Group("/api", s.authMiddleware())
	{
		api.POST("/accounts", s.handleCreateAccount)          // Admin only
		api.GET("/accounts/:id/balance", s.handleGetBalance)  // Own account or admin
		api.POST("/accounts/:id/deposit", s.handleDeposit)    // Own account or admin
		api.POST("/accounts/:id/withdraw", s.handleWithdraw)  // Own account or admin
		api.POST("/transfer", s.handleTransfer)               // Own account or admin
	}
}

// ── Raft RPC handlers ──────────────────────────────────────────

func (s *Server) handleRequestVote(c *gin.Context) {
	var args raft.RequestVoteArgs
	if err := c.ShouldBindJSON(&args); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	reply := s.node.HandleRequestVote(args)
	c.JSON(http.StatusOK, reply)
}

func (s *Server) handleAppendEntries(c *gin.Context) {
	var args raft.AppendEntriesArgs
	if err := c.ShouldBindJSON(&args); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	reply := s.node.HandleAppendEntries(args)
	c.JSON(http.StatusOK, reply)
}

// ── Auth ───────────────────────────────────────────────────────

// LoginRequest for generating a token (demo — in production verify a password hash)
type LoginRequest struct {
	UserID    string    `json:"user_id"`
	Role      auth.Role `json:"role"`
	AccountID string    `json:"account_id"`
}

func (s *Server) handleLogin(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	token, err := auth.GenerateToken(req.UserID, req.Role, req.AccountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not generate token"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"token": token})
}

// authMiddleware validates the JWT on every /api request
func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing token"})
			return
		}
		tokenStr := strings.TrimPrefix(header, "Bearer ")
		claims, err := auth.ValidateToken(tokenStr)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}
		c.Set("claims", claims)
		c.Next()
	}
}

// ── Banking handlers ───────────────────────────────────────────

func (s *Server) handleCreateAccount(c *gin.Context) {
	claims := c.MustGet("claims").(*auth.Claims)
	if claims.Role != auth.RoleAdmin {
		c.JSON(http.StatusForbidden, gin.H{"error": "admin only"})
		return
	}

	var req struct {
		AccountID string `json:"account_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Propose to Raft — only commits if this node is leader
	cmd := bank.BuildCreateCmd(req.AccountID)
	if err := s.node.Propose(cmd); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "not the leader — try another node"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"account_id": req.AccountID, "status": "pending"})
}

func (s *Server) handleGetBalance(c *gin.Context) {
	claims := c.MustGet("claims").(*auth.Claims)
	accountID := c.Param("id")

	if !auth.CanOperate(claims, accountID) {
		c.JSON(http.StatusForbidden, gin.H{"error": "access denied"})
		return
	}

	balance, err := s.store.GetBalance(accountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"account_id": accountID, "balance": balance})
}

func (s *Server) handleDeposit(c *gin.Context) {
	claims := c.MustGet("claims").(*auth.Claims)
	accountID := c.Param("id")

	if !auth.CanOperate(claims, accountID) {
		c.JSON(http.StatusForbidden, gin.H{"error": "access denied"})
		return
	}

	var req struct {
		Amount float64 `json:"amount"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	cmd := bank.BuildDepositCmd(accountID, req.Amount)
	if err := s.node.Propose(cmd); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "not the leader"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "pending", "account_id": accountID, "amount": req.Amount})
}

func (s *Server) handleWithdraw(c *gin.Context) {
	claims := c.MustGet("claims").(*auth.Claims)
	accountID := c.Param("id")

	if !auth.CanOperate(claims, accountID) {
		c.JSON(http.StatusForbidden, gin.H{"error": "access denied"})
		return
	}

	var req struct {
		Amount float64 `json:"amount"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	cmd := bank.BuildWithdrawCmd(accountID, req.Amount)
	if err := s.node.Propose(cmd); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "not the leader"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "pending", "account_id": accountID, "amount": req.Amount})
}

func (s *Server) handleTransfer(c *gin.Context) {
	claims := c.MustGet("claims").(*auth.Claims)

	var req struct {
		FromAccount string  `json:"from_account"`
		ToAccount   string  `json:"to_account"`
		Amount      float64 `json:"amount"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if !auth.CanOperate(claims, req.FromAccount) {
		c.JSON(http.StatusForbidden, gin.H{"error": "access denied"})
		return
	}

	cmd := bank.BuildTransferCmd(req.FromAccount, req.ToAccount, req.Amount)
	if err := s.node.Propose(cmd); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "not the leader"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "pending", "from": req.FromAccount, "to": req.ToAccount, "amount": req.Amount})
}
